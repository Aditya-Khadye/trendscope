"""yfinance -> DuckDB extract/load. Append-only raw layer.

Raw tables are append-only: no UPDATE, no DELETE. Every row carries
`_source` and `_loaded_at` (UTC). Idempotency is content-aware: re-loading
identical data is a no-op; genuinely changed bars append a *new version*
of the row. Downstream dbt staging resolves latest-wins per (date, ticker).

Point-in-time note: upstream restates `adj_close` for the entire history
whenever a dividend or split occurs, so a full-range backfill after such an
event legitimately appends new versions of old rows. That is by design —
the raw layer preserves what was known when.
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from importlib import resources
from pathlib import Path
from typing import Any, Protocol

import duckdb
import pandas as pd
import structlog
import yfinance as yf

from trendscope.settings import IngestSettings
from trendscope.universe import Universe

logger = structlog.get_logger(__name__)

# Canonical column layout for raw.prices (metadata columns excluded).
PRICE_COLUMNS: tuple[str, ...] = (
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "dividend",
    "split_ratio",
)

# Value columns compared when deciding whether an incoming row is a new version.
_CONTENT_COLUMNS: tuple[str, ...] = tuple(
    c for c in PRICE_COLUMNS if c not in {"date", "ticker"}
)

DEFAULT_SOURCE = "yfinance"

LEGACY_TABLES: tuple[str, ...] = ("prices", "tickers", "signals", "runs")


def _utcnow() -> datetime:
    """Naive UTC timestamp for storage (project rule: UTC in storage)."""
    return datetime.now(tz=UTC).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Fetcher abstraction — Protocol so tests can substitute a fake.
# ---------------------------------------------------------------------------
class YFinanceFetcher(Protocol):
    """The slice of yfinance that the loader depends on."""

    def fetch_history(
        self, ticker: str, start: date, end: date, settings: IngestSettings
    ) -> pd.DataFrame: ...

    def fetch_info(self, ticker: str) -> dict[str, Any]: ...


class DefaultYFinanceFetcher:
    """Production fetcher: actually hits yfinance."""

    def fetch_history(
        self, ticker: str, start: date, end: date, settings: IngestSettings
    ) -> pd.DataFrame:
        cfg = settings.yfinance
        result: pd.DataFrame = yf.download(
            ticker,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
            interval=cfg.interval,
            auto_adjust=cfg.auto_adjust,
            actions=cfg.actions,
            progress=False,
            threads=False,
            timeout=cfg.timeout_seconds,
        )
        return result

    def fetch_info(self, ticker: str) -> dict[str, Any]:
        try:
            info = yf.Ticker(ticker).info
        except Exception as e:
            logger.warning("ticker_info_fetch_failed", ticker=ticker, error=str(e))
            return {}
        return info or {}


# ---------------------------------------------------------------------------
# Connection / schema.
# ---------------------------------------------------------------------------
def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection, ensuring the parent dir exists and schema is applied."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    apply_schema(conn)
    return conn


def apply_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Apply the CREATE statements from data/schema.sql. Idempotent."""
    schema_sql = resources.files("trendscope.data").joinpath("schema.sql").read_text()
    conn.execute(schema_sql)


# ---------------------------------------------------------------------------
# Normalization — yfinance frame -> canonical raw layout.
#
# Only *structural* concerns live here (pandas shape, column names, dtypes).
# Semantic cleaning (NaN rows, the 0.0-split convention) is deliberately NOT
# done: raw preserves upstream as-is and dbt staging cleans.
# ---------------------------------------------------------------------------
def normalize_history(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Convert a yfinance OHLCV frame into the raw.prices column layout.

    Handles MultiIndex columns (recent yfinance), missing action columns
    (kept as NULL, not fabricated), the datetime index, and nullable volume.
    Rows with NaN values are KEPT — raw is a faithful record.
    """
    if df.empty:
        return pd.DataFrame(columns=list(PRICE_COLUMNS))

    df = df.copy()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    rename_map = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
        "Dividends": "dividend",
        "Stock Splits": "split_ratio",
    }
    df = df.rename(columns=rename_map)

    for col in ("dividend", "split_ratio"):
        if col not in df.columns:
            df[col] = pd.NA

    df = df.reset_index()
    date_col = next((c for c in ("Date", "Datetime", "index") if c in df.columns), None)
    if date_col is None:
        raise ValueError("yfinance frame has no recognized date index after reset_index()")
    df["date"] = pd.to_datetime(df[date_col]).dt.date
    df["ticker"] = ticker

    # Volume must survive NaN -> NULL round-trips into BIGINT.
    df["volume"] = df["volume"].astype("Int64")

    return df[list(PRICE_COLUMNS)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Append helpers.
# ---------------------------------------------------------------------------
def append_prices(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    *,
    source: str = DEFAULT_SOURCE,
    loaded_at: datetime | None = None,
) -> tuple[int, int]:
    """Append new/changed rows to raw.prices. Returns (appended, unchanged).

    A row is appended when its (date, ticker) has no version for `source`
    yet, or when any value column differs from the LATEST version (compared
    with IS DISTINCT FROM, so NULLs compare sanely). Identical rows are
    skipped — re-running the same load is a no-op.
    """
    if df.empty:
        return (0, 0)
    loaded_at = loaded_at or _utcnow()

    staging = f"_stg_prices_{uuid.uuid4().hex}"
    conn.register(staging, df)
    try:
        col_list = ", ".join(PRICE_COLUMNS)
        diff_clause = " OR ".join(
            f"s.{c} IS DISTINCT FROM l.{c}" for c in _CONTENT_COLUMNS
        )
        inserted = conn.execute(
            f"""
            INSERT INTO raw.prices ({col_list}, _source, _loaded_at)
            WITH latest AS (
                SELECT *
                FROM (
                    SELECT *,
                           row_number() OVER (
                               PARTITION BY date, ticker
                               ORDER BY _loaded_at DESC
                           ) AS rn
                    FROM raw.prices
                    WHERE _source = ?
                )
                WHERE rn = 1
            )
            SELECT {", ".join(f"s.{c}" for c in PRICE_COLUMNS)}, ?, ?
            FROM {staging} s
            LEFT JOIN latest l ON l.date = s.date AND l.ticker = s.ticker
            WHERE l.date IS NULL OR ({diff_clause})
            RETURNING 1
            """,
            [source, source, loaded_at],
        ).fetchall()
    finally:
        conn.unregister(staging)

    appended = len(inserted)
    return (appended, len(df) - appended)


def sync_ticker_metadata(
    conn: duckdb.DuckDBPyConnection,
    ticker: str,
    universe: Universe,
    fetcher: YFinanceFetcher,
    *,
    source: str = DEFAULT_SOURCE,
    loaded_at: datetime | None = None,
) -> bool:
    """Append a raw.tickers version if content changed. Returns True if appended.

    yfinance `.info` is fetched only on first sighting of a ticker; later
    runs reuse the latest stored API fields and refresh only the
    universe-derived fields (groups, benchmark). Comparison happens in
    Python so list-typed `groups` compares plainly.
    """
    loaded_at = loaded_at or _utcnow()
    groups = universe.groups_for(ticker)
    benchmark = universe.benchmark_for(ticker)

    latest = conn.execute(
        """
        SELECT name, sector, industry, asset_type, groups, benchmark
        FROM raw.tickers
        WHERE ticker = ?
        ORDER BY _loaded_at DESC
        LIMIT 1
        """,
        [ticker],
    ).fetchone()

    if latest is None:
        info = fetcher.fetch_info(ticker)
        candidate = (
            info.get("longName") or info.get("shortName"),
            info.get("sector"),
            info.get("industry"),
            _classify_asset(info),
            groups,
            benchmark,
        )
    else:
        # Reuse stored API fields; only universe-derived fields can drift.
        candidate = (latest[0], latest[1], latest[2], latest[3], groups, benchmark)
        if (list(latest[4] or []), latest[5]) == (groups, benchmark):
            return False

    conn.execute(
        """
        INSERT INTO raw.tickers
            (ticker, name, sector, industry, asset_type, groups, benchmark,
             _source, _loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [ticker, *candidate, source, loaded_at],
    )
    return True


def _classify_asset(info: dict[str, Any]) -> str:
    qt = str(info.get("quoteType") or "").lower()
    if qt == "equity":
        return "stock"
    if qt in {"etf", "index", "mutualfund"}:
        return qt
    return qt or "unknown"


# ---------------------------------------------------------------------------
# Load audit log.
# ---------------------------------------------------------------------------
def start_load(
    conn: duckdb.DuckDBPyConnection,
    *,
    mode: str,
    universe_size: int,
    metadata: dict[str, Any],
) -> str:
    """Insert a raw.load_log row in 'running' state. Returns the load_id."""
    load_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO raw.load_log (load_id, mode, started_at, status, universe_size, metadata)
        VALUES (?, ?, ?, 'running', ?, ?::JSON)
        """,
        [load_id, mode, _utcnow(), universe_size, json.dumps(metadata)],
    )
    return load_id


def finish_load(
    conn: duckdb.DuckDBPyConnection,
    *,
    load_id: str,
    status: str,
    rows_appended: int = 0,
    rows_unchanged: int = 0,
    tickers_failed: int = 0,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE raw.load_log SET
            finished_at = ?,
            status = ?,
            rows_appended = ?,
            rows_unchanged = ?,
            tickers_failed = ?,
            error_message = ?
        WHERE load_id = ?
        """,
        [_utcnow(), status, rows_appended, rows_unchanged, tickers_failed, error_message, load_id],
    )


# ---------------------------------------------------------------------------
# Per-ticker fetch with retries + range helpers.
# ---------------------------------------------------------------------------
def fetch_with_retries(
    fetcher: YFinanceFetcher,
    ticker: str,
    start: date,
    end: date,
    settings: IngestSettings,
) -> pd.DataFrame:
    """yfinance call with exponential backoff. Raises the last exception on exhaustion."""
    last_err: Exception | None = None
    for attempt in range(1, settings.retries.max_attempts + 1):
        try:
            return fetcher.fetch_history(ticker, start, end, settings)
        except Exception as e:
            last_err = e
            wait = settings.retries.backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "load_fetch_retry",
                ticker=ticker,
                attempt=attempt,
                error=str(e),
                wait_seconds=wait,
            )
            if attempt < settings.retries.max_attempts and wait > 0:
                time.sleep(wait)
    assert last_err is not None
    raise last_err


def latest_date_for(conn: duckdb.DuckDBPyConnection, ticker: str) -> date | None:
    """Most recent loaded date for a ticker (any version), or None if never seen."""
    row = conn.execute(
        "SELECT MAX(date) FROM raw.prices WHERE ticker = ?", [ticker]
    ).fetchone()
    if row is None or row[0] is None:
        return None
    val = row[0]
    return val if isinstance(val, date) else date.fromisoformat(str(val))


# ---------------------------------------------------------------------------
# Per-ticker orchestration.
# ---------------------------------------------------------------------------
def load_ticker(
    conn: duckdb.DuckDBPyConnection,
    ticker: str,
    *,
    start: date,
    end: date,
    universe: Universe,
    fetcher: YFinanceFetcher,
    settings: IngestSettings,
    loaded_at: datetime,
) -> tuple[int, int]:
    """Extract+load one ticker for [start, end] inclusive. Returns (appended, unchanged)."""
    log = logger.bind(ticker=ticker, start=start.isoformat(), end=end.isoformat())
    raw_df = fetch_with_retries(fetcher, ticker, start, end, settings)
    if raw_df.empty:
        log.warning("load_empty_response")
        sync_ticker_metadata(conn, ticker, universe, fetcher, loaded_at=loaded_at)
        return (0, 0)
    df = normalize_history(raw_df, ticker)
    appended, unchanged = append_prices(conn, df, loaded_at=loaded_at)
    sync_ticker_metadata(conn, ticker, universe, fetcher, loaded_at=loaded_at)
    log.info("load_ticker_complete", appended=appended, unchanged=unchanged)
    return (appended, unchanged)


# ---------------------------------------------------------------------------
# Top-level orchestrator.
# ---------------------------------------------------------------------------
def run_ingest(
    *,
    conn: duckdb.DuckDBPyConnection,
    universe: Universe,
    settings: IngestSettings,
    since: date | None = None,
    daily: bool = False,
    fetcher: YFinanceFetcher | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    """Run an extract/load pass over the full universe.

    Exactly one of `since` (backfill from this date) or `daily=True`
    (incremental from the latest loaded date per ticker) must be given.
    """
    if (since is None) == (not daily):
        raise ValueError("provide exactly one of: since=<date>, daily=True")

    fetcher = fetcher or DefaultYFinanceFetcher()
    today = today or datetime.now(tz=UTC).date()
    loaded_at = _utcnow()
    tickers = universe.all_tickers
    mode = "daily" if daily else "backfill"

    metadata: dict[str, Any] = {
        "since": since.isoformat() if since else None,
        "today": today.isoformat(),
        "tickers": tickers,
    }
    load_id = start_load(conn, mode=mode, universe_size=len(tickers), metadata=metadata)
    log = logger.bind(load_id=load_id, mode=mode)

    appended_total = 0
    unchanged_total = 0
    skipped = 0
    failures: list[tuple[str, str]] = []
    status = "success"

    try:
        for ticker in tickers:
            if daily:
                last = latest_date_for(conn, ticker)
                start_date = (
                    last + timedelta(days=1)
                    if last is not None
                    else date.fromisoformat(settings.default_start)
                )
                if start_date > today:
                    log.info(
                        "load_ticker_up_to_date",
                        ticker=ticker,
                        last=last.isoformat() if last else None,
                    )
                    skipped += 1
                    continue
            else:
                assert since is not None  # narrowed by the validation above
                start_date = since

            try:
                appended, unchanged = load_ticker(
                    conn=conn,
                    ticker=ticker,
                    start=start_date,
                    end=today,
                    universe=universe,
                    fetcher=fetcher,
                    settings=settings,
                    loaded_at=loaded_at,
                )
                appended_total += appended
                unchanged_total += unchanged
            except Exception as e:
                log.error("load_ticker_failed", ticker=ticker, error=str(e))
                failures.append((ticker, str(e)))

        if failures:
            status = "partial"
        finish_load(
            conn,
            load_id=load_id,
            status=status,
            rows_appended=appended_total,
            rows_unchanged=unchanged_total,
            tickers_failed=len(failures),
            error_message="; ".join(f"{t}: {e}" for t, e in failures) or None,
        )
    except Exception as e:
        finish_load(conn, load_id=load_id, status="error", error_message=str(e))
        raise

    summary: dict[str, Any] = {
        "load_id": load_id,
        "mode": mode,
        "status": status,
        "tickers_total": len(tickers),
        "tickers_processed": len(tickers) - len(failures) - skipped,
        "tickers_skipped": skipped,
        "tickers_failed": len(failures),
        "rows_appended": appended_total,
        "rows_unchanged": unchanged_total,
        "failures": failures,
    }
    log.info("load_run_complete", **summary)
    return summary


# ---------------------------------------------------------------------------
# One-time legacy migration (pre-ELT schema -> raw).
# ---------------------------------------------------------------------------
def migrate_legacy(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Copy the pre-ELT main.prices / main.tickers into raw, then drop legacy tables.

    - Preserves original load timestamps (`ingested_at` -> `_loaded_at`) and
      provenance (`data_source` -> `_source`). Legacy rows were pre-cleaned
      by the v1 pipeline (NaN rows dropped, 0.0 splits mapped to 1.0), which
      is fine — staging's cleaning is a no-op on already-clean rows.
    - Idempotent: the copy anti-joins on the full identity including
      `_loaded_at`, so re-running never duplicates.
    - Legacy tables (prices, tickers, signals, runs) are dropped only after
      a parity check confirms every legacy row landed in raw.
    - No-op with status 'no_legacy' when the legacy tables are absent
      (i.e. any fresh clone).
    """
    main_tables = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    if "prices" not in main_tables:
        return {"status": "no_legacy", "prices_migrated": 0, "tickers_migrated": 0}

    load_id = start_load(
        conn, mode="migration", universe_size=0, metadata={"legacy_tables": sorted(main_tables)}
    )

    prices_migrated = len(
        conn.execute(
            """
            INSERT INTO raw.prices
                (date, ticker, open, high, low, close, adj_close, volume,
                 dividend, split_ratio, _source, _loaded_at)
            SELECT m.date, m.ticker, m.open, m.high, m.low, m.close, m.adj_close,
                   m.volume, m.dividend, m.split_ratio, m.data_source, m.ingested_at
            FROM main.prices m
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.prices r
                WHERE r.date = m.date AND r.ticker = m.ticker
                  AND r._source = m.data_source AND r._loaded_at = m.ingested_at
            )
            RETURNING 1
            """
        ).fetchall()
    )

    tickers_migrated = 0
    if "tickers" in main_tables:
        tickers_migrated = len(
            conn.execute(
                """
                INSERT INTO raw.tickers
                    (ticker, name, sector, industry, asset_type, groups, benchmark,
                     _source, _loaded_at)
                SELECT m.ticker, m.name, m.sector, m.industry, m.asset_type,
                       m.groups, m.benchmark, 'yfinance', m.updated_at
                FROM main.tickers m
                WHERE NOT EXISTS (
                    SELECT 1 FROM raw.tickers r
                    WHERE r.ticker = m.ticker AND r._loaded_at = m.updated_at
                )
                RETURNING 1
                """
            ).fetchall()
        )

    # Parity: every legacy price row must exist in raw before we drop anything.
    missing = conn.execute(
        """
        SELECT COUNT(*) FROM main.prices m
        WHERE NOT EXISTS (
            SELECT 1 FROM raw.prices r
            WHERE r.date = m.date AND r.ticker = m.ticker
              AND r._source = m.data_source AND r._loaded_at = m.ingested_at
        )
        """
    ).fetchone()
    if missing is None or missing[0] != 0:
        finish_load(
            conn,
            load_id=load_id,
            status="error",
            error_message=f"parity check failed: {missing[0] if missing else '?'} rows missing",
        )
        raise RuntimeError("legacy migration parity check failed; legacy tables NOT dropped")

    dropped = [t for t in LEGACY_TABLES if t in main_tables]
    for table in dropped:
        conn.execute(f"DROP TABLE main.{table}")

    finish_load(
        conn,
        load_id=load_id,
        status="success",
        rows_appended=prices_migrated + tickers_migrated,
    )
    summary = {
        "status": "success",
        "prices_migrated": prices_migrated,
        "tickers_migrated": tickers_migrated,
        "legacy_tables_dropped": dropped,
    }
    logger.info("legacy_migration_complete", **summary)
    return summary
