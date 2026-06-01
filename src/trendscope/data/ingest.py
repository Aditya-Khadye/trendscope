"""yfinance -> DuckDB ingest. Idempotent per-ticker upsert."""
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

# Order matches the prices table (excluding data_source, ingested_at which we set in SQL).
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

DEFAULT_DATA_SOURCE = "yfinance"


# ---------------------------------------------------------------------------
# Fetcher abstraction — Protocol so tests can substitute a fake.
# ---------------------------------------------------------------------------
class YFinanceFetcher(Protocol):
    """The slice of yfinance that ingest depends on."""

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
# Normalization — yfinance DataFrame -> long-form prices rows.
# ---------------------------------------------------------------------------
def normalize_history(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Convert a yfinance OHLCV frame into the prices-table column layout.

    Handles: MultiIndex columns (recent yfinance behavior), missing actions,
    yfinance's "0.0 means no split" convention, and the index date column.
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

    if "dividend" not in df.columns:
        df["dividend"] = 0.0
    if "split_ratio" not in df.columns:
        df["split_ratio"] = 1.0
    # yfinance reports 0.0 when no split occurred; we store 1.0 so split_ratio
    # is always a valid multiplier.
    df["split_ratio"] = df["split_ratio"].where(df["split_ratio"] != 0.0, 1.0)

    df = df.reset_index()
    date_col = next((c for c in ("Date", "Datetime", "index") if c in df.columns), None)
    if date_col is None:
        raise ValueError("yfinance frame has no recognized date index after reset_index()")
    df["date"] = pd.to_datetime(df[date_col]).dt.date
    df["ticker"] = ticker

    # Drop rows where critical fields are NaN (occasionally happens at series edges).
    df = df.dropna(subset=["open", "high", "low", "close", "adj_close", "volume"])

    return df[list(PRICE_COLUMNS)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Upsert helpers.
# ---------------------------------------------------------------------------
def upsert_prices(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    data_source: str = DEFAULT_DATA_SOURCE,
) -> int:
    """Insert or replace rows in `prices`. Returns the number of rows written."""
    if df.empty:
        return 0

    # Register under a unique name so concurrent calls in tests don't collide.
    staging = f"_stg_prices_{uuid.uuid4().hex}"
    conn.register(staging, df)
    try:
        col_list = ", ".join(PRICE_COLUMNS)
        update_set = ",\n            ".join(
            f"{c} = EXCLUDED.{c}" for c in PRICE_COLUMNS if c not in {"date", "ticker"}
        )
        # NB: use now() rather than bare CURRENT_TIMESTAMP — DuckDB's ON CONFLICT
        # parser treats the latter as an identifier, not the time function.
        conn.execute(
            f"""
            INSERT INTO prices ({col_list}, data_source, ingested_at)
            SELECT {col_list}, ?, now() FROM {staging}
            ON CONFLICT (date, ticker) DO UPDATE SET
                {update_set},
                data_source = EXCLUDED.data_source,
                ingested_at = now()
            """,
            [data_source],
        )
    finally:
        conn.unregister(staging)
    return len(df)


def ensure_ticker_metadata(
    conn: duckdb.DuckDBPyConnection,
    ticker: str,
    universe: Universe,
    fetcher: YFinanceFetcher,
) -> None:
    """Insert tickers row on first encounter; refresh groups/benchmark every run."""
    groups = universe.groups_for(ticker)
    benchmark = universe.benchmark_for(ticker)

    existing = conn.execute("SELECT 1 FROM tickers WHERE ticker = ?", [ticker]).fetchone()
    if existing is None:
        info = fetcher.fetch_info(ticker)
        conn.execute(
            """
            INSERT INTO tickers (ticker, name, sector, industry, asset_type, groups, benchmark)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ticker,
                info.get("longName") or info.get("shortName"),
                info.get("sector"),
                info.get("industry"),
                _classify_asset(info),
                groups,
                benchmark,
            ],
        )
    else:
        conn.execute(
            """
            UPDATE tickers
            SET groups = ?, benchmark = ?, updated_at = CURRENT_TIMESTAMP
            WHERE ticker = ?
            """,
            [groups, benchmark, ticker],
        )


def _classify_asset(info: dict[str, Any]) -> str:
    qt = str(info.get("quoteType") or "").lower()
    if qt == "equity":
        return "stock"
    if qt in {"etf", "index", "mutualfund"}:
        return qt
    return qt or "unknown"


# ---------------------------------------------------------------------------
# Run audit log.
# ---------------------------------------------------------------------------
def start_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    kind: str,
    universe_size: int,
    metadata: dict[str, Any],
) -> str:
    """Insert a runs row in 'running' state. Returns the run_id."""
    run_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO runs (run_id, kind, started_at, status, universe_size, metadata)
        VALUES (?, ?, CURRENT_TIMESTAMP, 'running', ?, ?::JSON)
        """,
        [run_id, kind, universe_size, json.dumps(metadata)],
    )
    return run_id


def finish_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    status: str,
    rows_written: int = 0,
    rows_skipped: int = 0,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE runs SET
            finished_at = CURRENT_TIMESTAMP,
            status = ?,
            rows_written = ?,
            rows_skipped = ?,
            error_message = ?
        WHERE run_id = ?
        """,
        [status, rows_written, rows_skipped, error_message, run_id],
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
                "ingest_fetch_retry",
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
    """Most recent stored date for a ticker, or None if never seen."""
    row = conn.execute("SELECT MAX(date) FROM prices WHERE ticker = ?", [ticker]).fetchone()
    if row is None or row[0] is None:
        return None
    val = row[0]
    return val if isinstance(val, date) else date.fromisoformat(str(val))


# ---------------------------------------------------------------------------
# Per-ticker orchestration.
# ---------------------------------------------------------------------------
def ingest_ticker(
    conn: duckdb.DuckDBPyConnection,
    ticker: str,
    *,
    start: date,
    end: date,
    universe: Universe,
    fetcher: YFinanceFetcher,
    settings: IngestSettings,
) -> int:
    """Ingest one ticker for [start, end] inclusive. Returns rows written."""
    log = logger.bind(ticker=ticker, start=start.isoformat(), end=end.isoformat())
    raw = fetch_with_retries(fetcher, ticker, start, end, settings)
    if raw.empty:
        log.warning("ingest_empty_response")
        ensure_ticker_metadata(conn, ticker, universe, fetcher)
        return 0
    df = normalize_history(raw, ticker)
    if df.empty:
        log.warning("ingest_normalize_empty")
        return 0
    written = upsert_prices(conn, df)
    ensure_ticker_metadata(conn, ticker, universe, fetcher)
    log.info("ingest_ticker_complete", rows=written)
    return written


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
    """Run an ingest pass over the full universe.

    Exactly one of `since` (backfill from this date) or `daily=True` (incremental
    from the latest stored date per ticker) must be given.
    """
    if (since is None) == (not daily):
        raise ValueError("provide exactly one of: since=<date>, daily=True")

    fetcher = fetcher or DefaultYFinanceFetcher()
    today = today or datetime.now(tz=UTC).date()
    tickers = universe.all_tickers

    metadata: dict[str, Any] = {
        "since": since.isoformat() if since else None,
        "daily": daily,
        "today": today.isoformat(),
        "tickers": tickers,
    }
    run_id = start_run(conn, kind="ingest", universe_size=len(tickers), metadata=metadata)
    log = logger.bind(run_id=run_id)

    total_rows = 0
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
                        "ingest_ticker_up_to_date",
                        ticker=ticker,
                        last=last.isoformat() if last else None,
                    )
                    skipped += 1
                    continue
            else:
                assert since is not None  # narrowed by the validation above
                start_date = since

            try:
                rows = ingest_ticker(
                    conn=conn,
                    ticker=ticker,
                    start=start_date,
                    end=today,
                    universe=universe,
                    fetcher=fetcher,
                    settings=settings,
                )
                total_rows += rows
            except Exception as e:
                log.error("ingest_ticker_failed", ticker=ticker, error=str(e))
                failures.append((ticker, str(e)))

        if failures:
            status = "partial"
        finish_run(
            conn,
            run_id=run_id,
            status=status,
            rows_written=total_rows,
            rows_skipped=skipped,
            error_message="; ".join(f"{t}: {e}" for t, e in failures) or None,
        )
    except Exception as e:
        finish_run(conn, run_id=run_id, status="error", error_message=str(e))
        raise

    summary: dict[str, Any] = {
        "run_id": run_id,
        "status": status,
        "tickers_total": len(tickers),
        "tickers_processed": len(tickers) - len(failures) - skipped,
        "tickers_skipped": skipped,
        "tickers_failed": len(failures),
        "rows_written": total_rows,
        "failures": failures,
    }
    log.info("ingest_run_complete", **summary)
    return summary
