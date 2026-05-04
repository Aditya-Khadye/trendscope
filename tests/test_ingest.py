"""Tests for trendscope.data.ingest."""
from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

import trendscope
from trendscope.data import ingest
from trendscope.settings import IngestSettings
from trendscope.universe import Universe

from .conftest import FakeFetcher


# ---------------------------------------------------------------------------
# Smoke / package
# ---------------------------------------------------------------------------
def test_package_imports() -> None:
    assert trendscope.__version__ == "0.1.0"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def test_apply_schema_creates_all_tables(tmp_path: Path) -> None:
    conn = duckdb.connect(str(tmp_path / "x.duckdb"))
    ingest.apply_schema(conn)
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    assert {"prices", "tickers", "signals", "runs"}.issubset(tables)


def test_apply_schema_is_idempotent(tmp_path: Path) -> None:
    conn = duckdb.connect(str(tmp_path / "x.duckdb"))
    ingest.apply_schema(conn)
    ingest.apply_schema(conn)  # must not raise


def test_prices_has_expected_columns_and_pk(db_conn: duckdb.DuckDBPyConnection) -> None:
    cols = {
        row[0]: row[1]
        for row in db_conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'prices'"
        ).fetchall()
    }
    expected = {
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
        "data_source",
        "ingested_at",
    }
    assert expected.issubset(cols.keys())

    # PK on (date, ticker) — verify by trying to insert a duplicate without UPSERT.
    db_conn.execute(
        "INSERT INTO prices (date, ticker, close) VALUES ('2024-01-02', 'FOO', 1.0)"
    )
    with pytest.raises(duckdb.ConstraintException):
        db_conn.execute(
            "INSERT INTO prices (date, ticker, close) VALUES ('2024-01-02', 'FOO', 2.0)"
        )


# ---------------------------------------------------------------------------
# normalize_history
# ---------------------------------------------------------------------------
def test_normalize_history_empty_returns_empty_with_columns() -> None:
    out = ingest.normalize_history(pd.DataFrame(), "FOO")
    assert list(out.columns) == list(ingest.PRICE_COLUMNS)
    assert out.empty


def test_normalize_history_renames_yfinance_columns(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 5))
    out = ingest.normalize_history(raw, "FOO")
    assert list(out.columns) == list(ingest.PRICE_COLUMNS)
    assert (out["ticker"] == "FOO").all()
    assert len(out) == len(raw)


def test_normalize_history_zero_split_becomes_one(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 5))
    out = ingest.normalize_history(raw, "FOO")
    assert (out["split_ratio"] == 1.0).all()


def test_normalize_history_preserves_actual_split(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 8))
    raw.loc[raw.index[1], "Stock Splits"] = 4.0  # 4-for-1 split on day 2
    out = ingest.normalize_history(raw, "FOO")
    assert out["split_ratio"].iloc[1] == 4.0
    assert out["split_ratio"].iloc[0] == 1.0


def test_normalize_history_handles_multiindex_columns(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 5))
    raw.columns = pd.MultiIndex.from_tuples([(c, "FOO") for c in raw.columns])
    out = ingest.normalize_history(raw, "FOO")
    assert list(out.columns) == list(ingest.PRICE_COLUMNS)
    assert len(out) == 4  # 4 business days


def test_normalize_history_drops_nan_rows(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 8))
    raw.loc[raw.index[2], "Close"] = float("nan")
    out = ingest.normalize_history(raw, "FOO")
    assert len(out) == len(raw) - 1


def test_normalize_history_dates_are_python_date(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 5))
    out = ingest.normalize_history(raw, "FOO")
    assert isinstance(out["date"].iloc[0], date)


# ---------------------------------------------------------------------------
# upsert_prices
# ---------------------------------------------------------------------------
def test_upsert_prices_writes_rows(
    db_conn: duckdb.DuckDBPyConnection,
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 5))
    df = ingest.normalize_history(raw, "FOO")
    written = ingest.upsert_prices(db_conn, df)
    count = db_conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    assert written == 4
    assert count == 4


def test_upsert_prices_is_idempotent(
    db_conn: duckdb.DuckDBPyConnection,
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 5))
    df = ingest.normalize_history(raw, "FOO")
    ingest.upsert_prices(db_conn, df)
    ingest.upsert_prices(db_conn, df)
    count = db_conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    assert count == 4  # not 8


def test_upsert_prices_updates_on_conflict(
    db_conn: duckdb.DuckDBPyConnection,
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 3))
    df1 = ingest.normalize_history(raw, "FOO")
    ingest.upsert_prices(db_conn, df1, data_source="yfinance")

    df2 = df1.copy()
    df2["close"] = 999.0
    ingest.upsert_prices(db_conn, df2, data_source="polygon")

    rows = db_conn.execute(
        "SELECT close, data_source FROM prices WHERE ticker = 'FOO' ORDER BY date"
    ).fetchall()
    assert all(r[0] == 999.0 for r in rows)
    assert all(r[1] == "polygon" for r in rows)


def test_upsert_prices_empty_is_noop(db_conn: duckdb.DuckDBPyConnection) -> None:
    written = ingest.upsert_prices(
        db_conn, pd.DataFrame(columns=list(ingest.PRICE_COLUMNS))
    )
    assert written == 0


# ---------------------------------------------------------------------------
# ensure_ticker_metadata
# ---------------------------------------------------------------------------
def test_ensure_ticker_metadata_inserts_with_info(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fake_fetcher_factory: Callable[..., FakeFetcher],
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    fetcher = fake_fetcher_factory(
        info=lambda t: {
            "longName": f"{t} Inc.",
            "sector": "Technology",
            "industry": "Software",
            "quoteType": "EQUITY",
        }
    )
    ingest.ensure_ticker_metadata(db_conn, "FOO", test_universe, fetcher)
    row = db_conn.execute(
        "SELECT name, sector, industry, asset_type, groups, benchmark FROM tickers WHERE ticker = 'FOO'"
    ).fetchone()
    assert row is not None
    name, sector, industry, asset_type, groups, benchmark = row
    assert name == "FOO Inc."
    assert sector == "Technology"
    assert industry == "Software"
    assert asset_type == "stock"
    assert sorted(groups) == ["holdings", "test_group"]
    assert benchmark == "QQQ"


def test_ensure_ticker_metadata_updates_groups_on_second_call(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fake_fetcher: FakeFetcher,
) -> None:
    ingest.ensure_ticker_metadata(db_conn, "FOO", test_universe, fake_fetcher)
    # Mutate the universe — FOO no longer in holdings.
    universe2 = Universe(
        groups={
            "test_group": test_universe.groups["test_group"],
        },
        sector_etf_map={"FOO": "XLK"},
    )
    ingest.ensure_ticker_metadata(db_conn, "FOO", universe2, fake_fetcher)
    row = db_conn.execute(
        "SELECT groups, benchmark FROM tickers WHERE ticker = 'FOO'"
    ).fetchone()
    assert row is not None
    groups, benchmark = row
    assert groups == ["test_group"]
    assert benchmark == "XLK"


# ---------------------------------------------------------------------------
# run_ingest end-to-end
# ---------------------------------------------------------------------------
def test_run_ingest_backfill_happy_path(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fake_fetcher: FakeFetcher,
    fast_ingest_settings: IngestSettings,
) -> None:
    summary = ingest.run_ingest(
        conn=db_conn,
        universe=test_universe,
        settings=fast_ingest_settings,
        since=date(2024, 1, 2),
        today=date(2024, 1, 5),
        fetcher=fake_fetcher,
    )
    assert summary["status"] == "success"
    assert summary["tickers_processed"] == 2
    assert summary["tickers_failed"] == 0
    assert summary["rows_written"] == 4 * 2  # 4 business days, 2 tickers

    counts = dict(
        db_conn.execute(
            "SELECT ticker, COUNT(*) FROM prices GROUP BY ticker"
        ).fetchall()
    )
    assert counts == {"FOO": 4, "BAR": 4}


def test_run_ingest_writes_audit_row(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fake_fetcher: FakeFetcher,
    fast_ingest_settings: IngestSettings,
) -> None:
    summary = ingest.run_ingest(
        conn=db_conn,
        universe=test_universe,
        settings=fast_ingest_settings,
        since=date(2024, 1, 2),
        today=date(2024, 1, 5),
        fetcher=fake_fetcher,
    )
    runs = db_conn.execute(
        """
        SELECT run_id, kind, status, universe_size, rows_written, finished_at IS NOT NULL
        FROM runs
        """
    ).fetchall()
    assert len(runs) == 1
    run_id, kind, status, n_universe, rows, finished = runs[0]
    assert run_id == summary["run_id"]
    assert kind == "ingest"
    assert status == "success"
    assert n_universe == 2
    assert rows == summary["rows_written"]
    assert finished is True


def test_run_ingest_partial_status_on_one_failure(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
    fast_ingest_settings: IngestSettings,
) -> None:
    def flaky(ticker: str, start: date, end: date) -> pd.DataFrame:
        if ticker == "BAR":
            raise RuntimeError("yfinance went sideways")
        return synthetic_history(ticker, start, end)

    fetcher = FakeFetcher(history=flaky)
    summary = ingest.run_ingest(
        conn=db_conn,
        universe=test_universe,
        settings=fast_ingest_settings,
        since=date(2024, 1, 2),
        today=date(2024, 1, 5),
        fetcher=fetcher,
    )
    assert summary["status"] == "partial"
    assert summary["tickers_failed"] == 1
    assert summary["tickers_processed"] == 1
    assert summary["failures"][0][0] == "BAR"

    foo_count = db_conn.execute(
        "SELECT COUNT(*) FROM prices WHERE ticker = 'FOO'"
    ).fetchone()[0]
    bar_count = db_conn.execute(
        "SELECT COUNT(*) FROM prices WHERE ticker = 'BAR'"
    ).fetchone()[0]
    assert foo_count == 4
    assert bar_count == 0

    audit = db_conn.execute("SELECT status, error_message FROM runs").fetchone()
    assert audit[0] == "partial"
    assert audit[1] is not None and "BAR" in audit[1]


def test_run_ingest_daily_skips_up_to_date_tickers(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fake_fetcher: FakeFetcher,
    fast_ingest_settings: IngestSettings,
) -> None:
    # First ingest: backfill through Jan 5.
    ingest.run_ingest(
        conn=db_conn,
        universe=test_universe,
        settings=fast_ingest_settings,
        since=date(2024, 1, 2),
        today=date(2024, 1, 5),
        fetcher=fake_fetcher,
    )
    calls_before = len(fake_fetcher.calls)

    # Second ingest, daily, on the same "today" — nothing new to fetch.
    summary = ingest.run_ingest(
        conn=db_conn,
        universe=test_universe,
        settings=fast_ingest_settings,
        daily=True,
        today=date(2024, 1, 5),
        fetcher=fake_fetcher,
    )
    assert summary["tickers_skipped"] == 2
    assert summary["rows_written"] == 0
    assert len(fake_fetcher.calls) == calls_before  # no new fetches


def test_run_ingest_daily_continues_from_latest(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fake_fetcher: FakeFetcher,
    fast_ingest_settings: IngestSettings,
) -> None:
    ingest.run_ingest(
        conn=db_conn,
        universe=test_universe,
        settings=fast_ingest_settings,
        since=date(2024, 1, 2),
        today=date(2024, 1, 5),
        fetcher=fake_fetcher,
    )
    summary = ingest.run_ingest(
        conn=db_conn,
        universe=test_universe,
        settings=fast_ingest_settings,
        daily=True,
        today=date(2024, 1, 12),
        fetcher=fake_fetcher,
    )
    # Each ticker fetched starting Jan 6 (one calendar day after latest = Jan 5).
    # yfinance will then return business days Jan 8-12.
    starts = {(t, s) for (t, s, _e) in fake_fetcher.calls[-2:]}
    assert all(s == date(2024, 1, 6) for (_t, s) in starts)
    assert summary["status"] == "success"
    assert summary["rows_written"] > 0


def test_run_ingest_rejects_both_since_and_daily(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fast_ingest_settings: IngestSettings,
) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        ingest.run_ingest(
            conn=db_conn,
            universe=test_universe,
            settings=fast_ingest_settings,
            since=date(2024, 1, 2),
            daily=True,
            today=date(2024, 1, 5),
        )


def test_run_ingest_rejects_neither_since_nor_daily(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fast_ingest_settings: IngestSettings,
) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        ingest.run_ingest(
            conn=db_conn,
            universe=test_universe,
            settings=fast_ingest_settings,
            today=date(2024, 1, 5),
        )


# ---------------------------------------------------------------------------
# fetch_with_retries
# ---------------------------------------------------------------------------
def test_fetch_with_retries_eventually_succeeds(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
    fast_ingest_settings: IngestSettings,
) -> None:
    settings = fast_ingest_settings.model_copy(
        update={"retries": fast_ingest_settings.retries.model_copy(update={"max_attempts": 3})}
    )
    attempt_count = {"n": 0}

    def flaky(ticker: str, start: date, end: date) -> pd.DataFrame:
        attempt_count["n"] += 1
        if attempt_count["n"] < 3:
            raise RuntimeError("transient")
        return synthetic_history(ticker, start, end)

    fetcher = FakeFetcher(history=flaky)
    df = ingest.fetch_with_retries(fetcher, "FOO", date(2024, 1, 2), date(2024, 1, 5), settings)
    assert not df.empty
    assert attempt_count["n"] == 3


def test_fetch_with_retries_raises_after_exhaustion(
    fast_ingest_settings: IngestSettings,
) -> None:
    def always_fails(ticker: str, start: date, end: date) -> pd.DataFrame:
        raise RuntimeError("never works")

    fetcher = FakeFetcher(history=always_fails)
    with pytest.raises(RuntimeError, match="never works"):
        ingest.fetch_with_retries(
            fetcher, "FOO", date(2024, 1, 2), date(2024, 1, 5), fast_ingest_settings
        )
