"""Tests for trendscope.data.ingest (append-only raw ELT layer)."""
from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

import trendscope
from trendscope.data import ingest
from trendscope.settings import IngestSettings
from trendscope.universe import TickerGroup, Universe

from .conftest import FakeFetcher

# ---------------------------------------------------------------------------
# Smoke / package
# ---------------------------------------------------------------------------


def test_package_imports() -> None:
    assert trendscope.__version__ == "0.1.0"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _raw_tables(conn: duckdb.DuckDBPyConnection) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
        ).fetchall()
    }


def test_apply_schema_creates_raw_tables(tmp_path: Path) -> None:
    conn = duckdb.connect(str(tmp_path / "x.duckdb"))
    ingest.apply_schema(conn)
    assert {"prices", "tickers", "load_log"}.issubset(_raw_tables(conn))


def test_apply_schema_is_idempotent(tmp_path: Path) -> None:
    conn = duckdb.connect(str(tmp_path / "x.duckdb"))
    ingest.apply_schema(conn)
    ingest.apply_schema(conn)  # must not raise


def test_raw_prices_allows_multiple_versions(db_conn: duckdb.DuckDBPyConnection) -> None:
    """Append-only: same (date, ticker) twice must NOT raise — versions are the point."""
    for close in (1.0, 2.0):
        db_conn.execute(
            "INSERT INTO raw.prices (date, ticker, close, _source, _loaded_at) "
            "VALUES ('2024-01-02', 'FOO', ?, 'test', ?)",
            [close, datetime(2024, 1, 2, 21, 0, 0)],
        )
    n = db_conn.execute("SELECT COUNT(*) FROM raw.prices").fetchone()
    assert n is not None and n[0] == 2


# ---------------------------------------------------------------------------
# normalize_history — structural only; raw keeps upstream quirks
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


def test_normalize_history_handles_multiindex_columns(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 5))
    raw.columns = pd.MultiIndex.from_tuples([(c, "FOO") for c in raw.columns])
    out = ingest.normalize_history(raw, "FOO")
    assert list(out.columns) == list(ingest.PRICE_COLUMNS)
    assert len(out) == 4  # 4 business days


def test_normalize_history_keeps_nan_rows(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    """Raw is a faithful record — NaN rows are kept; staging filters them."""
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 8))
    raw.loc[raw.index[2], "Close"] = float("nan")
    out = ingest.normalize_history(raw, "FOO")
    assert len(out) == len(raw)
    assert pd.isna(out["close"].iloc[2])


def test_normalize_history_keeps_zero_split_convention(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    """yfinance's 0.0 = 'no split' is preserved in raw; staging maps it to 1.0."""
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 5))
    out = ingest.normalize_history(raw, "FOO")
    assert (out["split_ratio"] == 0.0).all()


def test_normalize_history_missing_action_columns_become_null(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 5))
    raw = raw.drop(columns=["Dividends", "Stock Splits"])
    out = ingest.normalize_history(raw, "FOO")
    assert out["dividend"].isna().all()
    assert out["split_ratio"].isna().all()


def test_normalize_history_volume_is_nullable_int(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 8))
    raw.loc[raw.index[1], "Volume"] = float("nan")
    out = ingest.normalize_history(raw, "FOO")
    assert str(out["volume"].dtype) == "Int64"
    assert pd.isna(out["volume"].iloc[1])
    assert out["volume"].iloc[0] == 1_000_000


def test_normalize_history_dates_are_python_date(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    raw = synthetic_history("FOO", date(2024, 1, 2), date(2024, 1, 5))
    out = ingest.normalize_history(raw, "FOO")
    assert isinstance(out["date"].iloc[0], date)


# ---------------------------------------------------------------------------
# append_prices — content-aware, append-only
# ---------------------------------------------------------------------------


def _normalized(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
    ticker: str,
    start: date,
    end: date,
) -> pd.DataFrame:
    return ingest.normalize_history(synthetic_history(ticker, start, end), ticker)


def test_append_prices_writes_rows(
    db_conn: duckdb.DuckDBPyConnection,
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    df = _normalized(synthetic_history, "FOO", date(2024, 1, 2), date(2024, 1, 5))
    appended, unchanged = ingest.append_prices(db_conn, df)
    assert (appended, unchanged) == (4, 0)
    n = db_conn.execute("SELECT COUNT(*) FROM raw.prices").fetchone()
    assert n is not None and n[0] == 4


def test_append_prices_identical_rerun_is_noop(
    db_conn: duckdb.DuckDBPyConnection,
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    df = _normalized(synthetic_history, "FOO", date(2024, 1, 2), date(2024, 1, 5))
    ingest.append_prices(db_conn, df)
    appended, unchanged = ingest.append_prices(db_conn, df)
    assert (appended, unchanged) == (0, 4)
    n = db_conn.execute("SELECT COUNT(*) FROM raw.prices").fetchone()
    assert n is not None and n[0] == 4  # not 8


def test_append_prices_changed_value_appends_new_version(
    db_conn: duckdb.DuckDBPyConnection,
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    df = _normalized(synthetic_history, "FOO", date(2024, 1, 2), date(2024, 1, 5))
    ingest.append_prices(db_conn, df, loaded_at=datetime(2024, 1, 5, 21, 0, 0))

    restated = df.copy()
    restated.loc[restated.index[0], "adj_close"] = 999.0  # upstream restatement
    appended, unchanged = ingest.append_prices(
        db_conn, restated, loaded_at=datetime(2024, 1, 6, 21, 0, 0)
    )
    assert (appended, unchanged) == (1, 3)

    versions = db_conn.execute(
        "SELECT adj_close FROM raw.prices WHERE date = '2024-01-02' AND ticker = 'FOO' "
        "ORDER BY _loaded_at"
    ).fetchall()
    assert len(versions) == 2  # both versions preserved
    assert versions[1][0] == 999.0


def test_append_prices_compares_against_latest_version(
    db_conn: duckdb.DuckDBPyConnection,
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    """After a restatement, re-loading the restated values must be a no-op."""
    df = _normalized(synthetic_history, "FOO", date(2024, 1, 2), date(2024, 1, 5))
    ingest.append_prices(db_conn, df, loaded_at=datetime(2024, 1, 5, 21, 0, 0))

    restated = df.copy()
    restated["adj_close"] = restated["adj_close"] * 0.99
    ingest.append_prices(db_conn, restated, loaded_at=datetime(2024, 1, 6, 21, 0, 0))

    appended, unchanged = ingest.append_prices(
        db_conn, restated, loaded_at=datetime(2024, 1, 7, 21, 0, 0)
    )
    assert (appended, unchanged) == (0, 4)


def test_append_prices_empty_is_noop(db_conn: duckdb.DuckDBPyConnection) -> None:
    out = ingest.append_prices(db_conn, pd.DataFrame(columns=list(ingest.PRICE_COLUMNS)))
    assert out == (0, 0)


def test_append_prices_sources_are_independent(
    db_conn: duckdb.DuckDBPyConnection,
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> None:
    """The same bar from a second source appends — provenance is part of identity."""
    df = _normalized(synthetic_history, "FOO", date(2024, 1, 2), date(2024, 1, 3))
    ingest.append_prices(db_conn, df, source="yfinance")
    appended, _ = ingest.append_prices(db_conn, df, source="polygon")
    assert appended == 2
    sources = {
        row[0]
        for row in db_conn.execute("SELECT DISTINCT _source FROM raw.prices").fetchall()
    }
    assert sources == {"yfinance", "polygon"}


# ---------------------------------------------------------------------------
# sync_ticker_metadata — content-aware versions, frugal info fetching
# ---------------------------------------------------------------------------


def test_sync_ticker_metadata_first_sighting_fetches_info(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fake_fetcher_factory: Callable[..., FakeFetcher],
) -> None:
    fetcher = fake_fetcher_factory(
        info=lambda t: {
            "longName": f"{t} Inc.",
            "sector": "Technology",
            "industry": "Software",
            "quoteType": "EQUITY",
        }
    )
    appended = ingest.sync_ticker_metadata(db_conn, "FOO", test_universe, fetcher)
    assert appended is True
    assert fetcher.info_calls == ["FOO"]
    row = db_conn.execute(
        "SELECT name, sector, industry, asset_type, groups, benchmark "
        "FROM raw.tickers WHERE ticker = 'FOO'"
    ).fetchone()
    assert row is not None
    assert row[0] == "FOO Inc."
    assert row[3] == "stock"
    assert sorted(row[4]) == ["holdings", "test_group"]
    assert row[5] == "QQQ"


def test_sync_ticker_metadata_unchanged_is_noop_and_no_refetch(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fake_fetcher: FakeFetcher,
) -> None:
    assert ingest.sync_ticker_metadata(db_conn, "FOO", test_universe, fake_fetcher) is True
    assert ingest.sync_ticker_metadata(db_conn, "FOO", test_universe, fake_fetcher) is False
    assert fake_fetcher.info_calls == ["FOO"]  # info fetched exactly once
    n = db_conn.execute("SELECT COUNT(*) FROM raw.tickers WHERE ticker = 'FOO'").fetchone()
    assert n is not None and n[0] == 1


def test_sync_ticker_metadata_group_change_appends_version(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fake_fetcher: FakeFetcher,
) -> None:
    ingest.sync_ticker_metadata(
        db_conn, "FOO", test_universe, fake_fetcher, loaded_at=datetime(2024, 1, 5, 21, 0, 0)
    )
    universe2 = Universe(
        groups={"test_group": test_universe.groups["test_group"]},
        sector_etf_map={"FOO": "XLK"},
    )
    appended = ingest.sync_ticker_metadata(
        db_conn, "FOO", universe2, fake_fetcher, loaded_at=datetime(2024, 1, 6, 21, 0, 0)
    )
    assert appended is True
    assert fake_fetcher.info_calls == ["FOO"]  # still just the first sighting
    latest = db_conn.execute(
        "SELECT groups, benchmark FROM raw.tickers WHERE ticker = 'FOO' "
        "ORDER BY _loaded_at DESC LIMIT 1"
    ).fetchone()
    assert latest is not None
    assert latest[0] == ["test_group"]
    assert latest[1] == "XLK"
    n = db_conn.execute("SELECT COUNT(*) FROM raw.tickers WHERE ticker = 'FOO'").fetchone()
    assert n is not None and n[0] == 2  # both versions kept


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
    assert summary["mode"] == "backfill"
    assert summary["tickers_processed"] == 2
    assert summary["rows_appended"] == 4 * 2  # 4 business days, 2 tickers
    assert summary["rows_unchanged"] == 0

    counts = dict(
        db_conn.execute("SELECT ticker, COUNT(*) FROM raw.prices GROUP BY ticker").fetchall()
    )
    assert counts == {"FOO": 4, "BAR": 4}


def test_run_ingest_rerun_appends_nothing(
    db_conn: duckdb.DuckDBPyConnection,
    test_universe: Universe,
    fake_fetcher: FakeFetcher,
    fast_ingest_settings: IngestSettings,
) -> None:
    kwargs: dict[str, object] = dict(
        conn=db_conn,
        universe=test_universe,
        settings=fast_ingest_settings,
        since=date(2024, 1, 2),
        today=date(2024, 1, 5),
        fetcher=fake_fetcher,
    )
    ingest.run_ingest(**kwargs)  # type: ignore[arg-type]
    summary = ingest.run_ingest(**kwargs)  # type: ignore[arg-type]
    assert summary["rows_appended"] == 0
    assert summary["rows_unchanged"] == 8
    n = db_conn.execute("SELECT COUNT(*) FROM raw.prices").fetchone()
    assert n is not None and n[0] == 8


def test_run_ingest_writes_load_log(
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
    rows = db_conn.execute(
        "SELECT load_id, mode, status, universe_size, rows_appended, rows_unchanged, "
        "tickers_failed, finished_at IS NOT NULL FROM raw.load_log"
    ).fetchall()
    assert len(rows) == 1
    load_id, mode, status, size, appended, unchanged, failed, finished = rows[0]
    assert load_id == summary["load_id"]
    assert (mode, status, size) == ("backfill", "success", 2)
    assert appended == summary["rows_appended"]
    assert unchanged == 0
    assert failed == 0
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
    assert summary["failures"][0][0] == "BAR"

    audit = db_conn.execute(
        "SELECT status, tickers_failed, error_message FROM raw.load_log"
    ).fetchone()
    assert audit is not None
    assert audit[0] == "partial"
    assert audit[1] == 1
    assert audit[2] is not None and "BAR" in audit[2]


def test_run_ingest_daily_skips_up_to_date_tickers(
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
    calls_before = len(fake_fetcher.calls)

    summary = ingest.run_ingest(
        conn=db_conn,
        universe=test_universe,
        settings=fast_ingest_settings,
        daily=True,
        today=date(2024, 1, 5),
        fetcher=fake_fetcher,
    )
    assert summary["tickers_skipped"] == 2
    assert summary["rows_appended"] == 0
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
    starts = {s for (_t, s, _e) in fake_fetcher.calls[-2:]}
    assert starts == {date(2024, 1, 6)}
    assert summary["status"] == "success"
    assert summary["rows_appended"] > 0


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


# ---------------------------------------------------------------------------
# migrate_legacy
# ---------------------------------------------------------------------------


def _create_legacy_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Recreate the pre-ELT v1 schema with a few rows."""
    conn.execute(
        """
        CREATE TABLE main.prices (
            date DATE, ticker VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, adj_close DOUBLE, volume BIGINT, dividend DOUBLE,
            split_ratio DOUBLE, data_source VARCHAR, ingested_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        INSERT INTO main.prices VALUES
        ('2024-01-02', 'FOO', 1, 2, 0.5, 1.5, 1.5, 100, 0.0, 1.0, 'yfinance',
         TIMESTAMP '2024-01-02 21:00:00'),
        ('2024-01-03', 'FOO', 2, 3, 1.5, 2.5, 2.5, 200, 0.0, 1.0, 'yfinance',
         TIMESTAMP '2024-01-03 21:00:00'),
        ('2024-01-02', 'BAR', 5, 6, 4.5, 5.5, 5.5, 300, 0.0, 1.0, 'yfinance',
         TIMESTAMP '2024-01-02 21:00:00')
        """
    )
    conn.execute(
        """
        CREATE TABLE main.tickers (
            ticker VARCHAR, name VARCHAR, sector VARCHAR, industry VARCHAR,
            asset_type VARCHAR, groups VARCHAR[], benchmark VARCHAR,
            added_at TIMESTAMP, updated_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        INSERT INTO main.tickers VALUES
        ('FOO', 'Foo Inc.', 'Tech', 'Software', 'stock', ['g1'], 'QQQ',
         TIMESTAMP '2024-01-02 21:00:00', TIMESTAMP '2024-01-02 21:00:00')
        """
    )
    conn.execute("CREATE TABLE main.signals (date DATE, ticker VARCHAR)")
    conn.execute("CREATE TABLE main.runs (run_id VARCHAR)")


def test_migrate_legacy_copies_and_drops(db_conn: duckdb.DuckDBPyConnection) -> None:
    _create_legacy_tables(db_conn)
    summary = ingest.migrate_legacy(db_conn)
    assert summary["status"] == "success"
    assert summary["prices_migrated"] == 3
    assert summary["tickers_migrated"] == 1
    assert set(summary["legacy_tables_dropped"]) == {"prices", "tickers", "signals", "runs"}

    # Timestamps and provenance preserved.
    row = db_conn.execute(
        "SELECT _source, _loaded_at FROM raw.prices WHERE ticker = 'FOO' AND date = '2024-01-02'"
    ).fetchone()
    assert row is not None
    assert row[0] == "yfinance"
    assert row[1] == datetime(2024, 1, 2, 21, 0, 0)

    # Legacy tables gone.
    main_tables = {
        r[0]
        for r in db_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    assert main_tables.isdisjoint({"prices", "tickers", "signals", "runs"})

    # Audit row written.
    audit = db_conn.execute(
        "SELECT mode, status FROM raw.load_log ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    assert audit == ("migration", "success")


def test_migrate_legacy_noop_on_fresh_db(db_conn: duckdb.DuckDBPyConnection) -> None:
    summary = ingest.migrate_legacy(db_conn)
    assert summary["status"] == "no_legacy"
    n = db_conn.execute("SELECT COUNT(*) FROM raw.prices").fetchone()
    assert n is not None and n[0] == 0


def test_migrate_legacy_skips_already_migrated_rows(
    db_conn: duckdb.DuckDBPyConnection,
) -> None:
    _create_legacy_tables(db_conn)
    # Pre-copy one legacy row into raw with identical identity.
    db_conn.execute(
        """
        INSERT INTO raw.prices (date, ticker, open, high, low, close, adj_close,
                                volume, dividend, split_ratio, _source, _loaded_at)
        VALUES ('2024-01-02', 'FOO', 1, 2, 0.5, 1.5, 1.5, 100, 0.0, 1.0, 'yfinance',
                TIMESTAMP '2024-01-02 21:00:00')
        """
    )
    summary = ingest.migrate_legacy(db_conn)
    assert summary["status"] == "success"
    assert summary["prices_migrated"] == 2  # 3 legacy rows, 1 already present
    n = db_conn.execute("SELECT COUNT(*) FROM raw.prices").fetchone()
    assert n is not None and n[0] == 3  # no duplicate of the pre-copied row


# ---------------------------------------------------------------------------
# run_ingest metadata coverage (universe fields flow into raw.tickers)
# ---------------------------------------------------------------------------


def test_run_ingest_populates_ticker_metadata(
    db_conn: duckdb.DuckDBPyConnection,
    fake_fetcher: FakeFetcher,
    fast_ingest_settings: IngestSettings,
) -> None:
    universe = Universe(
        groups={"solo": TickerGroup(description="one", tickers=["FOO"])},
        sector_etf_map={},
    )
    ingest.run_ingest(
        conn=db_conn,
        universe=universe,
        settings=fast_ingest_settings,
        since=date(2024, 1, 2),
        today=date(2024, 1, 5),
        fetcher=fake_fetcher,
    )
    row = db_conn.execute(
        "SELECT groups, benchmark FROM raw.tickers WHERE ticker = 'FOO'"
    ).fetchone()
    assert row is not None
    assert row[0] == ["solo"]
    assert row[1] == "SPY"  # default fallback
