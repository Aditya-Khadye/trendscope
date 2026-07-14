"""Shared pytest fixtures."""
from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from trendscope.data import ingest
from trendscope.settings import (
    IngestRetriesSettings,
    IngestSettings,
    YFinanceIngestSettings,
)
from trendscope.universe import TickerGroup, Universe


@pytest.fixture
def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


@pytest.fixture
def db_conn(tmp_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    """A fresh DuckDB with the raw schema applied, backed by a tmp file."""
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    ingest.apply_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def synthetic_history() -> Callable[[str, date, date], pd.DataFrame]:
    """Factory for yfinance-shaped daily OHLCV DataFrames over a date range."""

    def _make(ticker: str, start: date, end: date) -> pd.DataFrame:
        idx = pd.date_range(start=start, end=end, freq="B")
        if len(idx) == 0:
            return pd.DataFrame(
                columns=[
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Adj Close",
                    "Volume",
                    "Dividends",
                    "Stock Splits",
                ]
            )
        n = len(idx)
        prices = [100.0 + i for i in range(n)]
        df = pd.DataFrame(
            {
                "Open": prices,
                "High": [p + 1.0 for p in prices],
                "Low": [p - 1.0 for p in prices],
                "Close": [p + 0.5 for p in prices],
                "Adj Close": [p + 0.5 for p in prices],
                "Volume": [1_000_000] * n,
                "Dividends": [0.0] * n,
                "Stock Splits": [0.0] * n,
            },
            index=idx,
        )
        df.index.name = "Date"
        # Mark the ticker so multi-ticker assertions can distinguish frames.
        df["Open"] = df["Open"] + (hash(ticker) % 7)
        return df

    return _make


class FakeFetcher:
    """Test double for YFinanceFetcher.

    history: callable taking (ticker, start, end) and returning a yfinance-shaped
    DataFrame. Raise inside the callable to simulate failure.
    info: optional callable taking ticker and returning a metadata dict.
    Records every fetch_history call in `calls` and every fetch_info call in
    `info_calls`.
    """

    def __init__(
        self,
        history: Callable[[str, date, date], pd.DataFrame],
        info: Callable[[str], dict[str, Any]] | None = None,
    ) -> None:
        self._history = history
        self._info = info or (lambda _t: {})
        self.calls: list[tuple[str, date, date]] = []
        self.info_calls: list[str] = []

    def fetch_history(
        self, ticker: str, start: date, end: date, settings: IngestSettings
    ) -> pd.DataFrame:
        self.calls.append((ticker, start, end))
        return self._history(ticker, start, end)

    def fetch_info(self, ticker: str) -> dict[str, Any]:
        self.info_calls.append(ticker)
        return self._info(ticker)


@pytest.fixture
def fake_fetcher_factory(
    synthetic_history: Callable[[str, date, date], pd.DataFrame],
) -> Callable[..., FakeFetcher]:
    """Factory for FakeFetcher instances. Lets tests override history/info."""

    def _make(
        history: Callable[[str, date, date], pd.DataFrame] | None = None,
        info: Callable[[str], dict[str, Any]] | None = None,
    ) -> FakeFetcher:
        return FakeFetcher(history=history or synthetic_history, info=info)

    return _make


@pytest.fixture
def fake_fetcher(fake_fetcher_factory: Callable[..., FakeFetcher]) -> FakeFetcher:
    return fake_fetcher_factory()


@pytest.fixture
def test_universe() -> Universe:
    """Two-ticker universe; FOO benchmarked against QQQ, BAR falls back to SPY."""
    return Universe(
        groups={
            "test_group": TickerGroup(description="test universe", tickers=["FOO", "BAR"]),
            "holdings": TickerGroup(description="positions", tickers=["FOO"]),
        },
        sector_etf_map={"FOO": "QQQ"},
    )


@pytest.fixture
def fast_ingest_settings() -> IngestSettings:
    """Ingest settings tuned for fast tests: 1 attempt, 0s backoff."""
    return IngestSettings(
        default_start="2020-01-01",
        yfinance=YFinanceIngestSettings(),
        retries=IngestRetriesSettings(max_attempts=1, backoff_seconds=0),
    )
