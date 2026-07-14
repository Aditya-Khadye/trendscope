"""Rule-based selection of "interesting today" tickers from the signal marts.

All rules are deterministic thresholds over marts.fct_daily_signals (long
form, pivoted wide here). The LLM never decides what is interesting — it
only narrates what these rules flag.
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date
from typing import Any

import duckdb
import structlog
from pydantic import BaseModel, Field

from trendscope.settings import DigestFiltersSettings

logger = structlog.get_logger(__name__)

# Metrics carried into the prompt / appendix when present for a flagged ticker.
METRIC_KEYS: tuple[str, ...] = (
    "return_5d",
    "return_21d",
    "momentum_rank_21d",
    "zscore",
    "rsi",
    "realized_vol",
    "vol_percentile",
    "volume_ratio",
    "relative_return_21d",
)


class FlaggedTicker(BaseModel):
    """A ticker the rules flagged, with human-readable reasons and raw metrics."""

    ticker: str
    reasons: list[str] = Field(default_factory=list)
    metrics: dict[str, float] = Field(default_factory=dict)


def _value(row: Mapping[Any, Any], name: str) -> float | None:
    v = row.get(name)
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return float(v)


def latest_signal_date(conn: duckdb.DuckDBPyConnection) -> date:
    row = conn.execute("SELECT max(date) FROM marts.fct_daily_signals").fetchone()
    if row is None or row[0] is None:
        raise ValueError(
            "no rows in marts.fct_daily_signals — run `trendscope ingest` and `make signals` first"
        )
    return row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))


def get_breadth(conn: duckdb.DuckDBPyConnection, as_of: date) -> dict[str, float] | None:
    """The market-breadth row for a date, or None if breadth isn't computed."""
    row = conn.execute(
        """
        SELECT tickers_observed, pct_above_slow_ma, pct_positive_21d, pct_volume_spike
        FROM marts.fct_market_breadth WHERE date = ?
        """,
        [as_of],
    ).fetchone()
    if row is None:
        return None
    keys = ("tickers_observed", "pct_above_slow_ma", "pct_positive_21d", "pct_volume_spike")
    return {k: float(v) for k, v in zip(keys, row, strict=True) if v is not None}


def flag_interesting(
    conn: duckdb.DuckDBPyConnection,
    filters: DigestFiltersSettings,
    as_of: date | None = None,
) -> tuple[date, list[FlaggedTicker]]:
    """Apply the digest rules for a date (default: latest available).

    Returns (date used, flagged tickers sorted by reason count, capped at
    max_tickers_per_digest).
    """
    as_of = as_of or latest_signal_date(conn)

    # Fetch long-form and pivot in pandas: DuckDB's PIVOT cannot combine
    # data-derived pivot values with bound parameters.
    long = conn.execute(
        "SELECT ticker, signal_name, value FROM marts.fct_daily_signals WHERE date = ?",
        [as_of],
    ).fetchdf()
    wide = (
        long.pivot(index="ticker", columns="signal_name", values="value")
        .reset_index()
        .rename_axis(None, axis=1)
    )

    flagged: list[FlaggedTicker] = []
    for row in wide.to_dict("records"):
        ticker = str(row["ticker"])
        reasons: list[str] = []

        if _value(row, "golden_cross") == 1.0:
            reasons.append("golden cross: fast MA crossed above slow MA today")
        if _value(row, "death_cross") == 1.0:
            reasons.append("death cross: fast MA crossed below slow MA today")

        zscore = _value(row, "zscore")
        if zscore is not None and abs(zscore) >= filters.zscore_threshold:
            reasons.append(f"price z-score {zscore:+.1f} vs its trailing window")

        if _value(row, "volume_spike") == 1.0:
            ratio = _value(row, "volume_ratio")
            reasons.append(
                f"volume {ratio:.1f}x its prior average"
                if ratio is not None
                else "volume spike vs prior average"
            )

        rank = _value(row, "momentum_rank_21d")
        if rank is not None:
            if rank >= filters.momentum_rank_extreme:
                reasons.append(f"21d momentum near top of universe (rank {rank:.2f})")
            elif rank <= 1 - filters.momentum_rank_extreme:
                reasons.append(f"21d momentum near bottom of universe (rank {rank:.2f})")

        vol_pct = _value(row, "vol_percentile")
        if vol_pct is not None and vol_pct >= filters.vol_percentile_threshold:
            reasons.append(f"realized vol in the {vol_pct:.0%} percentile of its past year")

        rel = _value(row, "relative_return_21d")
        if rel is not None and abs(rel) >= filters.rel_return_threshold:
            reasons.append(f"{rel:+.1%} vs sector benchmark over 21d")

        if reasons:
            metrics = {k: v for k in METRIC_KEYS if (v := _value(row, k)) is not None}
            flagged.append(FlaggedTicker(ticker=ticker, reasons=reasons, metrics=metrics))

    flagged.sort(key=lambda f: (-len(f.reasons), f.ticker))
    capped = flagged[: filters.max_tickers_per_digest]
    logger.info(
        "digest_filters_applied",
        as_of=as_of.isoformat(),
        candidates=len(wide),
        flagged=len(flagged),
        kept=len(capped),
    )
    return as_of, capped
