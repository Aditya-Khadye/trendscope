"""Tests for the digest pipeline: filters, news parsing, prompt, render, e2e."""
from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest

from trendscope.digest import llm, news, render
from trendscope.digest.filters import FlaggedTicker, flag_interesting, get_breadth
from trendscope.digest.pipeline import run_digest
from trendscope.settings import (
    DigestFiltersSettings,
    DigestLLMSettings,
    PathsSettings,
    Settings,
)

AS_OF = date(2026, 7, 10)
OLDER = date(2026, 7, 9)


@pytest.fixture
def marts_conn(tmp_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    """A DuckDB with hand-built marts tables mimicking the dbt output."""
    # NB: file must not be named marts.duckdb — DuckDB registers the file as
    # a catalog under its stem, which would collide with CREATE SCHEMA marts.
    conn = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    conn.execute("CREATE SCHEMA marts")
    conn.execute(
        "CREATE TABLE marts.fct_daily_signals "
        "(date DATE, ticker VARCHAR, signal_name VARCHAR, value DOUBLE)"
    )
    conn.execute(
        "CREATE TABLE marts.fct_market_breadth (date DATE, tickers_observed BIGINT, "
        "pct_above_slow_ma DOUBLE, pct_positive_21d DOUBLE, pct_volume_spike DOUBLE)"
    )
    rows: list[tuple[date, str, str, float]] = [
        # AAA: golden cross + stretched z-score -> 2 reasons, flagged first
        (AS_OF, "AAA", "golden_cross", 1.0),
        (AS_OF, "AAA", "zscore", 2.5),
        (AS_OF, "AAA", "return_21d", 0.08),
        # BBB: volume spike only
        (AS_OF, "BBB", "volume_spike", 1.0),
        (AS_OF, "BBB", "volume_ratio", 3.2),
        # CCC: nothing interesting
        (AS_OF, "CCC", "zscore", 0.2),
        (AS_OF, "CCC", "momentum_rank_21d", 0.5),
        # DDD: extreme momentum rank
        (AS_OF, "DDD", "momentum_rank_21d", 0.95),
        # An older date that must be ignored when as_of defaults to latest
        (OLDER, "CCC", "golden_cross", 1.0),
    ]
    conn.executemany("INSERT INTO marts.fct_daily_signals VALUES (?, ?, ?, ?)", rows)
    conn.execute(
        "INSERT INTO marts.fct_market_breadth VALUES (?, 4, 0.75, 0.5, 0.25)", [AS_OF]
    )
    yield conn
    conn.close()


@pytest.fixture
def default_filters() -> DigestFiltersSettings:
    return DigestFiltersSettings()


# ---------------------------------------------------------------------------
# filters
# ---------------------------------------------------------------------------


def test_flag_interesting_applies_rules(
    marts_conn: duckdb.DuckDBPyConnection, default_filters: DigestFiltersSettings
) -> None:
    as_of, flagged = flag_interesting(marts_conn, default_filters)
    assert as_of == AS_OF  # defaults to latest date, ignoring OLDER
    tickers = [f.ticker for f in flagged]
    assert tickers == ["AAA", "BBB", "DDD"]  # AAA first (2 reasons); CCC absent
    aaa = flagged[0]
    assert len(aaa.reasons) == 2
    assert any("golden cross" in r for r in aaa.reasons)
    assert any("z-score +2.5" in r for r in aaa.reasons)
    assert aaa.metrics["return_21d"] == pytest.approx(0.08)
    assert any("volume 3.2x" in r for f in flagged if f.ticker == "BBB" for r in f.reasons)


def test_flag_interesting_explicit_date(
    marts_conn: duckdb.DuckDBPyConnection, default_filters: DigestFiltersSettings
) -> None:
    as_of, flagged = flag_interesting(marts_conn, default_filters, as_of=OLDER)
    assert as_of == OLDER
    assert [f.ticker for f in flagged] == ["CCC"]


def test_flag_interesting_respects_cap(
    marts_conn: duckdb.DuckDBPyConnection,
) -> None:
    capped = DigestFiltersSettings(max_tickers_per_digest=1)
    _, flagged = flag_interesting(marts_conn, capped)
    assert [f.ticker for f in flagged] == ["AAA"]  # highest reason count wins


def test_flag_interesting_empty_db_raises(tmp_path: Path) -> None:
    conn = duckdb.connect(str(tmp_path / "empty.duckdb"))
    conn.execute("CREATE SCHEMA marts")
    conn.execute(
        "CREATE TABLE marts.fct_daily_signals "
        "(date DATE, ticker VARCHAR, signal_name VARCHAR, value DOUBLE)"
    )
    with pytest.raises(ValueError, match="no rows"):
        flag_interesting(conn, DigestFiltersSettings())


def test_get_breadth(marts_conn: duckdb.DuckDBPyConnection) -> None:
    breadth = get_breadth(marts_conn, AS_OF)
    assert breadth is not None
    assert breadth["pct_above_slow_ma"] == pytest.approx(0.75)
    assert get_breadth(marts_conn, date(2020, 1, 1)) is None


# ---------------------------------------------------------------------------
# news parsing
# ---------------------------------------------------------------------------


def test_fetch_headlines_legacy_shape() -> None:
    items: list[dict[str, Any]] = [
        {"title": "Old shape headline", "publisher": "Wire", "providerPublishTime": 1720000000}
    ]
    out = news.fetch_headlines("FOO", source=lambda _t: items)
    assert len(out) == 1
    assert out[0].title == "Old shape headline"
    assert out[0].publisher == "Wire"
    assert out[0].published is not None


def test_fetch_headlines_nested_shape() -> None:
    items: list[dict[str, Any]] = [
        {
            "content": {
                "title": "New shape headline",
                "provider": {"displayName": "NewsCo"},
                "pubDate": "2026-07-10T12:00:00Z",
            }
        }
    ]
    out = news.fetch_headlines("FOO", source=lambda _t: items)
    assert out[0].title == "New shape headline"
    assert out[0].publisher == "NewsCo"


def test_fetch_headlines_limits_and_skips_untitled() -> None:
    items: list[dict[str, Any]] = [{"title": f"h{i}"} for i in range(10)] + [{"junk": 1}]
    out = news.fetch_headlines("FOO", limit=3, source=lambda _t: items)
    assert [h.title for h in out] == ["h0", "h1", "h2"]


def test_fetch_headlines_failure_returns_empty() -> None:
    def boom(_t: str) -> list[dict[str, Any]]:
        raise RuntimeError("no internet")

    assert news.fetch_headlines("FOO", source=boom) == []


# ---------------------------------------------------------------------------
# llm
# ---------------------------------------------------------------------------


def _flagged() -> list[FlaggedTicker]:
    return [
        FlaggedTicker(
            ticker="AAA",
            reasons=["golden cross: fast MA crossed above slow MA today"],
            metrics={"return_21d": 0.08},
        )
    ]


def test_build_prompt_contains_facts() -> None:
    headlines = {"AAA": [news.Headline(title="AAA wins big", publisher="Wire")]}
    prompt = llm.build_prompt(AS_OF, {"tickers_observed": 4, "pct_above_slow_ma": 0.75},
                              _flagged(), headlines)
    assert "2026-07-10" in prompt
    assert "AAA" in prompt
    assert "golden cross" in prompt
    assert "return_21d = 0.0800" in prompt
    assert "AAA wins big (Wire)" in prompt
    assert "75% above their slow MA" in prompt


def test_narrate_without_key_raises() -> None:
    with pytest.raises(llm.MissingAPIKeyError):
        llm.narrate("prompt", llm=DigestLLMSettings(), api_key=None)


def test_narrate_calls_anthropic(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    from anthropic.types import TextBlock

    class FakeMessages:
        def create(self, **kwargs: Any) -> Any:
            captured.update(kwargs)
            return SimpleNamespace(
                content=[TextBlock(type="text", text="A calm narrative.")]
            )

    class FakeClient:
        def __init__(self, api_key: str) -> None:
            captured["api_key"] = api_key
            self.messages = FakeMessages()

    monkeypatch.setattr(llm.anthropic, "Anthropic", FakeClient)
    out = llm.narrate("the prompt", llm=DigestLLMSettings(), api_key="sk-test")
    assert out == "A calm narrative."
    assert captured["api_key"] == "sk-test"
    assert captured["model"] == DigestLLMSettings().model
    assert captured["messages"][0]["content"] == "the prompt"
    assert "narration layer" in captured["system"]


# ---------------------------------------------------------------------------
# render
# ---------------------------------------------------------------------------


def test_render_digest_no_llm_placeholder() -> None:
    text = render.render_digest(AS_OF, {"tickers_observed": 4, "pct_above_slow_ma": 0.75,
                                        "pct_positive_21d": 0.5, "pct_volume_spike": 0.25},
                                _flagged(), {}, narrative=None)
    assert "# TrendScope Daily Digest — 2026-07-10" in text
    assert "not investment advice" in text
    assert "LLM narrative disabled" in text
    assert "### AAA" in text
    assert "golden cross" in text


def test_render_digest_with_narrative_and_headlines() -> None:
    headlines = {"AAA": [news.Headline(title="AAA wins big", publisher="Wire")]}
    text = render.render_digest(AS_OF, None, _flagged(), headlines, narrative="The story.")
    assert "The story." in text
    assert "AAA wins big — Wire" in text
    assert "Breadth stats unavailable" in text


def test_write_digest_creates_dated_file(tmp_path: Path) -> None:
    out = render.write_digest("hello\n", tmp_path / "digests", AS_OF)
    assert out == tmp_path / "digests" / "2026-07-10.md"
    assert out.read_text() == "hello\n"


# ---------------------------------------------------------------------------
# pipeline end-to-end (no LLM, no network)
# ---------------------------------------------------------------------------


def test_run_digest_end_to_end(
    marts_conn: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    marts_conn.close()  # pipeline opens its own read-only connection
    settings = Settings(
        paths=PathsSettings(
            duckdb=tmp_path / "warehouse.duckdb",
            digests=tmp_path / "digests",
            universe_yaml=tmp_path / "unused.yaml",
        )
    )
    path = run_digest(settings=settings, use_llm=False, news_source=lambda _t: [])
    assert path.name == "2026-07-10.md"
    text = path.read_text()
    assert "### AAA" in text
    assert "LLM narrative disabled" in text


def test_run_digest_missing_db_raises(tmp_path: Path) -> None:
    settings = Settings(
        paths=PathsSettings(
            duckdb=tmp_path / "nope.duckdb",
            digests=tmp_path / "digests",
            universe_yaml=tmp_path / "unused.yaml",
        )
    )
    with pytest.raises(FileNotFoundError, match="does not exist"):
        run_digest(settings=settings, use_llm=False)
