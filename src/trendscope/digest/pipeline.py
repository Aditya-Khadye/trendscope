"""End-to-end digest run: filters -> news -> (LLM) -> markdown file.

Shared by the CLI (`trendscope digest`) and the Airflow task.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import structlog

from trendscope.digest.filters import flag_interesting, get_breadth
from trendscope.digest.llm import build_prompt, narrate
from trendscope.digest.news import NewsSource, fetch_headlines
from trendscope.digest.render import render_digest, write_digest
from trendscope.settings import Settings, get_settings

logger = structlog.get_logger(__name__)


def run_digest(
    *,
    settings: Settings | None = None,
    use_llm: bool | None = None,
    as_of: date | None = None,
    news_source: NewsSource | None = None,
) -> Path:
    """Produce the digest for a date (default: latest signal date).

    use_llm=None auto-detects: narrate when an ANTHROPIC_API_KEY is
    configured, otherwise emit the deterministic report with a note.
    """
    settings = settings or get_settings()

    db_path = settings.paths.duckdb
    if not db_path.exists():
        raise FileNotFoundError(
            f"{db_path} does not exist — run `trendscope ingest` and `make signals` first"
        )

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        as_of_resolved, flagged = flag_interesting(conn, settings.digest.filters, as_of)
        breadth = get_breadth(conn, as_of_resolved)
    finally:
        conn.close()

    headlines = {
        f.ticker: fetch_headlines(
            f.ticker, settings.digest.news.headlines_per_ticker, source=news_source
        )
        for f in flagged
    }

    if use_llm is None:
        use_llm = settings.anthropic_api_key is not None

    narrative: str | None = None
    if use_llm:
        api_key = (
            settings.anthropic_api_key.get_secret_value()
            if settings.anthropic_api_key
            else None
        )
        narrative = narrate(
            build_prompt(as_of_resolved, breadth, flagged, headlines),
            llm=settings.digest.llm,
            api_key=api_key,
        )
    else:
        logger.info("digest_llm_disabled")

    text = render_digest(as_of_resolved, breadth, flagged, headlines, narrative)
    path = write_digest(text, settings.paths.digests, as_of_resolved)
    logger.info(
        "digest_written",
        path=str(path),
        as_of=as_of_resolved.isoformat(),
        flagged=len(flagged),
        llm=use_llm,
    )
    return path
