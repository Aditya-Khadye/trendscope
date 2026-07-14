"""Headline fetching per flagged ticker. yfinance today, Polygon later.

News is garnish, not signal: any failure degrades to an empty list and the
digest carries on. Handles both yfinance news schemas (the flat pre-2024
shape and the nested `content` shape).
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import structlog
import yfinance as yf
from pydantic import BaseModel

logger = structlog.get_logger(__name__)

NewsSource = Callable[[str], list[dict[str, Any]]]


class Headline(BaseModel):
    title: str
    publisher: str | None = None
    published: str | None = None  # ISO-ish display string


def _yfinance_news(ticker: str) -> list[dict[str, Any]]:
    return yf.Ticker(ticker).news or []


def _parse_item(item: dict[str, Any]) -> Headline | None:
    content = item.get("content")
    if isinstance(content, dict):  # current yfinance shape
        title = content.get("title")
        provider = content.get("provider")
        publisher = provider.get("displayName") if isinstance(provider, dict) else None
        published = content.get("pubDate")
    else:  # legacy flat shape
        title = item.get("title")
        publisher = item.get("publisher")
        ts = item.get("providerPublishTime")
        published = (
            datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d %H:%M UTC")
            if isinstance(ts, int | float)
            else None
        )
    if not title:
        return None
    return Headline(title=str(title), publisher=publisher, published=published)


def fetch_headlines(
    ticker: str,
    limit: int = 5,
    source: NewsSource | None = None,
) -> list[Headline]:
    """Up to `limit` parsed headlines for a ticker; [] on any failure."""
    src = source or _yfinance_news
    try:
        items = src(ticker)
    except Exception as e:
        logger.warning("news_fetch_failed", ticker=ticker, error=str(e))
        return []
    parsed = (_parse_item(i) for i in items)
    return [h for h in parsed if h is not None][:limit]
