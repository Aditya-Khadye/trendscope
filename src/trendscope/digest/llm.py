"""Anthropic narration: structured signal context + headlines -> markdown.

The prompt hands the model ONLY deterministic numbers computed by dbt and
verbatim headlines; the system prompt forbids inventing or extrapolating.
The LLM is a narration layer, never a signal source.
"""
from __future__ import annotations

from datetime import date

import anthropic
import structlog
from anthropic.types import TextBlock

from trendscope.digest.filters import FlaggedTicker
from trendscope.digest.news import Headline
from trendscope.settings import DigestLLMSettings

logger = structlog.get_logger(__name__)


class MissingAPIKeyError(RuntimeError):
    """Raised when narration is requested without an ANTHROPIC_API_KEY."""


SYSTEM_PROMPT = """\
You are the narration layer of a personal, descriptive market-analytics tool.
You receive deterministic daily signal values and recent headlines for a small
set of flagged tickers. Write a concise markdown brief.

Hard rules:
- Descriptive, not predictive. Never forecast, recommend, or imply trades.
- Use ONLY the numbers provided, quoted verbatim. Never invent or extrapolate
  figures, price levels, or dates.
- If headlines and signals point different directions, note the tension
  plainly instead of resolving it.
- Structure: one short market-context paragraph first (from the breadth
  stats), then one compact bullet block per ticker explaining what changed
  today in plain English.
- Analyst tone, no hype, no emojis. Do not add disclaimers (the report
  template already carries one).
"""


def _format_breadth(as_of: date, breadth: dict[str, float] | None) -> str:
    if not breadth:
        return "Breadth stats unavailable for this date."
    return (
        f"Universe breadth on {as_of.isoformat()}: "
        f"{breadth.get('tickers_observed', 0):.0f} tickers observed; "
        f"{breadth.get('pct_above_slow_ma', 0) * 100:.0f}% above their slow MA; "
        f"{breadth.get('pct_positive_21d', 0) * 100:.0f}% with positive 21d returns; "
        f"{breadth.get('pct_volume_spike', 0) * 100:.0f}% with a volume spike."
    )


def build_prompt(
    as_of: date,
    breadth: dict[str, float] | None,
    flagged: list[FlaggedTicker],
    headlines: dict[str, list[Headline]],
) -> str:
    """Assemble the structured user prompt for the narration call."""
    lines: list[str] = [
        f"Trading date: {as_of.isoformat()}",
        "",
        _format_breadth(as_of, breadth),
        "",
        f"Flagged tickers ({len(flagged)}):",
    ]
    for f in flagged:
        lines.append(f"\n## {f.ticker}")
        lines.append("Why flagged (rule output, verbatim):")
        lines.extend(f"- {r}" for r in f.reasons)
        if f.metrics:
            lines.append("Signal values:")
            lines.extend(f"- {k} = {v:.4f}" for k, v in sorted(f.metrics.items()))
        ticker_news = headlines.get(f.ticker, [])
        if ticker_news:
            lines.append("Recent headlines (verbatim, may be noise):")
            for h in ticker_news:
                suffix = f" ({h.publisher})" if h.publisher else ""
                lines.append(f"- {h.title}{suffix}")
        else:
            lines.append("Recent headlines: none available.")
    return "\n".join(lines)


def narrate(prompt: str, *, llm: DigestLLMSettings, api_key: str | None) -> str:
    """Call the Anthropic API and return the markdown narrative."""
    if not api_key:
        raise MissingAPIKeyError(
            "ANTHROPIC_API_KEY is not configured — set it in .env or run with --no-llm"
        )
    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=llm.model,
        max_tokens=llm.max_tokens,
        temperature=llm.temperature,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    text = "\n".join(
        block.text for block in response.content if isinstance(block, TextBlock)
    ).strip()
    logger.info("digest_narrated", model=llm.model, chars=len(text))
    return text
