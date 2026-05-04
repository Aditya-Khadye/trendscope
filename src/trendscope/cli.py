"""Trendscope command-line interface."""
from __future__ import annotations

from datetime import date

import structlog
import typer

from trendscope import log
from trendscope.data import ingest
from trendscope.settings import get_settings
from trendscope.universe import Universe

app = typer.Typer(
    help="Trendscope: descriptive market-trend analytics.",
    no_args_is_help=True,
    add_completion=False,
)

logger = structlog.get_logger("trendscope.cli")


def _bootstrap_logging() -> None:
    settings = get_settings()
    log.configure(level=settings.logging.level, format=settings.logging.format)


@app.command("ingest")
def ingest_cmd(
    since: str | None = typer.Option(
        None,
        "--since",
        help="Backfill from YYYY-MM-DD (inclusive). Mutually exclusive with --daily.",
    ),
    daily: bool = typer.Option(
        False,
        "--daily",
        help="Idempotent incremental ingest from the latest stored date per ticker.",
    ),
) -> None:
    """Pull price data into DuckDB."""
    _bootstrap_logging()

    if (since is None) == (not daily):
        typer.echo("Error: must pass exactly one of --since YYYY-MM-DD or --daily", err=True)
        raise typer.Exit(code=2)

    settings = get_settings()
    universe = Universe.from_yaml(settings.paths.universe_yaml)

    since_date = date.fromisoformat(since) if since else None

    conn = ingest.connect(settings.paths.duckdb)
    try:
        summary = ingest.run_ingest(
            conn=conn,
            universe=universe,
            settings=settings.ingest,
            since=since_date,
            daily=daily,
        )
    finally:
        conn.close()

    typer.echo(
        f"[{summary['status']}] "
        f"processed={summary['tickers_processed']} "
        f"skipped={summary['tickers_skipped']} "
        f"failed={summary['tickers_failed']} "
        f"rows={summary['rows_written']} "
        f"run_id={summary['run_id']}"
    )

    if summary["status"] == "partial":
        raise typer.Exit(code=1)


@app.command()
def signals() -> None:
    """Compute the signals table from prices. (Phase 2.)"""
    raise NotImplementedError("signals is Phase 2")


@app.command()
def digest() -> None:
    """Render the daily LLM-narrated markdown digest. (Phase 3.)"""
    raise NotImplementedError("digest is Phase 3")


@app.command()
def daily() -> None:
    """Run ingest + signals + digest end-to-end."""
    raise NotImplementedError("daily wires up once ingest, signals, and digest exist")


if __name__ == "__main__":
    app()
