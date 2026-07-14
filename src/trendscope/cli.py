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
        help="Incremental load from the latest loaded date per ticker.",
    ),
) -> None:
    """Extract prices from yfinance and load into the raw DuckDB schema."""
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
        f"[{summary['status']}] mode={summary['mode']} "
        f"processed={summary['tickers_processed']} "
        f"skipped={summary['tickers_skipped']} "
        f"failed={summary['tickers_failed']} "
        f"appended={summary['rows_appended']} "
        f"unchanged={summary['rows_unchanged']} "
        f"load_id={summary['load_id']}"
    )

    if summary["status"] == "partial":
        raise typer.Exit(code=1)


@app.command("migrate-legacy")
def migrate_legacy_cmd() -> None:
    """One-time migration of the pre-ELT schema into raw. No-op on fresh databases."""
    _bootstrap_logging()
    settings = get_settings()
    conn = ingest.connect(settings.paths.duckdb)
    try:
        summary = ingest.migrate_legacy(conn)
    finally:
        conn.close()

    if summary["status"] == "no_legacy":
        typer.echo("No legacy tables found — nothing to migrate.")
    else:
        typer.echo(
            f"[{summary['status']}] prices={summary['prices_migrated']} "
            f"tickers={summary['tickers_migrated']} "
            f"dropped={','.join(summary['legacy_tables_dropped'])}"
        )


@app.command()
def signals() -> None:
    """Signal computation is owned by dbt — run `make signals` (dbt build)."""
    typer.echo("Signals are computed by dbt. Run: make signals  (or: cd dbt && dbt build)")
    raise typer.Exit(code=2)


@app.command()
def digest(
    date_str: str | None = typer.Option(
        None, "--date", help="YYYY-MM-DD; defaults to the latest available signal date."
    ),
    no_llm: bool = typer.Option(
        False, "--no-llm", help="Skip the LLM narrative even if an API key is configured."
    ),
) -> None:
    """Render the daily markdown digest from the signal marts (+ optional LLM narrative)."""
    _bootstrap_logging()
    from trendscope.digest.pipeline import run_digest

    path = run_digest(
        use_llm=False if no_llm else None,
        as_of=date.fromisoformat(date_str) if date_str else None,
    )
    typer.echo(path.read_text())
    typer.echo(f"[written] {path}")


@app.command()
def daily() -> None:
    """Run ingest + signals + digest end-to-end (use `make daily` or the Airflow DAG)."""
    typer.echo("Use `make daily` locally, or the trendscope_daily Airflow DAG.")
    raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
