"""trendscope_daily: extract/load -> dbt build (Cosmos) -> digest -> publish.

Runs weekdays after US market close. Every task touches the same DuckDB
file and DuckDB is single-writer, so the DAG serializes task execution
(max_active_tasks=1) instead of juggling pools — boring and sufficient at
this scale.
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.sdk import Asset, dag, task
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import InvocationMode

logger = logging.getLogger(__name__)

DBT_DIR = Path(os.environ.get("TRENDSCOPE_DBT_DIR", "/usr/local/airflow/dbt"))
# dbt lives in its own py3.12 venv (see Dockerfile) — Cosmos shells out to it.
DBT_EXECUTABLE = os.environ.get("TRENDSCOPE_DBT_EXECUTABLE", "/home/astro/dbt-venv/bin/dbt")

# Emitted when a load completes; lets future DAGs schedule off raw-data
# updates without coupling to this DAG's id.
RAW_PRICES_ASSET = Asset("duckdb://trendscope/raw/prices")


def _log_failure(context: dict[str, Any]) -> None:
    """Failure callback: one unmissable, grep-able line in the task log."""
    ti = context.get("task_instance")
    logger.error(
        "TRENDSCOPE TASK FAILED dag=%s task=%s run_id=%s try=%s error=%r",
        getattr(ti, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        context.get("run_id", "?"),
        getattr(ti, "try_number", "?"),
        context.get("exception"),
    )


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "trendscope",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": _log_failure,
}


@dag(
    dag_id="trendscope_daily",
    description="Daily market ELT -> dbt signal marts -> LLM digest",
    schedule="0 18 * * 1-5",  # 18:00 weekdays; timezone comes from start_date
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # DuckDB is single-writer: serialize all tasks
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["trendscope"],
)
def trendscope_daily() -> None:
    @task(outlets=[RAW_PRICES_ASSET])
    def extract_load() -> dict[str, Any]:
        """Idempotent incremental load: yfinance -> raw.prices / raw.tickers.

        On a fresh database this bootstraps a full backfill from
        ingest.default_start; on subsequent runs it appends only new dates.
        """
        from trendscope.data import ingest
        from trendscope.settings import get_settings
        from trendscope.universe import Universe

        settings = get_settings()
        conn = ingest.connect(settings.paths.duckdb)
        try:
            summary = ingest.run_ingest(
                conn=conn,
                universe=Universe.from_yaml(settings.paths.universe_yaml),
                settings=settings.ingest,
                daily=True,
            )
        finally:
            conn.close()
        if summary["status"] != "success":
            raise RuntimeError(
                f"extract_load status={summary['status']} failures={summary['failures']}"
            )
        return summary

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(dbt_project_path=DBT_DIR),
        profile_config=ProfileConfig(
            profile_name="trendscope",
            target_name="dev",
            profiles_yml_filepath=DBT_DIR / "profiles.yml",
        ),
        # SUBPROCESS everywhere: shell out to the dedicated py3.12 dbt venv —
        # the in-process DBT_RUNNER default can't work on the image's py3.14.
        # NB: RenderConfig has its own invocation_mode; both must be set.
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        render_config=RenderConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
    )

    @task
    def generate_llm_digest() -> str:
        """Render the digest; LLM narrative only when ANTHROPIC_API_KEY is set."""
        from trendscope.digest.pipeline import run_digest
        from trendscope.settings import get_settings

        use_llm = get_settings().anthropic_api_key is not None
        if not use_llm:
            logger.warning(
                "ANTHROPIC_API_KEY not set — writing digest without narrative "
                "(add it to .env to enable narration)"
            )
        return str(run_digest(use_llm=use_llm))

    @task
    def publish_digest(path: str) -> None:
        """Log the digest body so the run itself carries the day's output."""
        text = Path(path).read_text()
        logger.info("digest written: %s (%d chars)", path, len(text))
        for line in text.splitlines():
            logger.info("| %s", line)

    digest_path = generate_llm_digest()
    extract_load() >> dbt_build >> digest_path
    publish_digest(digest_path)


trendscope_daily()
