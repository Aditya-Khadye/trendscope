.PHONY: help install ingest signals dbt-docs digest daily test typecheck lint format check clean airflow-start airflow-stop airflow-test

help:
	@echo "Targets:"
	@echo "  install     uv sync + dbt deps"
	@echo "  ingest      pull latest prices into DuckDB (raw schema)"
	@echo "  signals     dbt build: staging -> intermediate -> marts + tests"
	@echo "  dbt-docs    dbt docs generate (lineage in dbt/target/)"
	@echo "  digest      LLM-narrated markdown digest   (Phase 3)"
	@echo "  daily       ingest + signals + digest"
	@echo "  test        run pytest"
	@echo "  typecheck   mypy --strict src"
	@echo "  lint        ruff check"
	@echo "  format      ruff format + ruff check --fix"
	@echo "  check       lint + typecheck + test"
	@echo "  clean       remove caches and build artifacts"

install:
	uv sync
	cd dbt && uv run dbt deps

ingest:
	uv run trendscope ingest --daily

signals:
	cd dbt && uv run dbt build

dbt-docs:
	cd dbt && uv run dbt docs generate

digest:
	uv run trendscope digest

daily: ingest signals digest

airflow-start:
	astro dev start --no-browser

airflow-stop:
	astro dev stop

airflow-test:
	astro dev run dags test trendscope_daily

test:
	uv run pytest

typecheck:
	uv run mypy src

lint:
	uv run ruff check src tests

format:
	uv run ruff format src tests
	uv run ruff check --fix src tests

check: lint typecheck test

clean:
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov build dist
	rm -rf dbt/target dbt/dbt_packages dbt/logs
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
