.PHONY: help install ingest signals digest daily test typecheck lint format check clean

help:
	@echo "Targets:"
	@echo "  install     uv sync (creates .venv, installs deps)"
	@echo "  ingest      pull latest prices into DuckDB"
	@echo "  signals     compute signals table          (Phase 2)"
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

ingest:
	uv run trendscope ingest --daily

signals:
	uv run trendscope signals

digest:
	uv run trendscope digest

daily: ingest signals digest

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
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
