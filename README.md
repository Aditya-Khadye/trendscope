# trendscope

[![CI](https://github.com/Aditya-Khadye/trendscope/actions/workflows/ci.yml/badge.svg)](https://github.com/Aditya-Khadye/trendscope/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![mypy: strict](https://img.shields.io/badge/mypy-strict-blue)](https://mypy.readthedocs.io/)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Personal market-trend detection. Surfaces what's changing across a configurable
universe of tickers each day so I can make my own calls. **Descriptive analytics
only — no prediction, no autonomous trading, no alpha claims.** The LLM layer
narrates structured signal output and headlines; all signal logic is
deterministic Python.

## Stack

- Python 3.11+ with [uv](https://github.com/astral-sh/uv) for env / packaging
- DuckDB single-file storage; yfinance for data (Phase 1)
- pandas / numpy for analytics; pandas-ta or equivalent in Phase 2
- Anthropic SDK (`claude-sonnet-4-6`) for narrative digests
- Streamlit dashboard, Polygon/FMP upgrade — out of scope for now

## Setup

```bash
brew install uv         # one time
uv sync                 # creates .venv, installs deps
cp .env.example .env    # add ANTHROPIC_API_KEY
```

### Private holdings (optional)

To track real positions without committing them, create
`config/universe.local.yaml` (gitignored). It overlays `universe.yaml`:

```yaml
# config/universe.local.yaml — never committed
groups:
  holdings:
    description: "Real positions"
    tickers: [AAPL, NVDA, ...]   # replaces the placeholder holdings group

sector_etf_map:
  COIN: XLF                       # adds/overrides individual mappings
```

`Universe.from_yaml()` automatically merges this on top of the base file.

## Daily flow

```bash
make ingest    # yfinance -> DuckDB raw schema (idempotent)
make signals   # dbt build: staging -> marts + 44 tests
make digest    # markdown digest (LLM narrative if ANTHROPIC_API_KEY set)
make daily     # all of the above
```

Backfill from a specific date:

```bash
uv run trendscope ingest --since 2020-01-01
```

## Airflow (local, via Astro CLI)

The same pipeline runs as an Airflow 3 DAG (`trendscope_daily`) with each
dbt model and test as an individual task via astronomer-cosmos.

```bash
brew install astro           # one time; Docker Desktop must be running
astro dev start              # build image + start Airflow at http://localhost:8080
astro dev run dags test trendscope_daily   # run the whole DAG once, in-process
astro dev stop               # shut the stack down
```

Notes:

- The container's DuckDB lives at `include/data/trendscope.duckdb` (host-
  mounted, gitignored). The first run bootstraps a full backfill.
- Digests land in `include/digests/` on the host.
- Add `ANTHROPIC_API_KEY` to `.env` (see `.env.example`) and restart to
  enable the LLM narrative; without it the digest still renders,
  deterministically, with a note.
- Schedule: 18:00 America/New_York weekdays, `catchup=False`, retries with
  exponential backoff, all tasks serialized (DuckDB is single-writer).

## Repo layout

```
config/                       universe + settings YAML (edit freely)
src/trendscope/
  data/                       ingestion + DuckDB schema
  signals/                    pure DataFrame -> DataFrame signal functions
  digest/                     filters, news, LLM, markdown render
  cli.py                      typer entry point
tests/
data/                         DuckDB lives here (gitignored)
digests/                      dated markdown output (gitignored)
```

## Architectural principles

- **Descriptive, not predictive.** Signals describe present-day state. Position
  sizing, entry/exit, and conviction are decisions for me, not the system.
- **Deterministic core, narrative shell.** Every number comes from Python; the
  LLM only summarizes structured input.
- **No look-ahead bias.** Features at date *t* use only data ≤ *t*, even though
  we're not forecasting — keeps the code valid if scope ever expands.
- **Config-driven.** Tickers, thresholds, and lookbacks live in YAML, not code.
- **Idempotent ingest.** Rerunning `--daily` doesn't duplicate rows.
- **UTC in storage, ET on display.** All timestamps are stored in UTC.

## Phase status

- ✅ Phase 1 — scaffolding, schema, idempotent ingest
- ⏳ Phase 2 — signals library
- ⏳ Phase 3 — LLM digest pipeline
- ⏳ Phase 4 — Streamlit dashboard
