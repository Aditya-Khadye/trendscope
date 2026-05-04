# trendscope

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

## Daily flow

```bash
trendscope ingest --daily   # pull latest prices into DuckDB
trendscope signals          # Phase 2: compute signal table
trendscope digest           # Phase 3: LLM-narrated markdown
make daily                  # all of the above
```

Backfill from a specific date:

```bash
trendscope ingest --since 2020-01-01
```

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
