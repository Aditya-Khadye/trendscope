-- Trendscope DuckDB schema.
--
-- Storage timestamps are UTC; the display layer converts to ET.
-- Each table has a stable PK so re-ingesting the same date is idempotent
-- (INSERT OR REPLACE / ON CONFLICT DO UPDATE).

-- ---------------------------------------------------------------------------
-- prices: one row per (ticker, trading day).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prices (
    date         DATE        NOT NULL,
    ticker       VARCHAR     NOT NULL,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE,
    adj_close    DOUBLE,                   -- split- and dividend-adjusted
    volume       BIGINT,
    dividend     DOUBLE      DEFAULT 0.0,  -- cash dividend paid that day
    split_ratio  DOUBLE      DEFAULT 1.0,  -- ratio if a split occurred (1.0 = none)
    data_source  VARCHAR     NOT NULL DEFAULT 'yfinance',
    ingested_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices(ticker, date);

-- ---------------------------------------------------------------------------
-- tickers: metadata about each instrument we track.
-- Populated/refreshed by ingest from universe.yaml + yfinance .info.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tickers (
    ticker       VARCHAR     PRIMARY KEY,
    name         VARCHAR,
    sector       VARCHAR,
    industry     VARCHAR,
    asset_type   VARCHAR,                  -- 'stock' | 'etf' | 'index'
    groups       VARCHAR[],                -- e.g. ['mega_cap', 'holdings']
    benchmark    VARCHAR,                  -- sector ETF for relative strength
    added_at     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- signals: long-form output of every signal function.
-- One row per (date, ticker, signal_name). Phase 2 writes here.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS signals (
    date         DATE        NOT NULL,
    ticker       VARCHAR     NOT NULL,
    signal_name  VARCHAR     NOT NULL,
    value        DOUBLE,                   -- numeric output (z-score, pct, raw)
    payload      JSON,                     -- optional structured detail (e.g. {"fast": 50, "slow": 200})
    computed_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, ticker, signal_name)
);

CREATE INDEX IF NOT EXISTS idx_signals_name_date ON signals(signal_name, date);

-- ---------------------------------------------------------------------------
-- runs: audit log for every CLI invocation that mutates data.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS runs (
    run_id          VARCHAR     PRIMARY KEY,           -- UUIDv4
    kind            VARCHAR     NOT NULL,              -- 'ingest' | 'signals' | 'digest'
    started_at      TIMESTAMP   NOT NULL,
    finished_at     TIMESTAMP,
    status          VARCHAR     NOT NULL,              -- 'running' | 'success' | 'partial' | 'error'
    universe_size   INTEGER,                           -- tickers in scope for this run
    rows_written    INTEGER,                           -- new or replaced rows
    rows_skipped    INTEGER,                           -- already-present rows
    error_message   VARCHAR,
    metadata        JSON                               -- run-specific context (date range, args, etc.)
);

CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at);
CREATE INDEX IF NOT EXISTS idx_runs_kind_started ON runs(kind, started_at);
