-- Trendscope raw layer (extract/load target).
--
-- Design rules:
--   * Append-only: no UPDATE or DELETE, ever. Restated upstream data
--     (e.g. adj_close shifting after a dividend) appends a NEW VERSION of
--     the row; downstream dbt staging resolves latest-wins per key.
--   * Every row carries `_source` (provenance) and `_loaded_at` (UTC).
--   * No primary keys on raw tables — multiple versions per natural key
--     are the point. Uniqueness is enforced downstream by dbt tests.
--   * Raw preserves upstream quirks (yfinance's 0.0 = "no split", NaN
--     rows at series edges). Cleaning is a staging concern.
--
-- Transform tables (staging/marts) are owned by dbt, not this file.

CREATE SCHEMA IF NOT EXISTS raw;

-- ---------------------------------------------------------------------------
-- raw.prices: versioned daily OHLCV. One row per (date, ticker, _source,
-- _loaded_at); the latest _loaded_at per (date, ticker) is the current view.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.prices (
    date         DATE        NOT NULL,
    ticker       VARCHAR     NOT NULL,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    close        DOUBLE,
    adj_close    DOUBLE,
    volume       BIGINT,
    dividend     DOUBLE,
    split_ratio  DOUBLE,                    -- yfinance convention: 0.0 = no split
    _source      VARCHAR     NOT NULL,
    _loaded_at   TIMESTAMP   NOT NULL       -- UTC, set by the loader
);

-- ---------------------------------------------------------------------------
-- raw.tickers: versioned instrument metadata. A new version is appended only
-- when content changes (first sighting, group/benchmark edit, info refresh).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.tickers (
    ticker       VARCHAR     NOT NULL,
    name         VARCHAR,
    sector       VARCHAR,
    industry     VARCHAR,
    asset_type   VARCHAR,                   -- 'stock' | 'etf' | 'index' | ...
    groups       VARCHAR[],                 -- universe group memberships
    benchmark    VARCHAR,                   -- sector ETF for relative strength
    _source      VARCHAR     NOT NULL,
    _loaded_at   TIMESTAMP   NOT NULL       -- UTC, set by the loader
);

-- ---------------------------------------------------------------------------
-- raw.load_log: audit log for the Python extract/load step only.
-- Transform observability lives in Airflow task logs + dbt artifacts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.load_log (
    load_id         VARCHAR     PRIMARY KEY,        -- UUIDv4
    mode            VARCHAR     NOT NULL,           -- 'backfill' | 'daily' | 'migration'
    started_at      TIMESTAMP   NOT NULL,           -- UTC
    finished_at     TIMESTAMP,                      -- UTC
    status          VARCHAR     NOT NULL,           -- 'running' | 'success' | 'partial' | 'error'
    universe_size   INTEGER,
    rows_appended   INTEGER,                        -- new version rows written
    rows_unchanged  INTEGER,                        -- fetched but identical to latest version
    tickers_failed  INTEGER,
    error_message   VARCHAR,
    metadata        JSON
);
