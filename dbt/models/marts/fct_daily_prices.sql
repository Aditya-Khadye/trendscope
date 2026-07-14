-- Clean daily price fact. Incremental with a trailing reprocess window so
-- late upstream restatements (adj_close shifting after a dividend) heal on
-- the next run: the last lookback_days of rows are recomputed from staging
-- and merged on (date, ticker).

{{
    config(
        materialized='incremental',
        unique_key=['date', 'ticker'],
    )
}}

select
    date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    dividend,
    split_ratio,
    _source,
    _loaded_at

from {{ ref('stg_prices') }}

{% if is_incremental() %}
where date >= (
    select coalesce(max(date), date '1900-01-01') from {{ this }}
) - interval '{{ var("lookback_days") }} days'
{% endif %}
