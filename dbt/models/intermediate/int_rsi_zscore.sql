-- Mean-reversion signals:
--   * rsi — Cutler's RSI (simple-average variant). Chosen over Wilder's
--     because Wilder's recursive smoothing doesn't fit window SQL; Cutler's
--     is a standard, documented substitute with the same 0-100 reading.
--   * zscore — adj_close z-score against its own trailing window.
-- Both are NULL until their full lookback exists.

{% set rsi_p = var('rsi_period') %}
{% set z_n = var('zscore_lookback') %}

with base as (

    select date, ticker, adj_close
    from {{ ref('stg_prices') }}

),

deltas as (

    select
        date,
        ticker,
        adj_close,
        adj_close - lag(adj_close) over (partition by ticker order by date) as delta
    from base

),

moves as (

    select
        *,
        greatest(delta, 0) as gain,
        greatest(-delta, 0) as loss
    from deltas

)

select
    date,
    ticker,
    case
        when count(delta) over w_rsi = {{ rsi_p }} then
            case
                when avg(loss) over w_rsi = 0 then 100.0
                else 100.0 - 100.0 / (1.0 + avg(gain) over w_rsi / avg(loss) over w_rsi)
            end
    end as rsi,
    case
        when count(adj_close) over w_z = {{ z_n }}
             and stddev_samp(adj_close) over w_z > 0
        then (adj_close - avg(adj_close) over w_z) / stddev_samp(adj_close) over w_z
    end as zscore

from moves
window
    w_rsi as (
        partition by ticker order by date
        rows between {{ rsi_p - 1 }} preceding and current row
    ),
    w_z as (
        partition by ticker order by date
        rows between {{ z_n - 1 }} preceding and current row
    )
