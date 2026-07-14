-- Realized volatility (annualized stddev of daily log returns) and its
-- trailing percentile within the ticker's own history. The percentile is a
-- strict trailing computation: at date t it compares today's vol only
-- against the previous {{ var('vol_percentile_lookback') }} observations
-- ending at t. NULL until the full lookback exists.

{% set v_n = var('realized_vol_window') %}
{% set p_n = var('vol_percentile_lookback') %}

with base as (

    select date, ticker, adj_close
    from {{ ref('stg_prices') }}

),

log_returns as (

    select
        date,
        ticker,
        ln(adj_close / nullif(lag(adj_close) over (partition by ticker order by date), 0))
            as log_return
    from base

),

vols as (

    select
        date,
        ticker,
        case
            when count(log_return) over w_vol = {{ v_n }}
            then stddev_samp(log_return) over w_vol * sqrt(252.0)
        end as realized_vol
    from log_returns
    window w_vol as (
        partition by ticker order by date
        rows between {{ v_n - 1 }} preceding and current row
    )

),

trails as (

    select
        *,
        list(realized_vol) over (
            partition by ticker order by date
            rows between {{ p_n - 1 }} preceding and current row
        ) as _trail
    from vols

)

select
    date,
    ticker,
    realized_vol,
    case
        when realized_vol is not null
             and len(list_filter(_trail, x -> x is not null)) = {{ p_n }}
        then len(list_filter(_trail, x -> x <= realized_vol)) * 1.0 / {{ p_n }}
    end as vol_percentile

from trails
