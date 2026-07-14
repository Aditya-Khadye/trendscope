-- Simple moving averages with strict full-window semantics: an MA is NULL
-- until its entire lookback exists (no partial-window averages, no
-- look-ahead). Cross flags fire only on the day the fast MA moves across
-- the slow MA.

{% set fast = var('fast_ma_window') %}
{% set slow = var('slow_ma_window') %}

with base as (

    select date, ticker, adj_close
    from {{ ref('stg_prices') }}

),

mas as (

    select
        date,
        ticker,
        adj_close,
        case
            when count(adj_close) over w_fast = {{ fast }}
            then avg(adj_close) over w_fast
        end as ma_fast,
        case
            when count(adj_close) over w_slow = {{ slow }}
            then avg(adj_close) over w_slow
        end as ma_slow
    from base
    window
        w_fast as (
            partition by ticker order by date
            rows between {{ fast - 1 }} preceding and current row
        ),
        w_slow as (
            partition by ticker order by date
            rows between {{ slow - 1 }} preceding and current row
        )

)

select
    date,
    ticker,
    adj_close,
    ma_fast,
    ma_slow,
    case when ma_slow is not null then adj_close > ma_slow end as above_slow_ma,
    case
        when ma_fast is not null and ma_slow is not null
             and lag(ma_fast) over t is not null and lag(ma_slow) over t is not null
        then ma_fast > ma_slow and lag(ma_fast) over t <= lag(ma_slow) over t
    end as golden_cross,
    case
        when ma_fast is not null and ma_slow is not null
             and lag(ma_fast) over t is not null and lag(ma_slow) over t is not null
        then ma_fast < ma_slow and lag(ma_fast) over t >= lag(ma_slow) over t
    end as death_cross

from mas
window t as (partition by ticker order by date)
