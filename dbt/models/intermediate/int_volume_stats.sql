-- Volume anomaly vs the PRIOR n-day average (the trailing window excludes
-- the current row so a spike doesn't inflate its own baseline). NULL until
-- the full prior window exists.

{% set n = var('volume_avg_window') %}
{% set mult = var('volume_spike_multiplier') %}

with base as (

    select date, ticker, volume
    from {{ ref('stg_prices') }}

)

select
    date,
    ticker,
    volume,
    case
        when count(volume) over w_prior = {{ n }}
        then avg(volume) over w_prior
    end as avg_volume_prior,
    case
        when count(volume) over w_prior = {{ n }}
        then volume / nullif(avg(volume) over w_prior, 0)
    end as volume_ratio,
    case
        when count(volume) over w_prior = {{ n }} and volume is not null
        then volume >= {{ mult }} * avg(volume) over w_prior
    end as volume_spike

from base
window w_prior as (
    partition by ticker order by date
    rows between {{ n }} preceding and 1 preceding
)
