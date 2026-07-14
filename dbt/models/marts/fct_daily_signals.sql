-- Long-form signal fact: one row per (date, ticker, signal_name) with a
-- numeric value — the direct successor of the v1 Python `signals` table
-- contract, now assembled from the intermediate models and unpivoted.
-- Boolean signals are cast to 0/1. NULL values (insufficient history) are
-- dropped: absence of a row means "not computable that day".

{{
    config(
        materialized='incremental',
        unique_key=['date', 'ticker', 'signal_name'],
    )
}}

with wide as (

    select
        r.date,
        r.ticker,
        r.return_5d,
        r.return_21d,
        r.return_63d,
        r.return_252d,
        r.momentum_rank_5d,
        r.momentum_rank_21d,
        r.momentum_rank_63d,
        r.momentum_rank_252d,
        cast(ma.above_slow_ma as double) as above_slow_ma,
        cast(ma.golden_cross as double) as golden_cross,
        cast(ma.death_cross as double) as death_cross,
        rz.rsi,
        rz.zscore,
        rv.realized_vol,
        rv.vol_percentile,
        vs.volume_ratio,
        cast(vs.volume_spike as double) as volume_spike,
        rs.relative_return_21d,
        rs.relative_return_63d

    from {{ ref('int_returns_multi_horizon') }} r
    left join {{ ref('int_moving_averages') }} ma
        on ma.date = r.date and ma.ticker = r.ticker
    left join {{ ref('int_rsi_zscore') }} rz
        on rz.date = r.date and rz.ticker = r.ticker
    left join {{ ref('int_realized_volatility') }} rv
        on rv.date = r.date and rv.ticker = r.ticker
    left join {{ ref('int_volume_stats') }} vs
        on vs.date = r.date and vs.ticker = r.ticker
    left join {{ ref('int_relative_strength') }} rs
        on rs.date = r.date and rs.ticker = r.ticker

    {% if is_incremental() %}
    where r.date >= (
        select coalesce(max(date), date '1900-01-01') from {{ this }}
    ) - interval '{{ var("lookback_days") }} days'
    {% endif %}

)

select date, ticker, signal_name, value
from wide
unpivot (
    value for signal_name in (
        return_5d,
        return_21d,
        return_63d,
        return_252d,
        momentum_rank_5d,
        momentum_rank_21d,
        momentum_rank_63d,
        momentum_rank_252d,
        above_slow_ma,
        golden_cross,
        death_cross,
        rsi,
        zscore,
        realized_vol,
        vol_percentile,
        volume_ratio,
        volume_spike,
        relative_return_21d,
        relative_return_63d
    )
)
where value is not null
