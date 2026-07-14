-- Universe-level daily breadth. Aggregates skip NULL inputs, so tickers
-- without enough history for a given signal simply don't count toward that
-- day's percentage. Fully rebuilt each run — it's tiny.

select
    r.date,
    count(*) as tickers_observed,
    avg(cast(ma.above_slow_ma as double)) as pct_above_slow_ma,
    avg(case when r.return_21d is not null then cast(r.return_21d > 0 as double) end)
        as pct_positive_21d,
    avg(cast(vs.volume_spike as double)) as pct_volume_spike

from {{ ref('int_returns_multi_horizon') }} r
left join {{ ref('int_moving_averages') }} ma
    on ma.date = r.date and ma.ticker = r.ticker
left join {{ ref('int_volume_stats') }} vs
    on vs.date = r.date and vs.ticker = r.ticker

group by r.date
