-- Relative strength: ticker return minus its benchmark sector-ETF return
-- over 21d and 63d horizons. Benchmark mapping comes from the
-- sector_etf_map seed with an SPY fallback; a benchmark ETF measured
-- against itself reads 0 by construction.

with ticker_returns as (

    select date, ticker, return_21d, return_63d
    from {{ ref('int_returns_multi_horizon') }}

),

benchmarked as (

    select
        r.date,
        r.ticker,
        coalesce(m.benchmark, 'SPY') as benchmark,
        r.return_21d,
        r.return_63d
    from ticker_returns r
    left join {{ ref('sector_etf_map') }} m
        on m.ticker = r.ticker

)

select
    b.date,
    b.ticker,
    b.benchmark,
    b.return_21d - bench.return_21d as relative_return_21d,
    b.return_63d - bench.return_63d as relative_return_63d

from benchmarked b
left join ticker_returns bench
    on bench.date = b.date
   and bench.ticker = b.benchmark
