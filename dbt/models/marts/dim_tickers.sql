-- Current instrument dimension: the live (dbt_valid_to IS NULL) rows of the
-- SCD2 snapshot, joined to the benchmark seed. The seed is the source of
-- truth for benchmarks; SPY is the fallback for unmapped tickers.

select
    s.ticker,
    s.name,
    s.sector,
    s.industry,
    s.asset_type,
    s.groups,
    coalesce(m.benchmark, 'SPY') as benchmark

from {{ ref('tickers_snapshot') }} s
left join {{ ref('sector_etf_map') }} m
    on m.ticker = s.ticker
where s.dbt_valid_to is null
