-- Wide return fact: multi-horizon returns, cross-sectional momentum ranks,
-- and relative strength vs the benchmark ETF. Window math happens in the
-- intermediate views over FULL history; the incremental filter here only
-- selects which finished rows to merge, so lookbacks stay correct.

{{
    config(
        materialized='incremental',
        unique_key=['date', 'ticker'],
    )
}}

select
    r.date,
    r.ticker,
    r.return_1d,
    r.return_5d,
    r.return_21d,
    r.return_63d,
    r.return_252d,
    r.momentum_rank_5d,
    r.momentum_rank_21d,
    r.momentum_rank_63d,
    r.momentum_rank_252d,
    rs.benchmark,
    rs.relative_return_21d,
    rs.relative_return_63d

from {{ ref('int_returns_multi_horizon') }} r
left join {{ ref('int_relative_strength') }} rs
    on rs.date = r.date and rs.ticker = r.ticker

{% if is_incremental() %}
where r.date >= (
    select coalesce(max(date), date '1900-01-01') from {{ this }}
) - interval '{{ var("lookback_days") }} days'
{% endif %}
