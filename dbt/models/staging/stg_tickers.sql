-- Current instrument metadata: newest version per ticker.

with versions as (

    select
        *,
        row_number() over (
            partition by ticker
            order by _loaded_at desc
        ) as _version_rank

    from {{ source('raw', 'tickers') }}

)

select
    ticker,
    name,
    sector,
    industry,
    asset_type,
    groups,
    _source,
    _loaded_at

from versions
where _version_rank = 1
