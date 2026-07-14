-- Latest-wins view over the versioned raw price rows, with semantic cleaning
-- that the raw layer deliberately does not do:
--   * resolve to the newest _loaded_at per (date, ticker)
--   * drop rows without a usable close/adj_close (upstream NaN edges)
--   * map yfinance's `0.0 = no split` to a valid 1.0 multiplier
--   * default missing dividends to 0.0

with versions as (

    select
        *,
        row_number() over (
            partition by date, ticker
            order by _loaded_at desc
        ) as _version_rank

    from {{ source('raw', 'prices') }}

)

select
    date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    coalesce(dividend, 0.0) as dividend,
    case
        when split_ratio is null or split_ratio = 0.0 then 1.0
        else split_ratio
    end as split_ratio,
    _source,
    _loaded_at

from versions
where _version_rank = 1
  and close is not null
  and adj_close is not null
