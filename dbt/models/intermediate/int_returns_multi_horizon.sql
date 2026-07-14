-- Multi-horizon simple returns on adj_close plus cross-sectional percentile
-- ranks per date. Point-in-time safe: every value at date t uses only rows
-- with date <= t (lag windows), and each horizon requires the full lookback
-- (lag returns NULL until enough history exists).
--
-- Ranks are computed per horizon over the tickers that HAVE a return that
-- day (nulls excluded before ranking, so the denominator is honest).

{% set horizons = var('momentum_horizons') %}

with base as (

    select date, ticker, adj_close
    from {{ ref('stg_prices') }}

),

returns as (

    select
        date,
        ticker,
        adj_close / nullif(lag(adj_close, 1) over w, 0) - 1 as return_1d,
        {% for h in horizons %}
        adj_close / nullif(lag(adj_close, {{ h }}) over w, 0) - 1 as return_{{ h }}d
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from base
    window w as (partition by ticker order by date)

)

{% for h in horizons %}
, ranks_{{ h }} as (

    select
        date,
        ticker,
        percent_rank() over (partition by date order by return_{{ h }}d) as momentum_rank_{{ h }}d
    from returns
    where return_{{ h }}d is not null

)
{% endfor %}

select
    r.date,
    r.ticker,
    r.return_1d,
    {% for h in horizons %}
    r.return_{{ h }}d,
    {% endfor %}
    {% for h in horizons %}
    ranks_{{ h }}.momentum_rank_{{ h }}d
    {%- if not loop.last %},{% endif %}
    {% endfor %}

from returns r
{% for h in horizons %}
left join ranks_{{ h }}
    on ranks_{{ h }}.date = r.date and ranks_{{ h }}.ticker = r.ticker
{% endfor %}
