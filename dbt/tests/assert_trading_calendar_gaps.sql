-- Gap detection: consecutive bars for a ticker should never be more than
-- var('max_calendar_gap_days') calendar days apart (US markets never close
-- longer than a long weekend). A larger gap means the loader silently
-- missed days.

with ordered as (

    select
        ticker,
        date,
        lag(date) over (partition by ticker order by date) as prev_date
    from {{ ref('fct_daily_prices') }}

)

select
    ticker,
    prev_date,
    date,
    date_diff('day', prev_date, date) as gap_days
from ordered
where prev_date is not null
  and date_diff('day', prev_date, date) > {{ var('max_calendar_gap_days') }}
