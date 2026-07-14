-- Point-in-time guard: nothing in raw or the marts may carry a date after
-- today. A future-dated bar would silently poison every trailing window.

select 'raw.prices' as relation, date, ticker
from {{ source('raw', 'prices') }}
where date > current_date

union all

select 'fct_daily_prices' as relation, date, ticker
from {{ ref('fct_daily_prices') }}
where date > current_date
