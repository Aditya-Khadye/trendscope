-- A negative price or volume anywhere in the clean price fact means the
-- pipeline let garbage through. Returns offending rows (test fails if any).

select date, ticker, open, high, low, close, adj_close, volume
from {{ ref('fct_daily_prices') }}
where open < 0
   or high < 0
   or low < 0
   or close < 0
   or adj_close < 0
   or volume < 0
