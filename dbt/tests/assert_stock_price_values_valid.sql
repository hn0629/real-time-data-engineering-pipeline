select *
from {{ ref('stg_stock_prices') }}
where price <= 0
   or trim(symbol) = ''
