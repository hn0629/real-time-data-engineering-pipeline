select
    cast(event_time as date) as event_date,
    symbol,
    source,
    count(*) as event_count,
    avg(price) as average_price,
    min(price) as minimum_price,
    max(price) as maximum_price,
    max(event_time) as latest_event_time
from {{ ref('stg_stock_prices') }}
group by
    cast(event_time as date),
    symbol,
    source