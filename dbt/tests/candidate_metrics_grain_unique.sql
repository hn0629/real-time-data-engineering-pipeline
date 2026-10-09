{{ config(
    enabled=var('use_clean_stock_prices_v2', false),
    tags=['clean_v2_candidate']
) }}

select
    event_date,
    symbol,
    source,
    count(*) as duplicate_group_count
from {{ ref('int_daily_symbol_metrics') }}
group by
    event_date,
    symbol,
    source
having count(*) > 1
