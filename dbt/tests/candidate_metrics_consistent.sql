{{ config(
    enabled=var('use_clean_stock_prices_v2', false),
    tags=['clean_v2_candidate']
) }}

select *
from {{ ref('int_daily_symbol_metrics') }}
where
    event_date is null
    or symbol is null
    or trim(symbol) = ''
    or source is null
    or trim(source) = ''
    or event_count is null
    or event_count <= 0
    or minimum_price is null
    or average_price is null
    or maximum_price is null
    or minimum_price <= 0
    or minimum_price > average_price
    or average_price > maximum_price
    or latest_event_time is null
    or cast(latest_event_time as date) <> event_date

