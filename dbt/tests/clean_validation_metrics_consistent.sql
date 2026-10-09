{{ config(
    enabled=var('enable_clean_publish_validation', false),
    tags=['clean_publish_validation']
) }}

select *
from {{ ref('daily_symbol_metrics_clean_validation') }}
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
