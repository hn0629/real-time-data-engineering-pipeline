{{ config(
    materialized='view',
    enabled=var('enable_clean_publish_validation', false),
    tags=['clean_publish_validation']
) }}

select
    event_date,
    symbol,
    source,
    count(*) as event_count,
    avg(price) as average_price,
    min(price) as minimum_price,
    max(price) as maximum_price,
    max(event_time) as latest_event_time
from {{ ref('stg_clean_publish_validation') }}
group by
    event_date,
    symbol,
    source