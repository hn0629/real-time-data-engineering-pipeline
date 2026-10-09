{{ config(
    materialized='view',
    enabled=var('enable_clean_publish_validation', false),
    tags=['clean_publish_validation']
) }}

select
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    upper(trim(symbol)) as symbol,
    price,
    source,
    cast(
        at_timezone(
            try(from_iso8601_timestamp(event_time)),
            'UTC'
        ) as timestamp
    ) as event_time,
    ingested_at as processed_at,
    cast(event_date as date) as event_date,
    batch_id
from {{ source('clean_publish_validation', 'stock_prices') }}
