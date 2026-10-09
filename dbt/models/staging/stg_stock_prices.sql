{% if var('use_clean_stock_prices_v2', false) %}

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
    cast(null as varchar) as event_hour
from {{ source('stock_pipeline', 'clean_stock_prices') }}

{% else %}

select
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    upper(trim(symbol)) as symbol,
    price,
    source,
    event_time,
    processed_at,
    event_date,
    event_hour
from {{ source('stock_pipeline', 'clean_stock_prices') }}

{% endif %}
