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
