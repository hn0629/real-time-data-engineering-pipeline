{% if var('use_clean_stock_prices_v2', false) %}

select
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    upper(trim(symbol)) as symbol,
    price,
    source,
    {{ stock_event_timestamp('event_time') }} as event_time,
    ingested_at as processed_at,
    cast(event_date as date) as event_date,
    cast(null as {{ dbt.type_string() }}) as event_hour
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
