{{ config(
    enabled=var('enable_clean_publish_validation', false),
    tags=['clean_publish_validation']
) }}

select
    kafka_topic,
    kafka_partition,
    kafka_offset,
    count(*) as occurrence_count
from {{ ref('stg_clean_publish_validation') }}
group by
    kafka_topic,
    kafka_partition,
    kafka_offset
having count(*) > 1