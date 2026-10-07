{{ config(
    enabled=var('enable_clean_publish_validation', false),
    tags=['clean_publish_validation']
) }}

select *
from {{ ref('stg_clean_publish_validation') }}
where price is null
   or not is_finite(price)
   or price <= 0
   or symbol is null
   or trim(symbol) = ''
   or source is null
   or trim(source) = ''
   or kafka_topic is null
   or trim(kafka_topic) = ''