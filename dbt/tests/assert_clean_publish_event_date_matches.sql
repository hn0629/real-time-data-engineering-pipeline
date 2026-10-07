{{ config(
    enabled=var('enable_clean_publish_validation', false),
    tags=['clean_publish_validation']
) }}

select *
from {{ ref('stg_clean_publish_validation') }}
where event_time is null
   or event_date is null
   or cast(event_time as date) <> event_date