{{ config(
    enabled=var('enable_clean_publish_validation', false),
    tags=['clean_publish_validation']
) }}

select
    event_date,
    symbol,
    source,
    count(*) as duplicate_group_count
from {{ ref('daily_symbol_metrics_clean_validation') }}
group by
    event_date,
    symbol,
    source
having count(*) > 1