{{ config(
    materialized='view',
    enabled=var('enable_clean_publish_validation', false),
    tags=['clean_publish_validation']
) }}

with ranked_daily_metrics as (

    select
        event_date,
        symbol,
        source,
        event_count,
        average_price,
        minimum_price,
        maximum_price,
        latest_event_time,
        row_number() over (
            partition by symbol, source
            order by event_date desc
        ) as recency_rank
    from {{ ref('daily_symbol_metrics_clean_validation') }}

)

select
    event_date,
    symbol,
    source,
    event_count,
    average_price,
    minimum_price,
    maximum_price,
    latest_event_time
from ranked_daily_metrics
where recency_rank = 1