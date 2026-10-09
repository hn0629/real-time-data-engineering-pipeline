{{ config(
    enabled=var('enable_clean_publish_validation', false),
    tags=['clean_publish_validation']
) }}

with source_counts as (
    select
        event_date,
        count(*) as source_event_count
    from {{ ref('stg_clean_publish_validation') }}
    group by event_date
),

analytics_counts as (
    select
        event_date,
        sum(event_count) as analytics_event_count
    from {{ ref('daily_symbol_metrics_clean_validation') }}
    group by event_date
)

select
    coalesce(s.event_date, a.event_date) as event_date,
    s.source_event_count,
    a.analytics_event_count
from source_counts s
full outer join analytics_counts a
    on s.event_date = a.event_date
where
    s.event_date is null
    or a.event_date is null
    or s.source_event_count <> a.analytics_event_count
    or a.analytics_event_count is null