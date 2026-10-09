{{ config(
    enabled=var('use_clean_stock_prices_v2', false),
    tags=['clean_v2_candidate']
) }}

with source_counts as (
    select
        event_date,
        count(*) as source_event_count
    from {{ ref('stg_stock_prices') }}
    group by event_date
),

analytics_counts as (
    select
        event_date,
        sum(event_count) as analytics_event_count
    from {{ ref('int_daily_symbol_metrics') }}
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
