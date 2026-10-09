{{ config(
    enabled=var('use_clean_stock_prices_v2', false),
    tags=['clean_v2_candidate']
) }}

with expected as (
    select
        symbol,
        source,
        max(event_date) as expected_event_date
    from {{ ref('int_daily_symbol_metrics') }}
    group by
        symbol,
        source
),

actual as (
    select
        symbol,
        source,
        count(*) as mart_row_count,
        min(event_date) as minimum_mart_event_date,
        max(event_date) as maximum_mart_event_date
    from {{ ref('mart_latest_symbol_metrics') }}
    group by
        symbol,
        source
)

select
    coalesce(e.symbol, a.symbol) as symbol,
    coalesce(e.source, a.source) as source,
    e.expected_event_date,
    a.mart_row_count,
    a.minimum_mart_event_date,
    a.maximum_mart_event_date
from expected e
full outer join actual a
    on e.symbol = a.symbol
    and e.source = a.source
where
    e.expected_event_date is null
    or a.mart_row_count is null
    or a.mart_row_count <> 1
    or a.minimum_mart_event_date is null
    or a.maximum_mart_event_date is null
    or a.minimum_mart_event_date <> e.expected_event_date
    or a.maximum_mart_event_date <> e.expected_event_date

