select
    symbol,
    source,
    count(*) as row_count
from {{ ref('mart_latest_symbol_metrics') }}
group by symbol, source
having count(*) > 1