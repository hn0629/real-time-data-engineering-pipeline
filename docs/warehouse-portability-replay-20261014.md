# Warehouse Portability Fixture Replay — 2026-10-14

## Purpose

Independently replay the saved synthetic Clean v2 fixture in isolated
Snowflake and Databricks environments, then rebuild and test the shared
dbt transformation chain from scratch.

## Scope

This replay validates synthetic-fixture transformation portability only.
It does not validate live S3 integration, production data quality,
scheduled execution, or Airflow behavior.

## Isolated replay targets

| Platform | Replay source schema | Fixture table |
|---|---|---|
| Snowflake | `STOCK_PIPELINE_DEV.CLEAN_REPLAY_20261014` | `CLEAN_STOCK_PRICES_V2` |
| Databricks | `workspace.stock_pipeline_clean_replay_20261014` | `clean_stock_prices_v2` |

## Fixture expectations

Each replay fixture must contain exactly six synthetic rows with six
distinct Kafka offsets:

```text
row_count = 6
distinct_offsets = 6
```

## Required dbt variables

```text
use_clean_stock_prices_v2: true
```

## Execution record

| Platform | Replay date | dbt command | Build result | Data tests | Evidence |
|---|---|---|---|---|---|
| Snowflake |  |  |  |  |  |
| Databricks |  |  |  |  |  |

## Result

Pending replay execution.