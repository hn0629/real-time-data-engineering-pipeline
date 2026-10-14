# Warehouse Portability Fixture Replay — 2026-10-14

## Purpose

This document records an independent replay of the saved synthetic Clean v2
fixture in an isolated Snowflake environment. The replay rebuilds and tests
the shared dbt transformation chain from scratch:

```text
Clean v2 source
    → staging
    → daily symbol metrics
    → latest-available symbol metrics
```

The purpose is to verify that the warehouse-portability result is
reproducible from the saved fixture scripts and documented dbt commands,
rather than relying only on the original validation run.

## Scope

This replay validates synthetic-fixture transformation portability only.

It does **not** validate:

- Live Amazon S3 integration on Snowflake or Databricks
- Production data quality
- Scheduled execution
- Airflow DAG behavior
- Real market-data ingestion

Both Airflow v2 DAGs remained paused throughout the replay.

## Isolated replay targets

| Platform | Replay source schema | Fixture table | dbt output schema |
|---|---|---|---|
| Snowflake | `STOCK_PIPELINE_DEV.CLEAN_REPLAY_20261014` | `CLEAN_STOCK_PRICES_V2` | `STOCK_PIPELINE_DEV.DBT_REPLAY_20261014` |
| Databricks | `workspace.stock_pipeline_clean_replay_20261014` | `clean_stock_prices_v2` | Pending replay execution |

The replay source schemas are separate from the schemas used in the original
warehouse-portability validation. The original validation fixtures and
candidate views were not modified.

## Fixture expectations

Each replay fixture contains the same six synthetic stock-price rows used in
the original validation. The fixture must contain exactly six rows with six
distinct Kafka offsets:

```text
row_count = 6
distinct_offsets = 6
```

The fixture includes intentionally inconsistent symbol formatting and
mixed-timezone event timestamps so that the staging normalization and
timestamp-parsing logic are exercised.

## dbt configuration

The replay explicitly enables the current-format Clean v2 source:

```text
use_clean_stock_prices_v2: true
```

The dbt build uses `--full-refresh` so that all candidate views are rebuilt
from the isolated replay fixture.

The legacy source branch remains the project default. No default source
mapping, Airflow DAG, or production configuration was changed for this replay.

## Snowflake replay

### Environment

| Item | Value |
|---|---|
| Replay date | 2026-10-14 |
| dbt Core | 1.12.5 |
| Adapter | dbt-snowflake 1.12.1 |
| Python | 3.11.3 |
| Warehouse | `STOCK_DBT_DEV_WH` |
| Role | `SYSADMIN` |
| Fixture schema | `STOCK_PIPELINE_DEV.CLEAN_REPLAY_20261014` |
| dbt output schema | `STOCK_PIPELINE_DEV.DBT_REPLAY_20261014` |

### Fixture verification

The replay fixture was loaded into the isolated schema and verified before
running dbt:

```text
row_count: 6
distinct_offsets: 6
```

### dbt command

```powershell
dbt build `
  --project-dir dbt `
  --profiles-dir dbt\profiles_replay_20261014 `
  --vars '{"use_clean_stock_prices_v2": true}' `
  --full-refresh
```

The dbt profile used environment-based authentication. The password was
provided only through the process environment and was not committed to Git.

### Result

```text
Completed successfully
Done. PASS=21 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=21
```

| Validation | Result |
|---|---|
| Candidate views built | 3 |
| Data tests executed | 18 |
| Data tests passed | 18 |
| Build result | PASS=21, WARN=0, ERROR=0, SKIP=0 |
| Matches original validation | Yes |

### Built candidate views

```text
STG_STOCK_PRICES_V2_CANDIDATE
INT_DAILY_SYMBOL_METRICS_V2_CANDIDATE
MART_LATEST_SYMBOL_METRICS_V2_CANDIDATE
```

### Evidence

- [Snowflake dbt replay build](../../images/warehouse-portability-replay/20261014/snowflake-dbt-build.png)

## Databricks replay

Pending replay execution.

The Databricks replay will use the isolated fixture schema:

```text
workspace.stock_pipeline_clean_replay_20261014
```

It must produce the same expected result:

```text
PASS=21
WARN=0
ERROR=0
SKIP=0
```

## Replay conclusion

The Snowflake replay independently reproduced the original Snowflake
warehouse-portability validation result on 2026-10-14.

The saved fixture script, isolated replay schema, dbt Core version, adapter
version, dbt command, and resulting PASS/ERROR/SKIP counts are recorded
above. This strengthens the portability evidence by demonstrating that the
Snowflake transformation chain can be rebuilt from the saved synthetic fixture
and documented commands.

Databricks replay verification remains pending.