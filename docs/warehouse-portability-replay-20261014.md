# Warehouse Portability Fixture Replay — 2026-10-14

## Purpose

This document records an independent replay of the saved synthetic Clean v2
fixture in isolated Snowflake and Databricks environments. The replay rebuilds
and tests the shared dbt transformation chain from scratch:

```text
Clean v2 source
    → staging
    → daily symbol metrics
    → latest-available symbol metrics
```

The purpose is to verify that the warehouse-portability result is reproducible
from saved fixture data and documented dbt commands, rather than relying only
on the original validation run.

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
| Databricks | `workspace.stock_pipeline_clean_replay_20261014` | `clean_stock_prices_v2` | `workspace.DBT_REPLAY_20261014` |

The replay source schemas are separate from the schemas used in the original
warehouse-portability validation. The original validation fixtures and
candidate views were not modified.

## Fixture expectations

Each replay fixture contains the same six synthetic stock-price rows. The
fixture must contain exactly six rows with six distinct Kafka offsets:

```text
row_count = 6
distinct_offsets = 6
```

The fixture includes intentionally inconsistent symbol casing and mixed-timezone
event timestamps so that staging normalization and timestamp parsing are
exercised.

The fixture columns are:

```text
KAFKA_TOPIC
KAFKA_PARTITION
KAFKA_OFFSET
KAFKA_TIMESTAMP
SYMBOL
PRICE
SOURCE
EVENT_TIME
INGESTED_AT
EVENT_DATE
```

The six Kafka offsets are:

```text
100
101
102
103
104
105
```

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
| dbt profile directory | `dbt/profiles_snowflake_replay_20261014` |

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
  --profiles-dir dbt\profiles_snowflake_replay_20261014 `
  --vars '{"use_clean_stock_prices_v2": true}' `
  --full-refresh
```

The dbt profile used environment-based authentication. Credentials and account
details remained outside version control.

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

### Environment

| Item | Value |
|---|---|
| Replay date | 2026-10-14 |
| dbt Core | 1.12.0 |
| Adapter | dbt-databricks 1.12.4 |
| Catalog | `workspace` |
| Fixture schema | `workspace.stock_pipeline_clean_replay_20261014` |
| dbt output schema | `workspace.DBT_REPLAY_20261014` |
| dbt profile directory | `dbt/profiles_databricks_replay_20261014` |

The Databricks replay uses dbt Core 1.12.0 with dbt-databricks 1.12.4, the
compatible version pairing documented for the August 2026 dbt compatible
release. This differs from the Snowflake replay versions but uses the same dbt
project, fixture, variables, and full-refresh command.

### Fixture verification

The Databricks fixture was recreated with the same schema and six rows as the
Snowflake replay fixture. It was verified before running dbt:

```text
row_count: 6
distinct_offsets: 6
```

### dbt command

```powershell
& ".venv-databricks-replay\Scripts\dbt.exe" build `
  --project-dir dbt `
  --profiles-dir dbt\profiles_databricks_replay_20261014 `
  --vars '{"use_clean_stock_prices_v2": true}' `
  --full-refresh
```

The Databricks workspace host, SQL warehouse HTTP path, and authentication
details remained in the local, untracked dbt profile.

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
| Matches Snowflake replay | Yes |
| Matches original validation | Yes |

### Built candidate views

```text
stg_stock_prices_v2_candidate
int_daily_symbol_metrics_v2_candidate
mart_latest_symbol_metrics_v2_candidate
```

### Final mart verification

```sql
SELECT *
FROM workspace.DBT_REPLAY_20261014.mart_latest_symbol_metrics_v2_candidate
ORDER BY symbol;
```

Expected symbols:

```text
AAPL
GOOG
MSFT
```

### Evidence

- [Databricks dbt replay build](../../images/warehouse-portability-replay/20261014/databricks-dbt-build.png)

## Replay conclusion

Both Snowflake and Databricks independently reproduced the original warehouse
portability validation result on 2026-10-14.

Each platform used an isolated replay fixture schema, rebuilt the three dbt
candidate views with `--full-refresh`, and passed all 18 data tests:

```text
PASS=21
WARN=0
ERROR=0
SKIP=0
```

The saved fixture data, isolated replay schemas, dbt versions, adapter
versions, dbt commands, and resulting PASS/ERROR/SKIP counts are recorded
above. This strengthens the portability evidence by demonstrating that the
shared transformation chain can be rebuilt from the same synthetic fixture in
both warehouses.

No default source mapping, Airflow DAG, or production configuration was
changed for this replay.