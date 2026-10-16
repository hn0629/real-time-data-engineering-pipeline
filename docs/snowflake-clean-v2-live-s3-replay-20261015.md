# Snowflake Clean v2 Live S3 Replay Validation

## Milestone

Validated the shared dbt Clean v2 transformation layer using a Snowflake table
loaded from an S3-backed replay snapshot.

- **Date:** 2026-10-15
- **Snowflake database:** `STOCK_PIPELINE_DEV`
- **Replay schema:** `CLEAN_LIVE_S3_REPLAY_20261015`
- **Replay table:** `CLEAN_STOCK_PRICES_V2`
- **S3 replay prefix:** `s3://hoang-real-time-data-pipeline-2026/replay/snowflake-clean-v2-20261015/`
- **dbt output schema:** `DBT_LIVE_S3_REPLAY_20261015`
- **Models built:** 3
- **Data tests executed:** 18
- **Final dbt result:** `PASS=21, WARN=0, ERROR=0, SKIP=0`

## S3-to-Snowflake path

```text
Athena Clean v2 source
  → S3 replay prefix
  → Snowflake external stage
  → COPY INTO Snowflake replay table
  → dbt Clean v2 candidate chain
  → PASS=21
```

## Source verification

The S3-loaded Snowflake replay table contained 8,955 events with 8,955
distinct Kafka offsets.

| Metric | Result |
|---|---:|
| Source rows | 8,955 |
| Distinct Kafka offsets | 8,955 |
| Duplicate Kafka offsets | 0 |

## dbt replay command

```powershell
dbt build `
  --project-dir dbt `
  --profiles-dir dbt\profiles_snowflake_live_s3_replay_20261015 `
  --vars '{"use_clean_stock_prices_v2": true}' `
  --full-refresh
```

## Evidence

### S3 load and source verification

![Snowflake S3 load verification](../images/snowflake-clean-v2-live-s3-replay-20261015-copy-load.png)

### dbt build result

![Snowflake live S3 dbt build](../images/snowflake-clean-v2-live-s3-replay-20261015-dbt-build.png)

### Final mart result

![Snowflake live S3 final mart](../images/snowflake-clean-v2-live-s3-replay-20261015-final-mart.png)

## Warehouse replay status

| Warehouse | Clean v2 validation type | Result |
|---|---|---|
| Athena | Live S3-backed Parquet source | PASS=21 |
| Snowflake | S3-loaded replay table | PASS=21 |
| Databricks | Unity Catalog external Parquet table over the published S3 snapshot | Model run PASS=3; data tests PASS=18 |
The Databricks direct S3 snapshot replay was completed on October 16,
2026, after the Snowflake validation recorded here. It is separate
from the earlier six-row synthetic-fixture replay.

For the Athena and Snowflake combined builds, PASS=21 represents
three model builds plus 18 data tests, not 21 tests. Databricks ran
the model build and tests separately.

These successful validations do not establish automated row-level
or aggregate parity across warehouses.
