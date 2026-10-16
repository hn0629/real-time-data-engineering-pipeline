\# Athena Clean v2 Live S3 Replay Validation



\## Milestone



Validated the shared dbt Clean v2 transformation layer against the live

S3-backed Athena Clean source.



\- \*\*Date:\*\* 2026-10-14

\- \*\*Source database:\*\* `realtime\_pipeline`

\- \*\*Source table:\*\* `clean\_stock\_prices\_v2`

\- \*\*Source format:\*\* S3-backed partitioned Parquet

\- \*\*dbt output schema:\*\* `dbt\_replay\_20261014`

\- \*\*Models built:\*\* 3

\- \*\*Data tests executed:\*\* 18

\- \*\*Final dbt result:\*\* `PASS=21, WARN=0, ERROR=0, SKIP=0`



\## Validation scope



This replay read the existing live S3-backed Clean v2 table:



```sql

realtime\_pipeline.clean\_stock\_prices\_v2

```



The dbt models were built as isolated candidate views in:



```sql

dbt\_replay\_20261014

```



The source table and the original Athena models were not modified.



\## Source verification



The live Clean v2 source contained 8,955 events with 8,955 distinct Kafka

offsets, confirming that the source was duplicate-free before dbt

transformation.



```sql

SELECT

&#x20;   COUNT(\*) AS row\_count,

&#x20;   COUNT(DISTINCT kafka\_offset) AS distinct\_offsets

FROM realtime\_pipeline.clean\_stock\_prices\_v2;

```



| Metric | Result |

|---|---:|

| Source rows | 8,955 |

| Distinct Kafka offsets | 8,955 |

| Duplicate Kafka offsets | 0 |



\## dbt replay command



```powershell

dbt build `

&#x20; --project-dir dbt `

&#x20; --profiles-dir dbt\\profiles\_athena\_clean\_v2\_replay\_20261014 `

&#x20; --vars '{"use\_clean\_stock\_prices\_v2": true}' `

&#x20; --full-refresh

```



The replay enabled:



```text

use\_clean\_stock\_prices\_v2=true

```



and produced these isolated candidate views:



\- `dbt\_replay\_20261014.stg\_stock\_prices\_v2\_candidate`

\- `dbt\_replay\_20261014.int\_daily\_symbol\_metrics\_v2\_candidate`

\- `dbt\_replay\_20261014.mart\_latest\_symbol\_metrics\_v2\_candidate`



\## IAM scope



The replay used isolated AWS Glue permissions scoped to the replay output

database and its tables:



```text

dbt\_replay\_20261014

```



The production `realtime\_pipeline` database remained read-only for this

validation.



\## Evidence



\### dbt build result



!\[Athena Clean v2 dbt build](../images/athena-clean-v2-replay-20261014-dbt-build.png)



\### Live source verification



!\[Athena Clean v2 source check](../images/athena-clean-v2-replay-20261014-source-check.png)



\### Final mart result



!\[Athena Clean v2 final mart](../images/athena-clean-v2-replay-20261014-final-mart.png)



\## Warehouse replay status



| Warehouse | Clean v2 validation type | Result |

|---|---|---|

| Athena | Live S3-backed Parquet source | PASS=21 |

| Snowflake | Synthetic six-row fixture replay | PASS=21 |

| Databricks | Synthetic six-row fixture replay | PASS=21 |



\## Conclusion



The Athena replay extends the dbt warehouse-portability validation from

synthetic fixtures to the live S3-backed Clean v2 source. It confirms that the

shared dbt transformation chain can read the production-shaped Athena dataset,

build three candidate views, and pass all 21 dbt checks in an isolated replay

schema.

