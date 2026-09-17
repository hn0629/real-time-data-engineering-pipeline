\# Real-Time Stock Price Pipeline Architecture



```mermaid

flowchart LR

&#x20;   P\[Stock-price producer<br/>Python] -->|JSON price events| K\[(Kafka<br/>stock\_prices topic)]



&#x20;   K -->|Structured Streaming| S\[Spark streaming job<br/>kafka\_stream.py]



&#x20;   S -->|Append; event\_date partition| R\[(Raw<br/>Parquet)]

&#x20;   S -->|Rejected records + reason| Q\[(Quarantine<br/>Parquet)]

&#x20;   S -->|Validated + deduplicated;<br/>event\_date partition| C\[(Clean<br/>Parquet)]

&#x20;   S -->|Counts, quality signals,<br/>job status| M\[(Metrics<br/>Parquet / artifacts)]



&#x20;   C -->|Derived aggregates| BA\[Spark analytics job<br/>build\_analytics\_layer.py]

&#x20;   BA -->|event\_date partition| AN\[(Analytics<br/>Parquet)]



&#x20;   subgraph Local\["Local / Docker Data Plane"]

&#x20;       P

&#x20;       K

&#x20;       S

&#x20;       R

&#x20;       Q

&#x20;       C

&#x20;       M

&#x20;       BA

&#x20;       AN

&#x20;   end



&#x20;   subgraph AWS\["AWS Analytics Branch"]

&#x20;       S3\[(Amazon S3<br/>raw / clean / analytics prefixes)]

&#x20;       ATH\[(Amazon Athena<br/>analytics tables and views)]

&#x20;       S3 -->|External tables / views| ATH

&#x20;   end



&#x20;   R -->|Optional sync| S3

&#x20;   C -->|Optional sync| S3

&#x20;   AN -->|Optional sync| S3



&#x20;   subgraph GCP\["Google Cloud Platform Export Branch"]

&#x20;       GCS\[(Cloud Storage<br/>optional landing zone)]

&#x20;       BQ\[(BigQuery<br/>events / metrics / analytics)]

&#x20;       GCS -->|Load or external-table path| BQ

&#x20;   end



&#x20;   R -->|Optional export| GCS

&#x20;   C -->|Optional export| GCS

&#x20;   AN -->|Optional export| GCS

&#x20;   M -->|Optional metrics export| BQ



&#x20;   AF\[Airflow DAGs] -.->|Read-only: latest Clean partition| C

&#x20;   AF -.->|Read-only: quality/health artifacts| M

&#x20;   AF -.->|Read-only orchestration queries| ATH

&#x20;   AF -.->|Optional read-only validation/query| BQ



&#x20;   ATH -->|Read-only analytics| DASH\[Streamlit dashboard]

&#x20;   BQ -->|Optional read-only analytics| DASH



&#x20;   ATH -->|Allowlisted read-only SQL| LLM\[Optional LLM / NLQ layer]

&#x20;   BQ -->|Allowlisted read-only SQL| LLM



&#x20;   S -->|Checkpoints, logs, batch status| OBS\[Observability]

&#x20;   AF -->|DAG status, retries, task logs| OBS

```



\## Implementation legend



| Component | Status | Notes |

|---|---|---|

| Kafka → Spark → local Parquet Raw/Clean/Quarantine/Metrics | Implemented locally | Spark owns writes; event-oriented outputs use `event\_date` partitions |

| Analytics Parquet layer | Implemented locally | Derived from Clean data |

| S3 synchronization and Athena query path | Implemented/validated as configured | Athena serves warehouse-style, read-only analytics |

| GCP / BigQuery export branch | Optional or under review | Preserve as a separate, documented path until exporter changes are reviewed and tested |

| Airflow | Implemented | Reads the latest Clean partition and operational outputs; it does not mutate lake data |

| LLM/NLQ | Planned only | Limited to approved read-only analytics query templates |



The diagram uses solid arrows for data writes/exports and dashed arrows for Airflow’s \*\*read-only operational access\*\*.



\## Data ownership and boundaries



\- \*\*Spark is the only lake writer.\*\* It writes Raw, Quarantine, Clean, Metrics, and Analytics outputs.

\- \*\*Raw is immutable.\*\* It preserves the ingested event representation for audit and replay.

\- \*\*Quarantine preserves rejected events.\*\* Every rejected record includes a reason; failures are not silently discarded.

\- \*\*Clean is trusted event-level data.\*\* It is validated, normalized, deduplicated, and partitioned by `event\_date`.

\- \*\*Analytics is derived data.\*\* It supports dashboard, Athena, BigQuery, and future NLQ consumers.

\- \*\*Airflow is read-only with respect to lake data.\*\* It validates the latest Clean partition and reads status/query results; it does not overwrite Parquet data.

\- \*\*Athena and BigQuery are consumer/query surfaces.\*\* The project can support either or both, but each must query derived/approved datasets through read-only identities.

