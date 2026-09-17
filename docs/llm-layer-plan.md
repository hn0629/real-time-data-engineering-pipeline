\# Optional AI / LLM Analytics Layer Plan



\## Goal



Provide a constrained natural-language analytics interface for the stock-price \*\*Analytics\*\* layer. The LLM is a read-only consumer: it cannot publish to Kafka, control Spark, alter Parquet data, trigger Airflow, modify S3/GCS objects, or run administrative warehouse commands.



The integration supports one selected warehouse backend per deployment:



\- \*\*AWS deployment:\*\* Amazon Athena queries approved analytics views over S3.

\- \*\*GCP deployment:\*\* BigQuery queries approved analytics tables or views.



The application must not route a single question to both clouds by default. The selected backend is configured by the operator and made visible to the user.



\## Architecture



```mermaid

flowchart LR

&#x20;   U\[User question] --> I\[Intent classifier<br/>and parameter extractor]

&#x20;   I --> V\[Allowlist and<br/>parameter validation]

&#x20;   V --> R{Configured<br/>read-only backend}



&#x20;   R -->|AWS| A\[Athena read-only workgroup]

&#x20;   R -->|GCP| B\[BigQuery read-only service identity]



&#x20;   A --> AV\[(Approved Athena<br/>analytics view)]

&#x20;   B --> BV\[(Approved BigQuery<br/>analytics view)]



&#x20;   AV --> F\[Result formatter]

&#x20;   BV --> F

&#x20;   F --> U



&#x20;   L\[Audit log] <-.-> I

&#x20;   L <-.-> V

&#x20;   L <-.-> A

&#x20;   L <-.-> B

```



\## Mandatory controls



\- Permit a small set of named query intents only.

\- Build SQL solely from server-owned templates; do not execute model-generated SQL.

\- Bind parameters through the application query builder rather than direct string interpolation.

\- Permit `SELECT` only. Reject DDL, DML, multiple statements, comments, system-catalog access, wildcard table references, and external URLs.

\- Query only approved Analytics views, never Raw, Clean, Quarantine, Metrics, Kafka, Spark checkpoints, Airflow metadata, or object-storage paths.

\- Use a dedicated read-only warehouse identity.

\- Require result-row limits, execution timeouts, scan-cost/billing limits, and structured audit logging.

\- Return a clarification request when the question does not match an approved intent.

\- Display the warehouse backend, data freshness timestamp, and query outcome in the response.



\## Allowlisted intent 1: latest price by symbol



\*\*Example NLQ:\*\* “What is the latest price for AAPL?”



\### Athena template



```sql

SELECT

&#x20; symbol,

&#x20; latest\_price,

&#x20; latest\_event\_time,

&#x20; event\_date

FROM v\_latest\_stock\_price\_summary

WHERE symbol = :symbol

LIMIT 1;

```



\### BigQuery template



```sql

SELECT

&#x20; symbol,

&#x20; latest\_price,

&#x20; latest\_event\_time,

&#x20; event\_date

FROM `PROJECT\_ID.DATASET\_ID.v\_latest\_stock\_price\_summary`

WHERE symbol = @symbol

LIMIT 1;

```



Validation:



\- `symbol` must match `^\[A-Z]{1,10}$`.

\- The application binds `:symbol` or `@symbol`; the LLM does not concatenate it into SQL.

\- A zero-row answer must be presented as “no result found,” not fabricated.



\## Allowlisted intent 2: latest prices for selected symbols



\*\*Example NLQ:\*\* “Show the latest prices for AAPL, MSFT, and NVDA.”



\### Athena template



```sql

SELECT

&#x20; symbol,

&#x20; latest\_price,

&#x20; latest\_event\_time,

&#x20; event\_date

FROM v\_latest\_stock\_price\_summary

WHERE symbol IN (:symbols)

ORDER BY symbol

LIMIT 20;

```



\### BigQuery template



```sql

SELECT

&#x20; symbol,

&#x20; latest\_price,

&#x20; latest\_event\_time,

&#x20; event\_date

FROM `PROJECT\_ID.DATASET\_ID.v\_latest\_stock\_price\_summary`

WHERE symbol IN UNNEST(@symbols)

ORDER BY symbol

LIMIT 20;

```



Validation:



\- Each requested symbol must match `^\[A-Z]{1,10}$`.

\- Accept at most 20 symbols.

\- The backend-specific query builder expands or binds the symbol list safely.



\## Allowlisted intent 3: daily summary by symbol and date



\*\*Example NLQ:\*\* “Give me AAPL’s summary for 2026-09-16.”



\### Athena template



```sql

SELECT

&#x20; symbol,

&#x20; event\_date,

&#x20; open\_price,

&#x20; high\_price,

&#x20; low\_price,

&#x20; close\_price,

&#x20; record\_count

FROM v\_daily\_stock\_price\_summary

WHERE symbol = :symbol

&#x20; AND event\_date = DATE :event\_date

LIMIT 1;

```



\### BigQuery template



```sql

SELECT

&#x20; symbol,

&#x20; event\_date,

&#x20; open\_price,

&#x20; high\_price,

&#x20; low\_price,

&#x20; close\_price,

&#x20; record\_count

FROM `PROJECT\_ID.DATASET\_ID.v\_daily\_stock\_price\_summary`

WHERE symbol = @symbol

&#x20; AND event\_date = @event\_date

LIMIT 1;

```



Validation:



\- `symbol` must match `^\[A-Z]{1,10}$`.

\- `event\_date` must be parsed as an ISO-8601 date.

\- The accepted date range is limited to retained Analytics history.



\## Request lifecycle



1\. Receive a user question and identify a supported intent.

2\. Extract only the intent’s permitted parameters.

3\. Validate parameters and reject unsupported or ambiguous requests.

4\. Select the configured cloud backend.

5\. Generate the query only from the backend-specific server template.

6\. Submit it through the read-only Athena workgroup or BigQuery service identity.

7\. Enforce time, row, and scan/billing limits.

8\. Return formatted rows plus backend and freshness information.

9\. Audit the intent name, validated parameters, backend, query ID/job ID, duration, bytes scanned, and outcome.



\## Non-goals



\- Financial advice, price predictions, or trading recommendations.

\- Arbitrary SQL or natural-language database exploration.

\- Writes, deletes, updates, DDL, warehouse administration, or access-policy changes.

\- Direct access to Kafka, Airflow, Spark, lake partitions, or cloud-storage objects.

\- Automated production remediation such as restarting streaming jobs or rerunning failed workflows.



\## Implementation gate



Implement this feature only after the Analytics schemas/views, cloud IAM boundaries, and Athena/BigQuery cost controls are stable. Until then, this file is a design artifact. Replace `PROJECT\_ID` and `DATASET\_ID` only through deployment configuration; never hard-code credentials or secrets.

