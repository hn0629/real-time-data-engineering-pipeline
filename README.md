# Real-Time Stock Price Data Engineering Pipeline

A containerized, end-to-end streaming data pipeline that ingests stock-price events from Kafka, processes and validates them with Apache Spark Structured Streaming, persists layered Parquet datasets, publishes analytics-ready data to Amazon S3 and Athena, monitors pipeline health through Apache Airflow, and exposes results through a Streamlit dashboard with an LLM-powered natural-language analytics assistant.

The project is designed as a local, reproducible data-engineering portfolio implementation. It emphasizes streaming ingestion, data quality, layered storage, cloud analytics, orchestration, monitoring, and AI-assisted data exploration.

For the full system design, see [ARCHITECTURE.md](ARCHITECTURE.md).

<p align="center">
  <img src="images/new%20dash.png" alt="Real-time stock price dashboard" width="900">
</p>

## Architecture

```mermaid
flowchart TB
    P\[Stock Price Producer<br/>Python] -->|JSON stock ticks| K\[Apache Kafka<br/>pipeline-events]

    K -->|Structured Streaming| S\[Apache Spark<br/>validates and deduplicates]

    S -->|Raw events| R\[Raw Parquet<br/>data/raw/stock\_prices]
    S -->|Validated events| C\[Clean Parquet<br/>data/clean/stock\_prices]
    S -->|Invalid records| Q\[Quarantine Parquet<br/>data/quarantine/stock\_prices]
    S -->|Batch metrics| M\[Metrics Parquet<br/>data/metrics/stream\_batches]

    C -->|Batch analytics job| A\[Analytics Parquet<br/>stock\_price\_summary]
    A -->|sync\_to\_s3.ps1| S3\[Amazon S3]
    S3 -->|Query| ATH\[Amazon Athena<br/>v\_latest\_stock\_price\_summary]

    A -->|Read Parquet| D\[Streamlit Dashboard]
    D -->|Natural-language queries| LLM\[Pipeline Analytics Assistant<br/>LLM-powered]

    AF\[Apache Airflow] -->|Every 5 min| H\[Health Check<br/>pipeline\_status.json]
    AF -->|Scheduled| DQ\[Data Quality Check]
    AF -->|Every 30 min| AR\[Athena Refresh]

    H -->|Kafka, Spark, PostgreSQL| MON\[data/monitoring<br/>JSON + log]

    S -.->|Optional export| BQ\[Google BigQuery<br/>realtime\_pipeline]
    M -.->|Optional export| BQ
```

## Pipeline Flow

1. The Python producer publishes JSON stock-price events to the Kafka topic `pipeline-events`.
2. Spark Structured Streaming reads Kafka micro-batches every 10 seconds.
3. Each batch is retained in the raw layer for traceability.
4. Spark validates that each event has a non-empty `symbol`, a positive numeric `price`, and an `event\_time`.
5. Valid events are deduplicated by Kafka topic, partition, and offset, then written to partitioned clean Parquet data.
6. Invalid events are written to the quarantine layer with a validation-error reason.
7. Spark writes batch-level counts for raw, clean, and quarantined records.
8. The analytics job creates daily stock-price summaries by event date, symbol, and source.
9. Analytics Parquet is synchronized to S3 and queried in Athena.
10. Airflow schedules quality checks, Athena refresh work, and infrastructure health checks.
11. The Streamlit dashboard reads the latest Analytics partition and pipeline health status.
12. The Pipeline Analytics Assistant uses an LLM to answer natural-language questions about pipeline output and stock-price summaries.

## Data Layers

|Layer|Location|Purpose|
|-|-|-|
|Raw|`data/raw/stock\_prices`|Immutable record of ingested Kafka payloads and ingestion metadata|
|Clean|`data/clean/stock\_prices`|Validated and deduplicated stock-price events, partitioned by `event\_date`|
|Quarantine|`data/quarantine/stock\_prices`|Rejected events with validation-error context|
|Metrics|`data/metrics/stream\_batches`|Per-micro-batch raw, clean, and quarantine counts|
|Analytics|`data/analytics/stock\_price\_summary`|Daily summary by date, symbol, and source|
|Monitoring|`data/monitoring`|Current pipeline health JSON and append-only health history|

## Analytics Model

The Spark analytics job produces daily stock-price summaries grouped by:

```text
event\_date, symbol, source
```

Each summary includes:

* `tick\_count`
* `min\_price`
* `max\_price`
* `avg\_price`
* `latest\_price`
* `first\_event\_time`
* `last\_event\_time`
* `processed\_at`

The analytics dataset is partitioned by `event\_date`, making it suitable for partition-aware cloud queries.

## Pipeline Analytics Assistant

The dashboard includes an LLM-powered natural-language assistant that answers questions about pipeline output and stock-price analytics. It is accessible as a sidebar page in the Streamlit dashboard.

**Supported questions:**

* `Summarize the latest pipeline output`
* `What is the latest price for AAPL?`
* `Show latest prices for AAPL, MSFT, and NVDA`
* `Give me AAPL summary for 2026-09-15`

The assistant reads the same Analytics Parquet partition as the dashboard and uses an allowlisted query set. It refuses financial advice requests and does not execute arbitrary SQL, mutate pipeline state, or control operational services.

## Tech Stack

|Layer|Technology|Purpose|
|-|-|-|
|Event producer|Python|Publishes stock-price tick events|
|Streaming ingestion|Apache Kafka|Durable event transport through `pipeline-events`|
|Stream processing|Apache Spark 3.5|Validation, deduplication, Parquet persistence, metrics|
|Storage format|Apache Parquet|Columnar storage for raw, clean, quarantine, metrics, and analytics layers|
|Orchestration|Apache Airflow 2.7|Data-quality, health-check, and Athena-refresh workflows|
|Cloud storage|Amazon S3|Stores synchronized analytics Parquet data|
|Query engine|Amazon Athena|Queries S3-backed analytics datasets|
|Dashboard|Streamlit|Displays the analytics layer locally|
|AI assistant|OpenAI API|Natural-language query routing for pipeline analytics|
|Metadata database|PostgreSQL 14|Stores Airflow metadata|
|Containerization|Docker Compose|Reproducible local multi-service environment|
|Optional warehouse export|Google BigQuery|Optional cloud-warehouse export for events and metrics|

## Airflow Operations

|DAG|Schedule|Responsibility|
|-|-|-|
|`pipeline\_data\_quality\_check`|Every 5 minutes|Validates pipeline data-quality expectations|
|`pipeline\_health\_check`|Every 5 minutes|Verifies Kafka, Spark master, and PostgreSQL TCP connectivity|
|`refresh\_athena\_analytics`|Every 30 minutes|Refreshes the Athena analytics view|

**Retry logic:** Each Airflow task is configured with `retries=1` and `retry\_delay=1 minute`. If a task fails (e.g., Kafka is temporarily unreachable), Airflow retries it once after one minute. If the retry also fails, the task is marked failed and the failure is recorded in the monitoring artifacts.

The health-check DAG writes operational artifacts to:

```text
data/monitoring/
├── pipeline\_status.json
└── pipeline\_health.log
```

`pipeline\_status.json` contains the latest structured health snapshot. `pipeline\_health.log` is an append-only execution history. These runtime files are intentionally excluded from version control.

For full operational details, see [MONITORING.md](MONITORING.md) and [RUNBOOK.md](RUNBOOK.md).

## Project Structure

```text
Real-Time-Data-Engineering-Pipeline/
├── airflow/
│   └── dags/
│       ├── pipeline\_data\_quality\_check.py
│       ├── pipeline\_health\_check.py
│       └── refresh\_athena\_analytics.py
├── dashboard/
│   ├── app.py
│   └── pages/
│       └── 1\_Pipeline\_Analytics\_Assistant.py
├── data/
│   └── monitoring/
│       └── .gitkeep
├── gcp/
│   ├── README.md
│   ├── export\_events\_to\_bigquery.py
│   └── export\_metrics\_to\_bigquery.py
├── images/
├── llm/
│   ├── \_\_init\_\_.py
│   ├── analytics\_backend.py
│   ├── assistant.py
│   └── demo.py
├── producer/
│   ├── requirements.txt
│   └── stock\_producer.py
├── spark/
│   └── jobs/
│       ├── build\_analytics\_layer.py
│       ├── kafka\_stream.py
│       └── smoke\_test.py
├── tests/
├── utils/
├── ARCHITECTURE.md
├── docker-compose.yml
├── MONITORING.md
├── RUNBOOK.md
├── start\_stream.ps1
├── sync\_to\_s3.ps1
└── run\_health\_check.ps1
```

## Run Locally

### Prerequisites

* Docker Desktop with Docker Compose
* Python, if you want to run local helper scripts
* AWS credentials configured only when using S3/Athena features
* An OpenAI API key in `.env` for the Pipeline Analytics Assistant
* A `.env` file with your local configuration; do not commit this file

### 1\. Start the stack

From the repository root:

```powershell
docker compose up -d
```

Check service status:

```powershell
docker compose ps
```

Expected core services include:

* `airflow`
* `airflow-scheduler`
* `broker`
* `postgres`
* `spark-master`
* `spark-worker`
* `stock-producer`
* `pipeline-dashboard`

### 2\. Start the streaming job

Use the project helper script:

```powershell
.\\start\_stream.ps1
```

The streaming job reads from Kafka and writes to the raw, clean, quarantine, metrics, and checkpoint locations under `data/`.

### 3\. Build the analytics layer

After clean events exist, submit the analytics job to Spark:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-apps/build\_analytics\_layer.py
```

### 4\. Open local interfaces

|Service|Local URL|
|-|-|
|Airflow|`http://localhost:8080`|
|Spark master UI|`http://localhost:8081`|
|Streamlit dashboard|`http://localhost:8501`|

The Streamlit dashboard sidebar includes a **Pipeline Analytics Assistant** page for natural-language queries.

### 5\. Verify monitoring

Trigger `pipeline\_health\_check` in Airflow or wait for its five-minute schedule. Then inspect:

```powershell
Get-Content .\\data\\monitoring\\pipeline\_status.json
Get-Content .\\data\\monitoring\\pipeline\_health.log -Tail 5
```

A healthy result contains:

```json
{
  "status": "healthy",
  "errors\_seen": \[]
}
```

## AWS Analytics

The analytics dataset is synchronized from local Parquet storage to Amazon S3. Athena queries the S3-backed analytics data, including the latest stock-price summary view.

The intended analytics workflow is:

```text
Clean Parquet → Analytics Parquet → Amazon S3 → AWS Athena
```

The `sync\_to\_s3.ps1` helper is used to synchronize local data to S3. AWS credentials and bucket details remain in local configuration and are never committed.

## Optional Google BigQuery Export

The `gcp/` directory contains optional export utilities that load pipeline events and streaming metrics into Google BigQuery under the `realtime\_pipeline` dataset (GCP project: `steadfast-sign-473019-h5`).

|Script|Source|BigQuery Table|
|-|-|-|
|`gcp/export\_events\_to\_bigquery.py`|Pipeline events|`realtime\_pipeline.events`|
|`gcp/export\_metrics\_to\_bigquery.py`|Streaming metrics|`realtime\_pipeline.metrics`|

These scripts are retained as an optional secondary cloud-warehouse path for Looker Studio dashboards and BigQuery analysis. They are not required to run the current local stock-price streaming pipeline, which uses **AWS S3 + Athena as its primary validated analytics path**.

See [gcp/README.md](gcp/README.md) for requirements and usage.

## Validation and Testing

Run Python tests from the project root:

```powershell
python -m pytest tests -v
```

Run the Spark smoke test:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark-apps/smoke\_test.py
```

Operational validation checklist:

* Docker services are running: `docker compose ps`
* Spark stream is active and writing data
* Clean, quarantine, metrics, and analytics layers exist under `data/`
* Athena can query the S3-backed analytics table/view
* Airflow health check succeeds
* `data/monitoring/pipeline\_status.json` reports `healthy`
* Streamlit dashboard renders latest Analytics Parquet output
* Pipeline Analytics Assistant answers allowlisted natural-language queries

## Screenshots

### Airflow DAGs

!\[Airflow DAGs](images/Dags%20Airflow.png)

### Spark

!\[Spark execution](images/spark.png)

## Engineering Decisions

* **Layered Parquet storage:** Raw, clean, quarantine, metrics, and analytics layers preserve traceability while separating valid records from bad data.
* **Kafka-offset deduplication:** Valid events are deduplicated using Kafka topic, partition, and offset, creating a stable event identity within the stream.
* **Partitioned analytics:** Clean data and analytics are partitioned by event date to support efficient incremental cloud queries.
* **Quarantine instead of silent drops:** Invalid records are retained with error reasons instead of being discarded.
* **Independent orchestration:** Airflow controls data quality, infrastructure health, and analytics refresh as separate scheduled workflows.
* **Retry logic:** Airflow tasks retry once after one minute, providing resilience against transient failures without masking persistent issues.
* **Machine-readable monitoring:** The health DAG writes a current JSON status file and a persistent log in addition to Airflow task logs.
* **Read-only AI assistant:** The Pipeline Analytics Assistant uses an allowlisted query set and refuses financial advice. It does not write data, execute arbitrary SQL, or control services.
* **Local-first reproducibility:** Docker Compose runs the local stack, while S3/Athena integration provides the cloud analytics path.
* **Honest optional GCP support:** BigQuery export utilities are isolated and documented separately as an optional cloud-warehouse path, while AWS S3 + Athena is the primary validated analytics path.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).

