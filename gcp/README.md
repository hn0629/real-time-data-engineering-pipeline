# Google Cloud / BigQuery Export Utilities

## Purpose

This directory contains optional Python utilities that export Cassandra datasets to Google BigQuery.

These scripts are retained as an optional cloud-warehouse integration and are not required to run the current local stock-price streaming pipeline.

## Scripts

| Script | Cassandra source | BigQuery destination |
|---|---|---|
| `export_events_to_bigquery.py` | `events` | `realtime_pipeline.events` |
| `export_metrics_to_bigquery.py` | `source_metrics` | `realtime_pipeline.source_metrics` |

Both scripts connect to the Cassandra keyspace configured through the project `config` module, retrieve rows, and use the BigQuery Python client to insert JSON rows into BigQuery.

## Requirements

- Python 3.11 or compatible Python environment
- `cassandra-driver`
- `google-cloud-bigquery`
- A reachable Cassandra instance with the expected keyspace and tables
- A Google Cloud project with:
  - BigQuery API enabled
  - Dataset `realtime_pipeline`
  - Tables `events` and `source_metrics`
  - Permission to insert rows into those tables

## Authentication

The scripts use Google Application Default Credentials (ADC) through:

```python
bigquery.Client(project=PROJECT_ID)
```

For local development, authenticate with the Google Cloud CLI:

```powershell
gcloud auth application-default login
```

Do not place service-account JSON keys in this repository. If a service-account key is unavoidable for a non-local environment, store it outside the repository and expose its location with the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

## Run Commands

Run from the repository root after installing dependencies and configuring access to Cassandra:

```powershell
python .\gcp\export_events_to_bigquery.py
```

```powershell
python .\gcp\export_metrics_to_bigquery.py
```

## Current Architecture Status

The current local stock-price streaming pipeline uses:

```text
Stock producer → Kafka → Spark → Parquet → S3/Athena
```

Cassandra-to-BigQuery exports are maintained as an optional or legacy extension. They are not started by the current `docker-compose.yml`, which does not include a Cassandra service.