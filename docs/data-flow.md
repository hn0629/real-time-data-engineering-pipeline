## Cloud warehouse branches

The pipeline has two separately managed cloud-consumption paths:

- **AWS path:** Raw, Clean, and Analytics Parquet data can be synchronized to Amazon S3. Athena exposes approved Analytics tables or views for read-only analysis.
- **GCP path:** Selected event, metric, or analytics outputs can be exported to Google Cloud Storage and/or BigQuery. BigQuery provides a separate read-only analytics surface.

The AWS and GCP paths must be treated as independent integrations. Spark remains the owner of local lake writes. Cloud export jobs copy approved outputs outward; they do not alter Raw, Clean, Quarantine, or Analytics contracts locally.

## Read and write boundaries

Spark writes Raw, Quarantine, Clean, Metrics, and Analytics outputs. The streaming job owns ingestion-oriented writes; the analytics job owns derived analytics writes. Event-level datasets are partitioned by `event_date`.

Airflow is a read-only orchestration and validation plane for lake data. It reads the most recent Clean partition and operational artifacts, then records task state in Airflow logs. Airflow does not overwrite Parquet partitions, delete records, or modify Raw data.

Athena and BigQuery are read-only consumer surfaces over approved analytics representations. The Streamlit dashboard and future LLM layer query only these approved analytics outputs; they do not connect directly to Kafka or write to the lake.