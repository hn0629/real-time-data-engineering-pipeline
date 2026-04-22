from datetime import datetime, timezone

from cassandra.cluster import Cluster
from google.cloud import bigquery

PROJECT_ID = "steadfast-sign-473019-h5"
DATASET_ID = "realtime_pipeline"
TABLE_ID = "source_metrics"

CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "realtime_pipeline"


def fetch_source_metrics():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)

    rows = session.execute(
        "SELECT source, event_count, total_revenue FROM source_metrics;"
    )

    results = []
    for row in rows:
        results.append(
            {
                "source": row.source,
                "event_count": int(row.event_count) if row.event_count is not None else 0,
                "total_revenue": float(row.total_revenue) if row.total_revenue is not None else 0.0,
                "loaded_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    cluster.shutdown()
    return results


def load_to_bigquery(rows):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    errors = client.insert_rows_json(table_id, rows)

    if errors:
        print("BigQuery insert errors:")
        for error in errors:
            print(error)
    else:
        print(f"Inserted {len(rows)} rows into {table_id}")


if __name__ == "__main__":
    metrics_rows = fetch_source_metrics()

    if metrics_rows:
        load_to_bigquery(metrics_rows)
    else:
        print("No rows found in Cassandra source_metrics table.")