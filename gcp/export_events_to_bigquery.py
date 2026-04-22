from datetime import datetime, timezone

from cassandra.cluster import Cluster
from google.cloud import bigquery

PROJECT_ID = "steadfast-sign-473019-h5"
DATASET_ID = "realtime_pipeline"
TABLE_ID = "events"

CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "realtime_pipeline"


def fetch_events():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(CASSANDRA_KEYSPACE)

    rows = session.execute(
        "SELECT event_id, user_id, event_type, product_id, price, event_time, source FROM events;"
    )

    results = []
    for row in rows:
        results.append(
            {
                "event_id": row.event_id,
                "user_id": row.user_id,
                "event_type": row.event_type,
                "product_id": row.product_id,
                "price": float(row.price) if row.price is not None else None,
                "event_time": row.event_time.isoformat() if row.event_time is not None else None,
                "source": row.source,
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
    event_rows = fetch_events()

    if event_rows:
        load_to_bigquery(event_rows)
    else:
        print("No rows found in Cassandra events table.")