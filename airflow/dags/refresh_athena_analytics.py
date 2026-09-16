import os
import re
import time
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator


ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "realtime_pipeline")
ATHENA_OUTPUT = os.getenv(
    "ATHENA_OUTPUT",
    "s3://hoang-real-time-data-pipeline-2026/real-time-pipeline/athena-results/",
)
S3_BUCKET = os.getenv("S3_BUCKET", "hoang-real-time-data-pipeline-2026")
S3_PREFIX = os.getenv("S3_PREFIX", "real-time-pipeline").strip("/")
AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
SOURCE_TABLE = os.getenv("ATHENA_SOURCE_TABLE", "clean_stock_prices")
VIEW_NAME = "v_latest_daily_symbol_metrics"


def get_athena_client():
    return boto3.client("athena", region_name=AWS_REGION)


def run_athena_query(query: str) -> str:
    client = get_athena_client()
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    query_execution_id = response["QueryExecutionId"]
    print(f"Athena query ID: {query_execution_id}")

    while True:
        execution = client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = execution["QueryExecution"]["Status"]
        state = status["State"]

        if state == "SUCCEEDED":
            return query_execution_id

        if state in {"FAILED", "CANCELLED"}:
            reason = status.get("StateChangeReason", "No failure reason returned")
            raise RuntimeError(
                f"Athena query failed. QueryExecutionId={query_execution_id}; "
                f"State={state}; Reason={reason}"
            )

        time.sleep(2)


def refresh_daily_symbol_metrics(**context) -> str:
    ti = context["ti"]
    run_id = context["run_id"]
    now = datetime.utcnow()

    event_date = now.strftime("%Y-%m-%d")
    run_timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
    table_name = f"daily_symbol_metrics_{run_timestamp}"
    try_number = ti.try_number

    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id)
    snapshot_location = (
        f"s3://{S3_BUCKET}/{S3_PREFIX}/analytics/"
        f"daily_symbol_metrics_snapshots/"
        f"event_date={event_date}/"
        f"run_id={safe_run_id}/"
        f"try={try_number}/"
        f"table={table_name}/"
    )

    query = f"""
    CREATE TABLE {ATHENA_DATABASE}.{table_name}
    WITH (
        format = 'PARQUET',
        external_location = '{snapshot_location}',
        parquet_compression = 'SNAPPY'
    ) AS
    SELECT
        CAST(date(event_time) AS date) AS event_date,
        symbol,
        source,
        COUNT(*) AS event_count,
        AVG(price) AS average_price,
        MIN(price) AS minimum_price,
        MAX(price) AS maximum_price,
        MAX(event_time) AS latest_event_time
    FROM {ATHENA_DATABASE}.{SOURCE_TABLE}
    WHERE CAST(date(event_time) AS date) = current_date
    GROUP BY
        CAST(date(event_time) AS date),
        symbol,
        source
    """

    print(f"Creating snapshot table: {ATHENA_DATABASE}.{table_name}")
    print(f"Task attempt: {try_number}")
    print(f"Snapshot location: {snapshot_location}")
    query_execution_id = run_athena_query(query)

    print("Analytics refresh succeeded.")
    print(f"Table: {ATHENA_DATABASE}.{table_name}")
    print(f"Location: {snapshot_location}")
    print(f"Query execution ID: {query_execution_id}")

    return table_name


def update_latest_metrics_view(**context) -> None:
    table_name = context["ti"].xcom_pull(
        task_ids="refresh_daily_symbol_metrics",
        key="return_value",
    )

    if not table_name:
        raise ValueError(
            "No snapshot table name was returned by refresh_daily_symbol_metrics."
        )

    table_name = str(table_name).strip()

    if table_name.startswith(f"{ATHENA_DATABASE}."):
        table_name = table_name.split(".", 1)[1]

    if not re.fullmatch(r"daily_symbol_metrics_[A-Za-z0-9_]+", table_name):
        raise ValueError(f"Unexpected snapshot table name from XCom: {table_name!r}")

    query = f"""
    CREATE OR REPLACE VIEW {ATHENA_DATABASE}.{VIEW_NAME} AS
    SELECT *
    FROM {ATHENA_DATABASE}.{table_name}
    """

    print(f"Received XCom snapshot table: {table_name}")
    print(f"Updating stable view: {ATHENA_DATABASE}.{VIEW_NAME}")
    query_execution_id = run_athena_query(query)

    print("Latest metrics view updated successfully.")
    print(f"View: {ATHENA_DATABASE}.{VIEW_NAME}")
    print(f"Source table: {ATHENA_DATABASE}.{table_name}")
    print(f"Query execution ID: {query_execution_id}")


with DAG(
    dag_id="refresh_athena_analytics",
    description="Create a unique Athena metrics snapshot and update the stable latest-metrics view.",
    start_date=datetime(2026, 9, 4),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["athena", "analytics", "stocks"],
) as dag:
    refresh_task = PythonOperator(
        task_id="refresh_daily_symbol_metrics",
        python_callable=refresh_daily_symbol_metrics,
    )

    update_view_task = PythonOperator(
        task_id="update_latest_metrics_view",
        python_callable=update_latest_metrics_view,
    )

    refresh_task >> update_view_task