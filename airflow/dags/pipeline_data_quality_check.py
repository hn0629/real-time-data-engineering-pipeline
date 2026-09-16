from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator


DAG_ID = "pipeline_data_quality_check"

CLEAN_DATASET_PATH = Path("/opt/spark-data/clean/stock_prices")
PARTITION_PREFIX = "event_date="

MIN_ROWS = 1
MAX_NULL_RATE = 0.05
MAX_INVALID_PRICE_RATE = 0.01
MAX_INVALID_EVENT_TIME_RATE = 0.01
MAX_DUPLICATE_KAFKA_RECORD_RATE = 0.0
MAX_FUTURE_EVENT_TIME_MINUTES = 10

REQUIRED_COLUMNS = [
    "symbol",
    "price",
    "source",
    "event_time",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
    "batch_id",
    "raw_payload",
]

KAFKA_RECORD_KEY_COLUMNS = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
]


def find_latest_partition(base_path: Path) -> Tuple[Path, List[Path]]:
    """Find the newest event_date partition and its Parquet files."""
    if not base_path.exists():
        raise AirflowFailException(
            "Clean stock-price path does not exist: {0}".format(base_path)
        )

    partitions = sorted(
        directory
        for directory in base_path.iterdir()
        if directory.is_dir() and directory.name.startswith(PARTITION_PREFIX)
    )

    if not partitions:
        raise AirflowFailException(
            "No {0} partitions found in {1}".format(
                PARTITION_PREFIX,
                base_path,
            )
        )

    latest_partition = partitions[-1]

    parquet_files = sorted(
        file_path
        for file_path in latest_partition.glob("*.parquet")
        if file_path.is_file()
    )

    if not parquet_files:
        raise AirflowFailException(
            "No Parquet files found in latest partition: {0}".format(
                latest_partition
            )
        )

    return latest_partition, parquet_files


def calculate_rate(count: int, total: int) -> float:
    """Calculate a safe ratio for quality reporting."""
    if total <= 0:
        return 0.0

    return round(float(count) / float(total), 6)


def log_and_fail_if_needed(
    quality_report: Dict[str, object],
    failures: List[str],
) -> None:
    """Log a JSON report and fail the Airflow task when any check fails."""
    logger = logging.getLogger(__name__)

    if failures:
        quality_report["status"] = "failed"

    logger.info(
        "Data quality report:\n%s",
        json.dumps(quality_report, indent=2, default=str),
    )

    if failures:
        raise AirflowFailException(
            "Data quality checks failed:\n- {0}".format(
                "\n- ".join(failures)
            )
        )


def run_data_quality_checks() -> None:
    """Validate the newest clean Spark price-tick Parquet partition."""
    logger = logging.getLogger(__name__)

    latest_partition, parquet_files = find_latest_partition(CLEAN_DATASET_PATH)

    logger.info("Latest partition: %s", latest_partition)
    logger.info("Found %s Parquet files in latest partition.", len(parquet_files))
    logger.info("Reading the latest clean partition.")

    dataframe = pd.read_parquet(
        str(latest_partition),
        engine="pyarrow",
    )

    total_rows = int(len(dataframe.index))
    available_columns = [str(column) for column in dataframe.columns]

    logger.info(
        "Loaded %s rows and %s columns from latest partition.",
        total_rows,
        len(available_columns),
    )
    logger.info("Available columns: %s", available_columns)

    quality_report: Dict[str, object] = {
        "dag_id": DAG_ID,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_path": str(CLEAN_DATASET_PATH),
        "partition_path": str(latest_partition),
        "parquet_file_count": len(parquet_files),
        "row_count": total_rows,
        "column_count": len(available_columns),
        "columns": available_columns,
        "checks": {},
        "status": "passed",
    }

    failures: List[str] = []

    row_count_passed = total_rows >= MIN_ROWS
    quality_report["checks"]["minimum_row_count"] = {
        "passed": row_count_passed,
        "actual_rows": total_rows,
        "minimum_rows": MIN_ROWS,
    }

    if not row_count_passed:
        failures.append(
            "Row count is {0}; expected at least {1}.".format(
                total_rows,
                MIN_ROWS,
            )
        )

    missing_required_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in dataframe.columns
    ]

    required_columns_passed = len(missing_required_columns) == 0
    quality_report["checks"]["required_columns"] = {
        "passed": required_columns_passed,
        "required_columns": REQUIRED_COLUMNS,
        "missing_columns": missing_required_columns,
    }

    if not required_columns_passed:
        failures.append(
            "Missing required columns: {0}.".format(
                ", ".join(missing_required_columns)
            )
        )

        log_and_fail_if_needed(quality_report, failures)
        return

    null_counts = dataframe[REQUIRED_COLUMNS].isna().sum().to_dict()
    null_rates = {
        str(column): calculate_rate(int(null_count), total_rows)
        for column, null_count in null_counts.items()
    }

    columns_over_null_threshold = [
        column
        for column, null_rate in null_rates.items()
        if null_rate > MAX_NULL_RATE
    ]

    null_check_passed = len(columns_over_null_threshold) == 0
    quality_report["checks"]["required_column_null_rates"] = {
        "passed": null_check_passed,
        "maximum_allowed_null_rate": MAX_NULL_RATE,
        "null_rates": null_rates,
        "columns_over_threshold": columns_over_null_threshold,
    }

    if not null_check_passed:
        failures.append(
            "Null rate exceeded {0:.2%} for: {1}.".format(
                MAX_NULL_RATE,
                ", ".join(columns_over_null_threshold),
            )
        )

    empty_symbol_count = int(
        dataframe["symbol"].fillna("").astype(str).str.strip().eq("").sum()
    )
    empty_symbol_rate = calculate_rate(empty_symbol_count, total_rows)
    symbol_check_passed = empty_symbol_rate <= MAX_NULL_RATE

    quality_report["checks"]["symbol_not_empty"] = {
        "passed": symbol_check_passed,
        "empty_symbol_count": empty_symbol_count,
        "empty_symbol_rate": empty_symbol_rate,
        "maximum_allowed_empty_symbol_rate": MAX_NULL_RATE,
    }

    if not symbol_check_passed:
        failures.append(
            "Empty symbol rate is {0:.2%}; maximum is {1:.2%}.".format(
                empty_symbol_rate,
                MAX_NULL_RATE,
            )
        )

    numeric_price = pd.to_numeric(dataframe["price"], errors="coerce")
    invalid_price_count = int((numeric_price.isna() | (numeric_price <= 0)).sum())
    invalid_price_rate = calculate_rate(invalid_price_count, total_rows)
    price_check_passed = invalid_price_rate <= MAX_INVALID_PRICE_RATE

    quality_report["checks"]["price_positive"] = {
        "passed": price_check_passed,
        "invalid_price_count": invalid_price_count,
        "invalid_price_rate": invalid_price_rate,
        "maximum_allowed_invalid_price_rate": MAX_INVALID_PRICE_RATE,
    }

    if not price_check_passed:
        failures.append(
            "Invalid/non-positive price rate is {0:.2%}; maximum is {1:.2%}.".format(
                invalid_price_rate,
                MAX_INVALID_PRICE_RATE,
            )
        )

    parsed_event_time = pd.to_datetime(
        dataframe["event_time"],
        errors="coerce",
        utc=True,
    )
    invalid_event_time_count = int(parsed_event_time.isna().sum())
    invalid_event_time_rate = calculate_rate(invalid_event_time_count, total_rows)
    event_time_check_passed = (
        invalid_event_time_rate <= MAX_INVALID_EVENT_TIME_RATE
    )

    quality_report["checks"]["event_time_parseable"] = {
        "passed": event_time_check_passed,
        "invalid_event_time_count": invalid_event_time_count,
        "invalid_event_time_rate": invalid_event_time_rate,
        "maximum_allowed_invalid_event_time_rate": MAX_INVALID_EVENT_TIME_RATE,
    }

    if not event_time_check_passed:
        failures.append(
            "Invalid event_time rate is {0:.2%}; maximum is {1:.2%}.".format(
                invalid_event_time_rate,
                MAX_INVALID_EVENT_TIME_RATE,
            )
        )

    future_limit = pd.Timestamp.now(tz="UTC") + pd.Timedelta(
        minutes=MAX_FUTURE_EVENT_TIME_MINUTES
    )
    future_event_time_count = int(
        ((parsed_event_time.notna()) & (parsed_event_time > future_limit)).sum()
    )
    future_event_time_rate = calculate_rate(future_event_time_count, total_rows)
    future_event_time_check_passed = future_event_time_count == 0

    quality_report["checks"]["event_time_not_far_future"] = {
        "passed": future_event_time_check_passed,
        "future_event_time_count": future_event_time_count,
        "future_event_time_rate": future_event_time_rate,
        "future_time_limit_utc": future_limit.isoformat(),
    }

    if not future_event_time_check_passed:
        failures.append(
            "Found {0} event_time values more than {1} minutes in the future.".format(
                future_event_time_count,
                MAX_FUTURE_EVENT_TIME_MINUTES,
            )
        )

    duplicate_kafka_records = int(
        dataframe.duplicated(subset=KAFKA_RECORD_KEY_COLUMNS).sum()
    )
    duplicate_kafka_record_rate = calculate_rate(
        duplicate_kafka_records,
        total_rows,
    )
    duplicate_kafka_check_passed = (
        duplicate_kafka_record_rate <= MAX_DUPLICATE_KAFKA_RECORD_RATE
    )

    quality_report["checks"]["duplicate_kafka_records"] = {
        "passed": duplicate_kafka_check_passed,
        "key_columns": KAFKA_RECORD_KEY_COLUMNS,
        "duplicate_record_count": duplicate_kafka_records,
        "duplicate_record_rate": duplicate_kafka_record_rate,
        "maximum_allowed_duplicate_record_rate": (
            MAX_DUPLICATE_KAFKA_RECORD_RATE
        ),
    }

    if not duplicate_kafka_check_passed:
        failures.append(
            "Duplicate Kafka record rate is {0:.2%}; maximum is {1:.2%}.".format(
                duplicate_kafka_record_rate,
                MAX_DUPLICATE_KAFKA_RECORD_RATE,
            )
        )

    kafka_partition = pd.to_numeric(
        dataframe["kafka_partition"],
        errors="coerce",
    )
    invalid_kafka_partition_count = int(
        (kafka_partition.isna() | (kafka_partition < 0)).sum()
    )

    kafka_offset = pd.to_numeric(
        dataframe["kafka_offset"],
        errors="coerce",
    )
    invalid_kafka_offset_count = int(
        (kafka_offset.isna() | (kafka_offset < 0)).sum()
    )

    kafka_lineage_check_passed = (
        invalid_kafka_partition_count == 0
        and invalid_kafka_offset_count == 0
    )

    quality_report["checks"]["kafka_lineage_values"] = {
        "passed": kafka_lineage_check_passed,
        "invalid_kafka_partition_count": invalid_kafka_partition_count,
        "invalid_kafka_offset_count": invalid_kafka_offset_count,
    }

    if not kafka_lineage_check_passed:
        failures.append(
            "Kafka lineage validation failed: {0} invalid partition value(s), "
            "{1} invalid offset value(s).".format(
                invalid_kafka_partition_count,
                invalid_kafka_offset_count,
            )
        )

    batch_id = pd.to_numeric(dataframe["batch_id"], errors="coerce")
    invalid_batch_id_count = int((batch_id.isna() | (batch_id < 0)).sum())
    batch_id_check_passed = invalid_batch_id_count == 0

    quality_report["checks"]["batch_id_valid"] = {
        "passed": batch_id_check_passed,
        "invalid_batch_id_count": invalid_batch_id_count,
    }

    if not batch_id_check_passed:
        failures.append(
            "Found {0} invalid batch_id value(s).".format(
                invalid_batch_id_count
            )
        )

    log_and_fail_if_needed(quality_report, failures)

    logger.info("All data quality checks passed.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id=DAG_ID,
    description="Validate the newest clean Spark stock-price event partition.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "spark", "parquet", "kafka", "stock-prices"],
) as dag:
    check_clean_stock_price_data = PythonOperator(
        task_id="check_clean_stock_price_data",
        python_callable=run_data_quality_checks,
    )