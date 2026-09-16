import json
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator


SERVICES = {
    "Kafka": ("broker", 29092),
    "Spark master": ("spark-master", 7077),
    "PostgreSQL": ("postgres", 5432),
}

# These paths are inside the Docker containers.
# Your Docker volume mapping should make them appear in Windows at:
# .\data\monitoring\
MONITORING_DIR = Path("/opt/spark-data/monitoring")
STATUS_FILE = MONITORING_DIR / "pipeline_status.json"
LOG_FILE = MONITORING_DIR / "pipeline_health.log"


def utc_now() -> str:
    """Return the current time as an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def write_monitoring_files(
    status: str,
    services: Dict,
    errors: List[str],
) -> None:
    """
    Overwrite the latest JSON health report and append one short
    history line to the health log.
    """
    MONITORING_DIR.mkdir(parents=True, exist_ok=True)

    payload = {
        "status": status,
        "checked_at_utc": utc_now(),
        "services": services,
        "errors_seen": errors,
    }

    with STATUS_FILE.open("w", encoding="utf-8") as status_file:
        json.dump(payload, status_file, indent=2)

    log_line = (
        "{timestamp} status={status} errors={error_count}".format(
            timestamp=payload["checked_at_utc"],
            status=status,
            error_count=len(errors),
        )
    )

    if errors:
        log_line += " details={}".format(" | ".join(errors))

    with LOG_FILE.open("a", encoding="utf-8") as log_file:
        log_file.write(log_line + "\n")

    print("Monitoring JSON written to: {}".format(STATUS_FILE))
    print("Monitoring log updated at: {}".format(LOG_FILE))


def check_pipeline_services() -> None:
    """
    Verify that Airflow can reach each required Docker service.

    The Airflow task passes only if all services can be reached.
    A status report is still written before the task fails, so the
    most recent failure is available in pipeline_status.json.
    """
    service_results = {}
    failed = []

    for service_name, endpoint in SERVICES.items():
        host, port = endpoint
        checked_at = utc_now()

        try:
            with socket.create_connection((host, port), timeout=10):
                message = "{} is reachable at {}:{}".format(
                    service_name,
                    host,
                    port,
                )

                print("SUCCESS: {}".format(message))

                service_results[service_name] = {
                    "host": host,
                    "port": port,
                    "status": "healthy",
                    "checked_at_utc": checked_at,
                    "message": message,
                }

        except OSError as exc:
            message = "{} ({}:{}): {}".format(
                service_name,
                host,
                port,
                exc,
            )

            print("FAILED: {}".format(message))
            failed.append(message)

            service_results[service_name] = {
                "host": host,
                "port": port,
                "status": "failed",
                "checked_at_utc": checked_at,
                "message": message,
            }

    overall_status = "healthy" if not failed else "failed"

    write_monitoring_files(
        status=overall_status,
        services=service_results,
        errors=failed,
    )

    if failed:
        raise RuntimeError(
            "Unreachable pipeline services: {}".format(
                "; ".join(failed)
            )
        )

    print("SUCCESS: All pipeline services are reachable.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="pipeline_health_check",
    description="Checks TCP connectivity and writes pipeline health artifacts",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["pipeline", "health-check", "monitoring"],
) as dag:
    check_pipeline_services_task = PythonOperator(
        task_id="check_pipeline_services",
        python_callable=check_pipeline_services,
    )