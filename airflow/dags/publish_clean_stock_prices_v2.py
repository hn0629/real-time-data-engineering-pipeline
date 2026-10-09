import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator


DAG_ID = "publish_clean_stock_prices_v2"
CLEAN_ROOT = Path("/opt/spark-data/clean/stock_prices")
ARTIFACT_ROOT = Path("/opt/spark-data/publish-tests/prepared-clean")
LOGGER = logging.getLogger(__name__)


def get_run_conf(context):
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    if not isinstance(conf, dict):
        raise AirflowFailException("Run configuration must be a JSON object.")

    if not isinstance(conf.get("apply", False), bool):
        raise AirflowFailException(
            "apply must be a JSON boolean: true or false."
        )

    return conf


def validate_artifact_directory(value):
    if not isinstance(value, str) or not value.strip():
        raise AirflowFailException("artifact_dir must be a nonempty string.")

    root = ARTIFACT_ROOT.resolve()
    artifact = Path(value).resolve()

    if root not in artifact.parents:
        raise AirflowFailException(
            f"artifact_dir must be a directory beneath {root}"
        )

    if not artifact.is_dir():
        raise AirflowFailException(
            f"Artifact directory does not exist: {artifact}"
        )

    return artifact


def prepare_or_select_artifact(**context):
    conf = get_run_conf(context)

    has_artifact = "artifact_dir" in conf
    has_event_date = "event_date" in conf

    if has_artifact == has_event_date:
        raise AirflowFailException(
            "Provide exactly one of artifact_dir or event_date."
        )

    if has_artifact:
        artifact = validate_artifact_directory(conf["artifact_dir"])
        LOGGER.info("Using existing prepared artifact: %s", artifact)
        return {
            "status": "selected",
            "directory": str(artifact),
        }

    if conf.get("source_partition_stable") is not True:
        raise AirflowFailException(
            "Preparation requires source_partition_stable: true. "
            "This is an acknowledgement, not automatic partition sealing."
        )

    event_date = conf["event_date"]

    if not isinstance(event_date, str):
        raise AirflowFailException("event_date must be a YYYY-MM-DD string.")

    try:
        parsed_date = date.fromisoformat(event_date)
    except ValueError as exc:
        raise AirflowFailException("Invalid event_date.") from exc

    if parsed_date.isoformat() != event_date:
        raise AirflowFailException("event_date must use YYYY-MM-DD format.")

    if parsed_date >= datetime.now(timezone.utc).date():
        raise AirflowFailException(
            "Preparation mode requires a historical UTC event date. "
            "A historical date alone does not guarantee stability."
        )

    from src.clean_publish.prepare_clean_artifact import prepare

    result = prepare(
        source_root=str(CLEAN_ROOT),
        event_date=event_date,
        output_root=str(ARTIFACT_ROOT),
    )

    if result.get("status") not in ("prepared", "reused"):
        raise AirflowFailException(
            "Preparation did not return a verified success status."
        )

    artifact = validate_artifact_directory(result.get("directory"))

    summary = {
        "status": result["status"],
        "directory": str(artifact),
        "event_date": result["event_date"],
        "row_count": result["row_count"],
        "source_fingerprint": result["source_fingerprint"],
    }

    LOGGER.info("Preparation result:\n%s", json.dumps(summary, indent=2))
    return summary


def publish_prepared_artifact(**context):
    from src.clean_publish.publish_and_register import run

    conf = get_run_conf(context)

    selection = context["ti"].xcom_pull(
        task_ids="prepare_or_select_artifact",
        key="return_value",
    )

    if not isinstance(selection, dict):
        raise AirflowFailException(
            "Preparation task did not return an artifact selection."
        )

    artifact = validate_artifact_directory(selection.get("directory"))

    result = run(
        artifact_dir=str(artifact),
        bucket="hoang-real-time-data-pipeline-2026",
        prefix="real-time-pipeline/clean-v2/stock_prices",
        database="realtime_pipeline",
        table="clean_stock_prices_v2",
        region="us-east-1",
        apply=conf.get("apply", False),
    )

    LOGGER.info(
        "Publish-and-register result:\n%s",
        json.dumps(result, indent=2),
    )
    return result


def should_build_analytics(**context):
    conf = get_run_conf(context)

    result = context["ti"].xcom_pull(
        task_ids="publish_and_register",
        key="return_value",
    )

    if not isinstance(result, dict):
        raise AirflowFailException(
            "Publisher did not return a result dictionary."
        )

    if not conf.get("apply", False):
        if result.get("status") != "dry_run":
            raise AirflowFailException("Expected a dry-run result.")

        LOGGER.info("Dry run completed. Skipping analytics.")
        return False

    published = result.get("publish")
    registration = result.get("registration")

    if not isinstance(published, dict) or not isinstance(registration, dict):
        raise AirflowFailException(
            "Missing publication or registration details."
        )

    if result.get("status") != "completed":
        raise AirflowFailException("Applied publication did not complete.")

    if published.get("status") not in ("uploaded", "reused"):
        raise AirflowFailException("Publication was not verified.")

    if registration.get("status") not in ("created", "reused"):
        raise AirflowFailException("Registration was not verified.")

    if (
        registration.get("database") != "realtime_pipeline"
        or registration.get("table") != "clean_stock_prices_v2"
    ):
        raise AirflowFailException("Unexpected registration target.")

    event_date = published.get("event_date")

    if not event_date or registration.get("event_date") != event_date:
        raise AirflowFailException(
            "Publication and registration dates do not match."
        )

    LOGGER.info(
        "Verified applied publication for %s. Continuing to analytics.",
        event_date,
    )
    return True


with DAG(
    dag_id=DAG_ID,
    description=(
        "Manually select or prepare a stable Clean artifact, publish and "
        "register it, then build tested v2 candidate analytics."
    ),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["clean-v2", "prepare", "publish", "manual", "dbt", "analytics"],
) as dag:

    preparation_task = PythonOperator(
        task_id="prepare_or_select_artifact",
        python_callable=prepare_or_select_artifact,
    )

    publish_task = PythonOperator(
        task_id="publish_and_register",
        python_callable=publish_prepared_artifact,
    )

    analytics_gate = ShortCircuitOperator(
        task_id="should_build_analytics",
        python_callable=should_build_analytics,
    )

    dbt_task = BashOperator(
        task_id="dbt_build_v2_candidate",
        pool="dbt_stock_v2_candidate",
        pool_slots=1,
        bash_command=(
            "exec /opt/dbt-venv/bin/dbt build "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt/profiles "
            "--select +mart_latest_symbol_metrics "
            "--indirect-selection cautious "
            "--vars '{\"use_clean_stock_prices_v2\": true}'"
        ),
        env={
            "DBT_TARGET_PATH": (
                "/tmp/dbt-target/{{ dag.dag_id }}/"
                "{{ ts_nodash }}/{{ ti.try_number }}"
            ),
            "DBT_LOG_PATH": (
                "/opt/airflow/logs/dbt/{{ dag.dag_id }}/"
                "{{ ts_nodash }}/{{ ti.try_number }}"
            ),
        },
        append_env=True,
        skip_on_exit_code=None,
        execution_timeout=timedelta(minutes=10),
    )

    preparation_task >> publish_task >> analytics_gate >> dbt_task
