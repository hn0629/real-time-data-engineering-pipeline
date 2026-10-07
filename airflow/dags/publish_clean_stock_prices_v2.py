import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator


DAG_ID = "publish_clean_stock_prices_v2"
ARTIFACT_ROOT = Path("/opt/spark-data/publish-tests/prepared-clean")


def publish_prepared_artifact(**context):
    # Import shared code at task execution, not during DAG parsing.
    from src.clean_publish.publish_and_register import run

    dag_run = context.get("dag_run")
    conf = dag_run.conf or {} if dag_run else {}

    artifact_dir = conf.get("artifact_dir")
    apply = conf.get("apply", False)

    if not isinstance(artifact_dir, str) or not artifact_dir.strip():
        raise AirflowFailException(
            "Provide artifact_dir in the DAG run configuration."
        )
    if not isinstance(apply, bool):
        raise AirflowFailException(
            "apply must be a JSON boolean: true or false."
        )

    root = ARTIFACT_ROOT.resolve()
    artifact = Path(artifact_dir).resolve()

    if root not in artifact.parents:
        raise AirflowFailException(
            f"artifact_dir must be a directory beneath {root}"
        )
    if not artifact.is_dir():
        raise AirflowFailException(
            f"Artifact directory does not exist: {artifact}"
        )

    result = run(
        artifact_dir=str(artifact),
        bucket="hoang-real-time-data-pipeline-2026",
        prefix="real-time-pipeline/clean-v2/stock_prices",
        database="realtime_pipeline",
        table="clean_stock_prices_v2",
        region="us-east-1",
        apply=apply,
    )

    logging.getLogger(__name__).info(
        "Publish-and-register result:\n%s",
        json.dumps(result, indent=2),
    )
    return result


with DAG(
    dag_id=DAG_ID,
    description="Manually publish and register a prepared Clean artifact.",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["clean-v2", "publish", "manual"],
) as dag:
    publish_and_register = PythonOperator(
        task_id="publish_and_register",
        python_callable=publish_prepared_artifact,
    )