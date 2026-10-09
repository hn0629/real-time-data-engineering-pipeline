from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="refresh_dbt_stock_analytics_v2",
    description=(
        "Build and test current-format stock analytics "
        "using isolated v2 candidate views."
    ),
    start_date=datetime(2026, 10, 9, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["dbt", "athena", "analytics", "stocks", "v2"],
) as dag:

    dbt_build_v2 = BashOperator(
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
