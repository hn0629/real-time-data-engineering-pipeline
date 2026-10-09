import unittest
from pathlib import Path

from airflow.configuration import conf
from airflow.models import DagBag


class V2OrchestrationConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dags = {}
        dags_folder = Path(conf.get("core", "dags_folder"))

        for dag_id in (
            "publish_clean_stock_prices_v2",
            "refresh_dbt_stock_analytics_v2",
        ):
            bag = DagBag(
                dag_folder=str(dags_folder / f"{dag_id}.py"),
                include_examples=False,
            )

            if bag.import_errors:
                raise RuntimeError(
                    f"DAG import errors: {bag.import_errors}"
                )

            dag = bag.dags.get(dag_id)

            if dag is None:
                raise RuntimeError(f"DAG not found: {dag_id}")

            cls.dags[dag_id] = dag

    def test_publisher_is_manual_only(self):
        dag = self.dags["publish_clean_stock_prices_v2"]
        self.assertIsNone(dag.schedule_interval)

    def test_refresh_is_manual_only(self):
        dag = self.dags["refresh_dbt_stock_analytics_v2"]
        self.assertIsNone(dag.schedule_interval)

    def test_both_dbt_tasks_use_shared_single_slot_assignment(self):
        for dag_id, dag in self.dags.items():
            with self.subTest(dag_id=dag_id):
                task = dag.get_task("dbt_build_v2_candidate")
                self.assertEqual(
                    task.pool,
                    "dbt_stock_v2_candidate",
                )
                self.assertEqual(task.pool_slots, 1)

    def test_publisher_dependency_chain_is_preserved(self):
        dag = self.dags["publish_clean_stock_prices_v2"]

        expected = {
            "prepare_or_select_artifact": {"publish_and_register"},
            "publish_and_register": {"should_build_analytics"},
            "should_build_analytics": {"dbt_build_v2_candidate"},
            "dbt_build_v2_candidate": set(),
        }

        self.assertEqual(set(dag.task_ids), set(expected))

        for task_id, downstream in expected.items():
            with self.subTest(task_id=task_id):
                self.assertEqual(
                    set(dag.get_task(task_id).downstream_task_ids),
                    downstream,
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
