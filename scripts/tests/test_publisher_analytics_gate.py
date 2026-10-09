import copy
import unittest
from pathlib import Path
from types import SimpleNamespace

from airflow.configuration import conf
from airflow.exceptions import AirflowFailException
from airflow.models import DagBag


VALID_RESULT = {
    "status": "completed",
    "publish": {
        "status": "reused",
        "event_date": "2026-10-05",
    },
    "registration": {
        "status": "reused",
        "database": "realtime_pipeline",
        "table": "clean_stock_prices_v2",
        "event_date": "2026-10-05",
    },
}


class AnalyticsGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dag_path = (
            Path(conf.get("core", "dags_folder"))
            / "publish_clean_stock_prices_v2.py"
        )

        bag = DagBag(
            dag_folder=str(dag_path),
            include_examples=False,
        )

        if bag.import_errors:
            raise RuntimeError(
                f"DAG import errors: {bag.import_errors}"
            )

        dag = bag.dags.get("publish_clean_stock_prices_v2")

        if dag is None:
            raise RuntimeError("Publisher DAG was not found.")

        cls.gate = staticmethod(
            dag.get_task("should_build_analytics").python_callable
        )

    def evaluate(self, result, apply=True):
        def pull(task_ids, key):
            if (
                task_ids != "publish_and_register"
                or key != "return_value"
            ):
                raise AssertionError("Unexpected XCom lookup.")
            return result

        return self.gate(
            dag_run=SimpleNamespace(conf={"apply": apply}),
            ti=SimpleNamespace(xcom_pull=pull),
        )

    def test_dry_run_skips_analytics(self):
        self.assertIs(
            self.evaluate({"status": "dry_run"}, apply=False),
            False,
        )

    def test_verified_applied_status_combinations_allow_analytics(self):
        for publish_status in ("uploaded", "reused"):
            for registration_status in ("created", "reused"):
                with self.subTest(
                    publish=publish_status,
                    registration=registration_status,
                ):
                    result = copy.deepcopy(VALID_RESULT)
                    result["publish"]["status"] = publish_status
                    result["registration"]["status"] = registration_status
                    self.assertIs(self.evaluate(result), True)

    def test_nonboolean_apply_is_rejected(self):
        with self.assertRaises(AirflowFailException):
            self.evaluate(VALID_RESULT, apply="true")

    def test_missing_publisher_result_is_rejected(self):
        with self.assertRaises(AirflowFailException):
            self.evaluate(None)

    def test_unexpected_dry_run_result_is_rejected(self):
        with self.assertRaises(AirflowFailException):
            self.evaluate(VALID_RESULT, apply=False)

    def test_incomplete_applied_result_is_rejected(self):
        for change in (
            "overall_status",
            "publish_details",
            "registration_details",
        ):
            with self.subTest(change=change):
                result = copy.deepcopy(VALID_RESULT)

                if change == "overall_status":
                    result["status"] = "dry_run"
                elif change == "publish_details":
                    result.pop("publish")
                else:
                    result.pop("registration")

                with self.assertRaises(AirflowFailException):
                    self.evaluate(result)

    def test_unverified_stage_status_is_rejected(self):
        for stage in ("publish", "registration"):
            with self.subTest(stage=stage):
                result = copy.deepcopy(VALID_RESULT)
                result[stage]["status"] = "dry_run"

                with self.assertRaises(AirflowFailException):
                    self.evaluate(result)

    def test_wrong_registration_target_is_rejected(self):
        for field, value in (
            ("database", "unexpected_database"),
            ("table", "unexpected_table"),
        ):
            with self.subTest(field=field):
                result = copy.deepcopy(VALID_RESULT)
                result["registration"][field] = value

                with self.assertRaises(AirflowFailException):
                    self.evaluate(result)

    def test_missing_or_mismatched_dates_are_rejected(self):
        for change in (
            "missing_publish_date",
            "mismatched_registration_date",
        ):
            with self.subTest(change=change):
                result = copy.deepcopy(VALID_RESULT)

                if change == "missing_publish_date":
                    result["publish"].pop("event_date")
                else:
                    result["registration"]["event_date"] = "2026-10-06"

                with self.assertRaises(AirflowFailException):
                    self.evaluate(result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
