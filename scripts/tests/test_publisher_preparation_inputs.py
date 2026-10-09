import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from airflow.configuration import conf
from airflow.exceptions import AirflowFailException
from airflow.models import DagBag


class PublisherPreparationInputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = (
            Path(conf.get("core", "dags_folder"))
            / "publish_clean_stock_prices_v2.py"
        )
        bag = DagBag(dag_folder=str(path), include_examples=False)

        if bag.import_errors:
            raise RuntimeError(f"DAG import errors: {bag.import_errors}")

        dag = bag.dags.get("publish_clean_stock_prices_v2")
        if dag is None:
            raise RuntimeError("Publisher DAG was not found.")

        cls.select_artifact = staticmethod(
            dag.get_task("prepare_or_select_artifact").python_callable
        )

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)

        self.root = Path(temporary.name)
        self.clean_root = self.root / "clean"
        self.artifact_root = self.root / "prepared"
        self.artifact = self.artifact_root / "event_date=2026-10-05"
        self.artifact.mkdir(parents=True)

        clock = Mock(wraps=datetime)
        clock.now.return_value = datetime(
            2026, 10, 9, 12, tzinfo=timezone.utc
        )

        globals_patch = patch.dict(
            self.select_artifact.__globals__,
            {
                "CLEAN_ROOT": self.clean_root,
                "ARTIFACT_ROOT": self.artifact_root,
                "datetime": clock,
            },
        )
        globals_patch.start()
        self.addCleanup(globals_patch.stop)

        preparation_patch = patch(
            "src.clean_publish.prepare_clean_artifact.prepare"
        )
        self.prepare = preparation_patch.start()
        self.addCleanup(preparation_patch.stop)

        self.prepare.return_value = {
            "status": "reused",
            "directory": str(self.artifact),
            "event_date": "2026-10-05",
            "row_count": 2,
            "source_fingerprint": "a" * 64,
        }

    def evaluate(self, configuration):
        return self.select_artifact(
            dag_run=SimpleNamespace(conf=configuration)
        )

    def preparation_conf(self, **changes):
        result = {
            "event_date": "2026-10-05",
            "source_partition_stable": True,
            "apply": False,
        }
        result.update(changes)
        return result

    def test_exactly_one_input_mode_is_required(self):
        for configuration in (
            {},
            {
                "artifact_dir": str(self.artifact),
                "event_date": "2026-10-05",
            },
        ):
            with self.subTest(configuration=configuration):
                with self.assertRaises(AirflowFailException):
                    self.evaluate(configuration)

        self.prepare.assert_not_called()

    def test_apply_requires_boolean(self):
        for value in ("true", "false", 0, 1, None):
            with self.subTest(value=value):
                with self.assertRaises(AirflowFailException):
                    self.evaluate(self.preparation_conf(apply=value))

        self.prepare.assert_not_called()

    def test_invalid_artifact_paths_are_rejected(self):
        outside = self.root / "outside"
        outside.mkdir()

        for value in (
            "",
            " ",
            None,
            str(outside),
            str(self.artifact_root),
            str(self.artifact_root / "missing"),
        ):
            with self.subTest(value=value):
                with self.assertRaises(AirflowFailException):
                    self.evaluate({"artifact_dir": value})

        self.prepare.assert_not_called()

    def test_preparation_requires_explicit_stability_acknowledgement(self):
        configurations = [{"event_date": "2026-10-05"}]
        configurations.extend(
            self.preparation_conf(source_partition_stable=value)
            for value in (False, None, "true", 1)
        )

        for configuration in configurations:
            with self.subTest(configuration=configuration):
                with self.assertRaises(AirflowFailException):
                    self.evaluate(configuration)

        self.prepare.assert_not_called()

    def test_invalid_nonhistorical_or_noncanonical_dates_are_rejected(self):
        for value in (
            None,
            20261005,
            "",
            "not-a-date",
            "2026-02-30",
            "20261005",
            "2026-10-09",
            "2026-10-10",
        ):
            with self.subTest(value=value):
                with self.assertRaises(AirflowFailException):
                    self.evaluate(self.preparation_conf(event_date=value))

        self.prepare.assert_not_called()

    def test_existing_artifact_mode_does_not_prepare(self):
        result = self.evaluate({"artifact_dir": str(self.artifact)})

        self.assertEqual(
            result,
            {
                "status": "selected",
                "directory": str(self.artifact.resolve()),
            },
        )
        self.prepare.assert_not_called()

    def test_historical_mode_accepts_verified_preparation_statuses(self):
        for status in ("prepared", "reused"):
            with self.subTest(status=status):
                self.prepare.reset_mock()
                self.prepare.return_value["status"] = status

                result = self.evaluate(self.preparation_conf())

                self.prepare.assert_called_once_with(
                    source_root=str(self.clean_root),
                    event_date="2026-10-05",
                    output_root=str(self.artifact_root),
                )
                self.assertEqual(result["status"], status)
                self.assertEqual(result["event_date"], "2026-10-05")
                self.assertEqual(result["row_count"], 2)
                self.assertEqual(
                    result["directory"],
                    str(self.artifact.resolve()),
                )

    def test_unverified_preparation_status_is_rejected(self):
        for status in (None, "dry_run", "failed"):
            with self.subTest(status=status):
                self.prepare.return_value["status"] = status

                with self.assertRaises(AirflowFailException):
                    self.evaluate(self.preparation_conf())

    def test_preparation_result_directory_is_validated(self):
        for value in (
            None,
            str(self.root),
            str(self.artifact_root / "missing"),
        ):
            with self.subTest(value=value):
                self.prepare.return_value["directory"] = value

                with self.assertRaises(AirflowFailException):
                    self.evaluate(self.preparation_conf())

    def test_preparation_failure_propagates(self):
        self.prepare.side_effect = ValueError("Source changed")

        with self.assertRaisesRegex(ValueError, "Source changed"):
            self.evaluate(self.preparation_conf())


if __name__ == "__main__":
    unittest.main(verbosity=2)
