import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.clean_publish.publish_and_register import run


MODULE = "src.clean_publish.publish_and_register"


class CombinedRunTests(unittest.TestCase):
    def setUp(self):
        self.arguments = {
            "artifact_dir": "/prepared/event_date=2026-10-05",
            "bucket": "test-bucket",
            "prefix": "/clean-v2/stock_prices/",
            "database": "test_database",
            "table": "test_table",
            "region": "us-east-1",
        }

        self.publisher = self.start_patch(f"{MODULE}.publish")
        self.registrar = self.start_patch(f"{MODULE}.register_partition")
        self.clients = self.start_patch("boto3.client")

        self.s3 = MagicMock()
        self.glue = MagicMock()
        self.clients.side_effect = lambda service, **kwargs: {
            "s3": self.s3,
            "glue": self.glue,
        }[service]

        self.publisher.return_value = {
            "status": "reused",
            "event_date": "2026-10-05",
        }
        self.registrar.return_value = {"status": "reused"}

    def start_patch(self, target):
        patcher = patch(target)
        mock = patcher.start()
        self.addCleanup(patcher.stop)
        return mock

    def test_dry_run_skips_registration_and_aws_clients(self):
        self.publisher.return_value["status"] = "dry_run"

        result = run(**self.arguments)

        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["registration"]["status"], "skipped")
        self.registrar.assert_not_called()
        self.clients.assert_not_called()
        self.assertFalse(self.publisher.call_args.kwargs["apply"])
        self.assertIsNone(self.publisher.call_args.kwargs["client"])

    def test_verified_publish_statuses_allow_registration(self):
        for status in ("uploaded", "reused"):
            with self.subTest(status=status):
                self.publisher.reset_mock()
                self.registrar.reset_mock()
                self.publisher.return_value["status"] = status

                result = run(**self.arguments, apply=True)

                self.assertEqual(result["status"], "completed")
                self.registrar.assert_called_once_with(
                    database="test_database",
                    table="test_table",
                    event_date="2026-10-05",
                    dataset_uri="s3://test-bucket/clean-v2/stock_prices/",
                    apply=True,
                    client=self.glue,
                )
                self.assertIs(
                    self.publisher.call_args.kwargs["client"], self.s3
                )

    def test_publish_failure_prevents_registration(self):
        self.publisher.side_effect = ValueError("Checksum mismatch")

        with self.assertRaisesRegex(ValueError, "Checksum mismatch"):
            run(**self.arguments, apply=True)

        self.registrar.assert_not_called()
        self.assertEqual(
            [call.args[0] for call in self.clients.call_args_list],
            ["s3"],
        )

    def test_unverified_publish_status_prevents_registration(self):
        self.publisher.return_value["status"] = "dry_run"

        with self.assertRaisesRegex(RuntimeError, "verified success"):
            run(**self.arguments, apply=True)

        self.registrar.assert_not_called()

    def test_registration_failure_is_not_reported_as_completed(self):
        self.registrar.side_effect = RuntimeError("Registration failed")

        with self.assertRaisesRegex(RuntimeError, "Registration failed"):
            run(**self.arguments, apply=True)

        self.publisher.assert_called_once()
        self.registrar.assert_called_once()

    def test_retry_repeats_publish_verification_before_registration(self):
        self.registrar.side_effect = [
            RuntimeError("Temporary registration failure"),
            {"status": "created"},
        ]

        with self.assertRaisesRegex(RuntimeError, "Temporary"):
            run(**self.arguments, apply=True)

        result = run(**self.arguments, apply=True)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["registration"]["status"], "created")
        self.assertEqual(self.publisher.call_count, 2)
        self.assertEqual(self.registrar.call_count, 2)


if __name__ == "__main__":
    unittest.main()