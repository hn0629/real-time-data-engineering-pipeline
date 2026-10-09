import hashlib
import io
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.clean_publish.publish_clean_artifact import publish


class UploadTests(unittest.TestCase):
    def setUp(self):
        self.data = b"test-artifact-bytes"
        self.manifest = {
            "event_date": "2026-10-05",
            "row_count": 2,
            "output_sha256": hashlib.sha256(self.data).hexdigest(),
            "source_fingerprint": "a" * 64,
        }
        self.client = MagicMock()
        patcher = patch(
            "src.clean_publish.publish_clean_artifact.load_artifact",
            return_value=(self.manifest, self.data),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def response(self, data=None, fingerprint=None, row_count="2"):
        return {
            "Body": io.BytesIO(self.data if data is None else data),
            "Metadata": {
                "source-fingerprint": (
                    self.manifest["source_fingerprint"]
                    if fingerprint is None else fingerprint
                ),
                "row-count": row_count,
            },
        }

    def error(self, code, operation="GetObject"):
        return ClientError(
            {"Error": {"Code": code, "Message": "Simulated error"}},
            operation,
        )

    def apply(self):
        return publish(
            "artifact", "test-bucket", "clean-v2/stock_prices",
            apply=True, client=self.client,
        )

    def test_dry_run_makes_no_client_calls(self):
        result = publish(
            "artifact", "test-bucket", "clean-v2/stock_prices",
            client=self.client,
        )
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(self.client.mock_calls, [])

    def test_new_upload_is_verified_and_conditional(self):
        self.client.get_object.side_effect = [
            self.error("NoSuchKey"),
            self.response(),
        ]
        result = self.apply()
        self.assertEqual(result["status"], "uploaded")
        self.client.put_object.assert_called_once()
        arguments = self.client.put_object.call_args.kwargs
        self.assertEqual(arguments["Body"], self.data)
        self.assertEqual(
            arguments["Key"],
            "clean-v2/stock_prices/event_date=2026-10-05/part-00000.parquet",
        )
        registration = self.client.meta.events.register.call_args
        self.assertEqual(registration.args[0], "before-call.s3.PutObject")
        params = {"headers": {}}
        registration.args[1](params)
        self.assertEqual(params["headers"]["If-None-Match"], "*")
        self.client.meta.events.unregister.assert_called_once()

    def test_matching_object_is_reused_without_upload(self):
        self.client.get_object.return_value = self.response()
        self.assertEqual(self.apply()["status"], "reused")
        self.client.put_object.assert_not_called()

    def test_different_remote_bytes_are_rejected(self):
        self.client.get_object.return_value = self.response(data=b"different")
        with self.assertRaisesRegex(ValueError, "different bytes"):
            self.apply()
        self.client.put_object.assert_not_called()

    def test_different_source_fingerprint_is_rejected(self):
        self.client.get_object.return_value = self.response(
            fingerprint="b" * 64
        )
        with self.assertRaisesRegex(ValueError, "different source fingerprint"):
            self.apply()
        self.client.put_object.assert_not_called()

    def test_different_row_count_is_rejected(self):
        self.client.get_object.return_value = self.response(row_count="3")
        with self.assertRaisesRegex(ValueError, "different row count"):
            self.apply()
        self.client.put_object.assert_not_called()

    def test_access_denied_does_not_trigger_upload(self):
        self.client.get_object.side_effect = self.error("AccessDenied")
        with self.assertRaises(ClientError):
            self.apply()
        self.client.put_object.assert_not_called()

    def test_matching_concurrent_upload_is_reused(self):
        self.client.get_object.side_effect = [
            self.error("NoSuchKey"),
            self.response(),
        ]
        self.client.put_object.side_effect = self.error(
            "PreconditionFailed", "PutObject"
        )
        self.assertEqual(self.apply()["status"], "reused")
        self.client.meta.events.unregister.assert_called_once()

    def test_different_concurrent_upload_is_rejected(self):
        self.client.get_object.side_effect = [
            self.error("NoSuchKey"),
            self.response(data=b"different"),
        ]
        self.client.put_object.side_effect = self.error(
            "PreconditionFailed", "PutObject"
        )
        with self.assertRaisesRegex(ValueError, "different bytes"):
            self.apply()
        self.client.meta.events.unregister.assert_called_once()

    def test_post_upload_corruption_is_rejected(self):
        self.client.get_object.side_effect = [
            self.error("NoSuchKey"),
            self.response(data=b"corrupt"),
        ]
        with self.assertRaisesRegex(ValueError, "different bytes"):
            self.apply()

    def test_apply_requires_client(self):
        with self.assertRaisesRegex(ValueError, "S3 client"):
            publish(
                "artifact", "test-bucket", "clean-v2/stock_prices",
                apply=True,
            )


if __name__ == "__main__":
    unittest.main()
