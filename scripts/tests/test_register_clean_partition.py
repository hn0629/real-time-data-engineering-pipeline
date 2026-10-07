import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.clean_publish.register_clean_partition import register_partition


class RegistrationTests(unittest.TestCase):
    def setUp(self):
        self.root = "s3://test-bucket/clean-v2/stock_prices/"
        self.location = self.root + "event_date=2026-10-05/"
        self.sd = {
            "Location": self.root,
            "Columns": [{"Name": "symbol", "Type": "string"}],
            "InputFormat": (
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
            ),
            "OutputFormat": (
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
            ),
            "SerdeInfo": {
                "SerializationLibrary": (
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                )
            },
        }
        self.table = {
            "StorageDescriptor": self.sd,
            "PartitionKeys": [{"Name": "event_date", "Type": "string"}],
        }
        self.partition_sd = copy.deepcopy(self.sd)
        self.partition_sd["Location"] = self.location
        self.partition = {
            "Values": ["2026-10-05"],
            "StorageDescriptor": self.partition_sd,
        }

        self.client = MagicMock()
        self.client.exceptions.EntityNotFoundException = ClientError
        self.client.get_table.return_value = {"Table": self.table}
        self.client.get_partition.return_value = {
            "Partition": self.partition
        }
        self.client.batch_create_partition.return_value = {"Errors": []}

    def missing(self):
        return ClientError(
            {
                "Error": {
                    "Code": "EntityNotFoundException",
                    "Message": "Partition missing",
                }
            },
            "GetPartition",
        )

    def run_registration(self, apply=False):
        return register_partition(
            database="test_database",
            table="test_table",
            event_date="2026-10-05",
            dataset_uri=self.root,
            apply=apply,
            client=self.client,
        )

    def test_dry_run_existing_partition_plans_reuse(self):
        result = self.run_registration()
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["planned_action"], "reuse")
        self.client.batch_create_partition.assert_not_called()

    def test_dry_run_missing_partition_plans_creation(self):
        self.client.get_partition.side_effect = self.missing()
        result = self.run_registration()
        self.assertEqual(result["planned_action"], "create")
        self.client.batch_create_partition.assert_not_called()

    def test_matching_partition_is_reused(self):
        result = self.run_registration(apply=True)
        self.assertEqual(result["status"], "reused")
        self.client.batch_create_partition.assert_not_called()

    def test_missing_partition_is_created_and_verified(self):
        self.client.get_partition.side_effect = [
            self.missing(),
            {"Partition": self.partition},
        ]
        result = self.run_registration(apply=True)

        self.assertEqual(result["status"], "created")
        self.client.batch_create_partition.assert_called_once_with(
            DatabaseName="test_database",
            TableName="test_table",
            PartitionInputList=[{
                "Values": ["2026-10-05"],
                "StorageDescriptor": self.partition_sd,
            }],
        )
        self.assertEqual(self.client.get_partition.call_count, 2)

    def test_existing_location_conflict_is_rejected(self):
        self.partition_sd["Location"] = "s3://wrong-bucket/wrong/"
        with self.assertRaisesRegex(ValueError, "Location"):
            self.run_registration(apply=True)
        self.client.batch_create_partition.assert_not_called()

    def test_existing_schema_conflict_is_rejected(self):
        self.partition_sd["Columns"] = [
            {"Name": "symbol", "Type": "bigint"}
        ]
        with self.assertRaisesRegex(ValueError, "Columns"):
            self.run_registration(apply=True)
        self.client.batch_create_partition.assert_not_called()

    def test_wrong_table_location_is_rejected(self):
        self.sd["Location"] = "s3://wrong-bucket/wrong/"
        with self.assertRaisesRegex(ValueError, "Table location"):
            self.run_registration(apply=True)
        self.client.get_partition.assert_not_called()
        self.client.batch_create_partition.assert_not_called()

    def test_wrong_partition_key_is_rejected(self):
        self.table["PartitionKeys"] = [{"Name": "day", "Type": "string"}]
        with self.assertRaisesRegex(ValueError, "partition key"):
            self.run_registration(apply=True)
        self.client.batch_create_partition.assert_not_called()

    def test_batch_error_is_propagated(self):
        self.client.get_partition.side_effect = self.missing()
        self.client.batch_create_partition.return_value = {
            "Errors": [{
                "ErrorDetail": {
                    "ErrorCode": "InternalServiceException",
                    "ErrorMessage": "Simulated failure",
                }
            }]
        }
        with self.assertRaisesRegex(RuntimeError, "InternalServiceException"):
            self.run_registration(apply=True)

    def test_concurrent_matching_partition_is_reused(self):
        self.client.get_partition.side_effect = [
            self.missing(),
            {"Partition": self.partition},
        ]
        self.client.batch_create_partition.return_value = {
            "Errors": [{
                "ErrorDetail": {"ErrorCode": "AlreadyExistsException"}
            }]
        }
        result = self.run_registration(apply=True)
        self.assertEqual(result["status"], "reused")

    def test_concurrent_conflicting_partition_is_rejected(self):
        conflicting = copy.deepcopy(self.partition)
        conflicting["StorageDescriptor"]["Location"] = "s3://wrong/path/"
        self.client.get_partition.side_effect = [
            self.missing(),
            {"Partition": conflicting},
        ]
        self.client.batch_create_partition.return_value = {
            "Errors": [{
                "ErrorDetail": {"ErrorCode": "AlreadyExistsException"}
            }]
        }
        with self.assertRaisesRegex(ValueError, "Location"):
            self.run_registration(apply=True)

    def test_created_partition_is_verified(self):
        conflicting = copy.deepcopy(self.partition)
        conflicting["StorageDescriptor"]["Columns"] = []
        self.client.get_partition.side_effect = [
            self.missing(),
            {"Partition": conflicting},
        ]
        with self.assertRaisesRegex(ValueError, "Columns"):
            self.run_registration(apply=True)


if __name__ == "__main__":
    unittest.main()