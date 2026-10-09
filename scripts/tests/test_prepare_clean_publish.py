import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from prepare_clean_publish import prepare


SCHEMA = pa.schema([
    ("symbol", pa.string()),
    ("price", pa.float64()),
    ("source", pa.string()),
    ("event_time", pa.string()),
    ("kafka_topic", pa.string()),
    ("kafka_partition", pa.int32()),
    ("kafka_offset", pa.int64()),
    ("kafka_timestamp", pa.timestamp("us")),
    ("ingested_at", pa.timestamp("us")),
    ("batch_id", pa.int32()),
    ("raw_payload", pa.string()),
])


class PreparationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "clean"
        self.output = self.root / "prepared"
        self.partition = self.source / "event_date=2026-10-05"
        self.partition.mkdir(parents=True)

    def row(self, offset=1, **changes):
        result = {
            "symbol": "TSLA",
            "price": 158.27,
            "source": "simulated-producer",
            "event_time": "2026-10-05T15:38:31.466662+00:00",
            "kafka_topic": "pipeline-events",
            "kafka_partition": 0,
            "kafka_offset": offset,
            "kafka_timestamp": datetime(2026, 10, 5, 15, 38, 31),
            "ingested_at": datetime(2026, 10, 5, 15, 38, 42),
            "batch_id": 1,
            "raw_payload": "{}",
        }
        result.update(changes)
        return result

    def write(self, rows, name="input.parquet", schema=SCHEMA):
        path = self.partition / name
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
        return path

    def run_prepare(self):
        return prepare(self.source, "2026-10-05", self.output)

    def test_prepares_and_preserves_source(self):
        source_file = self.write([self.row()])
        original = source_file.read_bytes()
        result = self.run_prepare()
        self.assertEqual(result["status"], "prepared")
        self.assertEqual(result["row_count"], 1)
        self.assertEqual(source_file.read_bytes(), original)
        self.assertTrue(
            (Path(result["directory"]) / "manifest.json").is_file()
        )

    def test_compacts_multiple_files(self):
        self.write([self.row(1)], "first.parquet")
        self.write([self.row(2)], "second.parquet")
        result = self.run_prepare()
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(len(result["source_files"]), 2)
        files = list(Path(result["directory"]).glob("*.parquet"))
        self.assertEqual(len(files), 1)

    def test_unchanged_retry_reuses_artifact(self):
        self.write([self.row()])
        first = self.run_prepare()
        second = self.run_prepare()
        self.assertEqual(second["status"], "reused")
        self.assertEqual(first["output_sha256"], second["output_sha256"])

    def test_changed_source_is_rejected(self):
        self.write([self.row()])
        self.run_prepare()
        self.write([self.row(2)], "new.parquet")
        with self.assertRaisesRegex(ValueError, "different source input"):
            self.run_prepare()

    def test_corrupt_output_is_rejected(self):
        self.write([self.row()])
        result = self.run_prepare()
        output = Path(result["directory"]) / result["output_file"]
        output.write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.run_prepare()

    def test_invalid_values_are_rejected(self):
        cases = [
            {"price": 0.0},
            {"price": float("nan")},
            {"price": float("inf")},
            {"symbol": " "},
            {"source": None},
            {"kafka_topic": ""},
            {"kafka_offset": None},
            {"ingested_at": None},
            {"event_time": "not-a-timestamp"},
            {"event_time": "2026-10-05T15:00:00"},
            {"event_time": "2026-10-06T15:00:00+00:00"},
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                self.write([self.row(**changes)])
                with self.assertRaises(ValueError):
                    self.run_prepare()
        self.assertFalse(self.output.exists())

    def test_duplicate_identity_across_files_is_rejected(self):
        self.write([self.row()], "first.parquet")
        self.write([self.row()], "second.parquet")
        with self.assertRaisesRegex(ValueError, "Duplicate Kafka identity"):
            self.run_prepare()

    def test_schema_mismatch_is_rejected(self):
        schema = pa.schema([
            field for field in SCHEMA if field.name != "raw_payload"
        ])
        self.write([self.row()], schema=schema)
        with self.assertRaisesRegex(ValueError, "Source columns"):
            self.run_prepare()

    def test_missing_partition_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "does not exist"):
            prepare(self.source, "2026-10-06", self.output)

    def test_output_inside_source_is_rejected(self):
        self.write([self.row()])
        with self.assertRaisesRegex(ValueError, "outside the source"):
            prepare(
                self.source, "2026-10-05", self.source / "prepared"
            )


if __name__ == "__main__":
    unittest.main()
