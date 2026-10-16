import csv
import importlib.util
import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().with_name("compare_exports.py")
SPEC = importlib.util.spec_from_file_location("comparison_under_test", SCRIPT)
comparison = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(comparison)


class CompareExportsTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

        self.base_patch = patch.object(comparison, "BASE", Path(self.temp.name))
        self.base_patch.start()
        self.addCleanup(self.base_patch.stop)

        comparison.failures.clear()
        self.addCleanup(comparison.failures.clear)

        self.output = io.StringIO()
        self.output_redirect = redirect_stdout(self.output)
        self.output_redirect.__enter__()
        self.addCleanup(self.output_redirect.__exit__, None, None, None)

        self.row = {
            "event_date": "2026-10-05",
            "symbol": "TEST",
            "source": "synthetic-test",
            "event_count": "10",
            "average_price": "100",
            "minimum_price": "90",
            "maximum_price": "110",
            "latest_event_time": "2026-10-05 16:00:00.123",
        }

        for engine in comparison.ENGINES:
            self.write(engine, [self.row])

    def write(self, engine, rows, uppercase=False):
        headers = list(comparison.COLUMNS)
        if uppercase:
            headers = [name.upper() for name in headers]

        path = comparison.BASE / f"{engine}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)
            for row in rows:
                writer.writerow([row[name] for name in comparison.COLUMNS])

    def changed_row(self, **changes):
        return {**self.row, **changes}

    def test_matching_exports_pass(self):
        self.assertEqual(comparison.main(), 0)

    def test_uppercase_headers_pass(self):
        self.write("snowflake", [self.row], uppercase=True)
        self.assertEqual(comparison.main(), 0)

    def test_numeric_formatting_passes(self):
        row = self.changed_row(minimum_price="90.0000", maximum_price="110.0000")
        self.write("snowflake", [row])
        self.assertEqual(comparison.main(), 0)

    def test_average_within_tolerance_passes(self):
        row = self.changed_row(average_price="100.00000000005")
        self.write("snowflake", [row])
        self.assertEqual(comparison.main(), 0)

    def test_average_outside_tolerance_fails(self):
        row = self.changed_row(average_price="100.000000001")
        self.write("snowflake", [row])
        self.assertEqual(comparison.main(), 1)
        self.assertIn("average_price difference", self.output.getvalue())

    def test_event_count_difference_fails(self):
        self.write("snowflake", [self.changed_row(event_count="11")])
        self.assertEqual(comparison.main(), 1)
        self.assertIn("event_count differs", self.output.getvalue())

    def test_missing_key_fails(self):
        extra = self.changed_row(symbol="SECOND")
        self.write("athena", [self.row, extra])
        self.write("databricks", [self.row, extra])
        self.assertEqual(comparison.main(), 1)
        self.assertIn("missing key", self.output.getvalue())

    def test_extra_key_fails(self):
        extra = self.changed_row(symbol="EXTRA")
        self.write("snowflake", [self.row, extra])
        self.assertEqual(comparison.main(), 1)
        self.assertIn("extra key", self.output.getvalue())

    def test_duplicate_key_raises(self):
        self.write("snowflake", [self.row, self.row])
        with self.assertRaisesRegex(ValueError, "duplicate key"):
            comparison.main()

    def test_empty_export_raises(self):
        self.write("snowflake", [])
        with self.assertRaisesRegex(ValueError, "empty result"):
            comparison.main()

    def test_timestamp_format_difference_passes(self):
        row = self.changed_row(latest_event_time="2026-10-05T16:00:00.123Z")
        self.write("databricks", [row])
        self.assertEqual(comparison.main(), 0)

    def test_timestamp_clock_difference_fails(self):
        row = self.changed_row(latest_event_time="2026-10-05T16:00:01.123Z")
        self.write("databricks", [row])
        self.assertEqual(comparison.main(), 1)
        self.assertIn("wall-clock differs", self.output.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)