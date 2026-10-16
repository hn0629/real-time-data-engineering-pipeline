import csv
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

BASE = Path(__file__).resolve().parent / "local_exports"
ENGINES = ("athena", "snowflake", "databricks")
KEYS = ("event_date", "symbol", "source")
COLUMNS = KEYS + (
    "event_count",
    "average_price",
    "minimum_price",
    "maximum_price",
    "latest_event_time",
)

# Explicit policy for this exported-result comparison.
AVERAGE_TOLERANCE = Decimal("0.0000000001")
failures = []


def fail(message):
    failures.append(message)
    print(f"FAIL: {message}")


def load(engine):
    path = BASE / f"{engine}.csv"
    rows = {}

    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        headers = [name.strip().lower() for name in (reader.fieldnames or [])]

        if len(headers) != len(set(headers)):
            raise ValueError(f"{engine}: duplicate column names")

        if set(headers) != set(COLUMNS):
            raise ValueError(f"{engine}: unexpected columns: {headers}")

        reader.fieldnames = headers

        for line, raw in enumerate(reader, start=2):
            if None in raw or any(value is None for value in raw.values()):
                raise ValueError(f"{engine}: malformed CSV row at line {line}")

            row = {name: value.strip() for name, value in raw.items()}
            if any(not value for value in row.values()):
                raise ValueError(f"{engine}: empty field at line {line}")

            key = tuple(row[name] for name in KEYS)
            if key in rows:
                raise ValueError(f"{engine}: duplicate key {key}")

            rows[key] = row

    if not rows:
        raise ValueError(f"{engine}: empty result")

    return rows


def wall_clock(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=None)


def compare(reference, candidate, engine):
    reference_keys = set(reference)
    candidate_keys = set(candidate)

    for key in sorted(reference_keys - candidate_keys):
        fail(f"{engine}: missing key {key}")

    for key in sorted(candidate_keys - reference_keys):
        fail(f"{engine}: extra key {key}")

    largest_average_difference = Decimal("0")

    for key in sorted(reference_keys & candidate_keys):
        left = reference[key]
        right = candidate[key]

        for column in ("event_count", "minimum_price", "maximum_price"):
            if Decimal(left[column]) != Decimal(right[column]):
                fail(f"{engine}: {key}: {column} differs")

        difference = abs(
            Decimal(left["average_price"]) - Decimal(right["average_price"])
        )
        largest_average_difference = max(largest_average_difference, difference)

        if difference > AVERAGE_TOLERANCE:
            fail(f"{engine}: {key}: average_price difference {difference}")

        if wall_clock(left["latest_event_time"]) != wall_clock(
            right["latest_event_time"]
        ):
            fail(f"{engine}: {key}: latest_event_time wall-clock differs")

    print(
        f"{engine} vs athena: "
        f"maximum average-price difference = {largest_average_difference}"
    )


def main():
    datasets = {engine: load(engine) for engine in ENGINES}

    for engine, rows in datasets.items():
        total = sum(Decimal(row["event_count"]) for row in rows.values())
        print(f"{engine}: {len(rows)} rows; total event_count = {total}")

    for engine in ("snowflake", "databricks"):
        compare(datasets["athena"], datasets[engine], engine)

    print(f"\nAverage-price absolute tolerance: {AVERAGE_TOLERANCE}")
    print("Timestamp check: wall-clock values only; timezone parity NOT verified.")
    print("Source snapshot identity and full-result completeness NOT verified.")

    if failures:
        print(f"\nRESULT: FAIL ({len(failures)} differences)")
        return 1

    print("\nRESULT: PASS for supplied exports under the stated comparison policy")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, ArithmeticError, csv.Error) as error:
        print(f"ERROR: {error}")
        sys.exit(2)