import argparse
import hashlib
import json
import math
import shutil
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


FORMAT_VERSION = 1
EXPECTED_TYPES = {
    "symbol": "string",
    "price": "double",
    "source": "string",
    "event_time": "string",
    "kafka_topic": "string",
    "kafka_partition": "int32",
    "kafka_offset": "int64",
    "batch_id": "int32",
    "raw_payload": "string",
}
TIMESTAMP_COLUMNS = {"kafka_timestamp", "ingested_at"}


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def inventory(partition):
    files = sorted(partition.rglob("*.parquet"))
    if not files:
        raise ValueError("No source Parquet files found")
    return [
        {
            "path": path.relative_to(partition).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": file_hash(path),
        }
        for path in files
    ]


def check_schema(schema):
    expected = set(EXPECTED_TYPES) | TIMESTAMP_COLUMNS
    if set(schema.names) != expected or len(schema.names) != len(expected):
        raise ValueError("Source columns do not match the current Clean contract")
    for name, dtype in EXPECTED_TYPES.items():
        if str(schema.field(name).type) != dtype:
            raise ValueError(f"Unexpected type for {name}")
    for name in TIMESTAMP_COLUMNS:
        dtype = schema.field(name).type
        if not pa.types.is_timestamp(dtype) or dtype.tz is not None:
            raise ValueError(f"Expected timezone-naive timestamp for {name}")


def validate_rows(table, event_date):
    identities = set()
    for batch in table.to_batches(max_chunksize=4096):
        for row in batch.to_pylist():
            for name in ("symbol", "source", "kafka_topic"):
                value = row[name]
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(f"Missing or blank {name}")

            price = row["price"]
            if price is None or not math.isfinite(price) or price <= 0:
                raise ValueError("Invalid price")

            try:
                value = row["event_time"]
                timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ValueError("Invalid event_time") from exc

            if timestamp.tzinfo is None:
                raise ValueError("event_time must include a timezone")
            if timestamp.astimezone(timezone.utc).date().isoformat() != event_date:
                raise ValueError("UTC event date does not match partition")

            for name in TIMESTAMP_COLUMNS:
                if row[name] is None:
                    raise ValueError(f"Missing {name}")

            key = (
                row["kafka_topic"],
                row["kafka_partition"],
                row["kafka_offset"],
            )
            if any(value is None for value in key):
                raise ValueError("Missing Kafka identity")
            if key in identities:
                raise ValueError("Duplicate Kafka identity")
            identities.add(key)


def prepare(source_root, event_date, output_root):
    date.fromisoformat(event_date)
    source_root = Path(source_root).resolve()
    output_root = Path(output_root).resolve()
    partition = source_root / f"event_date={event_date}"

    if not partition.is_dir():
        raise ValueError(f"Source partition does not exist: {partition}")
    if output_root == source_root or source_root in output_root.parents:
        raise ValueError("Output must be outside the source dataset")

    before = inventory(partition)
    tables = []
    for item in before:
        table = pq.ParquetFile(partition / item["path"]).read()
        table = table.replace_schema_metadata(None)
        check_schema(table.schema)
        tables.append(table)

    table = pa.concat_tables(tables)
    if table.num_rows == 0:
        raise ValueError("Source partition contains no rows")
    validate_rows(table, event_date)

    fields = [
        pa.field(
            field.name,
            pa.timestamp("us")
            if field.name in TIMESTAMP_COLUMNS else field.type,
            nullable=field.nullable,
        )
        for field in table.schema
    ]
    table = table.cast(pa.schema(fields), safe=True)

    if inventory(partition) != before:
        raise ValueError("Source changed during preparation; retry later")

    fingerprint_payload = {
        "format_version": FORMAT_VERSION,
        "event_date": event_date,
        "source_files": before,
    }
    fingerprint = hashlib.sha256(
        json.dumps(
            fingerprint_payload, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()

    destination = output_root / f"event_date={event_date}"
    if destination.exists():
        manifest_path = destination / "manifest.json"
        output = destination / "part-00000.parquet"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("source_fingerprint") != fingerprint:
            raise ValueError("Existing artifact has different source input")
        if file_hash(output) != manifest.get("output_sha256"):
            raise ValueError("Existing artifact checksum mismatch")
        verified = pq.ParquetFile(output).read()
        if not table.equals(verified, check_metadata=False):
            raise ValueError("Existing artifact values do not match source")
        return {"status": "reused", "directory": str(destination), **manifest}

    output_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".prepare-", dir=output_root))
    try:
        output = temporary / "part-00000.parquet"
        pq.write_table(
            table,
            output,
            compression="snappy",
            version="1.0",
            coerce_timestamps="us",
            allow_truncated_timestamps=False,
        )
        verified = pq.ParquetFile(output).read()
        if not table.equals(verified, check_metadata=False):
            raise ValueError("Compacted output verification failed")

        manifest = {
            **fingerprint_payload,
            "source_fingerprint": fingerprint,
            "row_count": table.num_rows,
            "schema": [
                {"name": field.name, "type": str(field.type)}
                for field in table.schema
            ],
            "output_file": "part-00000.parquet",
            "output_bytes": output.stat().st_size,
            "output_sha256": file_hash(output),
        }
        (temporary / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        temporary.rename(destination)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)

    return {"status": "prepared", "directory": str(destination), **manifest}


def main():
    parser = argparse.ArgumentParser(
        description="Prepare a local Clean partition; no AWS operations."
    )
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--event-date", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args()
    try:
        result = prepare(args.source_root, args.event_date, args.output_root)
    except (ValueError, OSError, pa.ArrowException) as exc:
        parser.exit(1, f"Preparation failed: {exc}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
