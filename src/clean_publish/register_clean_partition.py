import argparse
import copy
import json
from datetime import date


def verify_partition(partition, expected_sd):
    actual = partition["StorageDescriptor"]
    for field in (
        "Location",
        "Columns",
        "InputFormat",
        "OutputFormat",
        "SerdeInfo",
    ):
        if actual.get(field) != expected_sd.get(field):
            raise ValueError(f"Existing partition differs in {field}")


def register_partition(
    database, table, event_date, dataset_uri, apply=False, client=None
):
    if date.fromisoformat(event_date).isoformat() != event_date:
        raise ValueError("event_date must use YYYY-MM-DD format")
    root = dataset_uri.rstrip("/") + "/"
    if not root.startswith("s3://") or len(root.split("/", 3)) < 4:
        raise ValueError("An S3 dataset URI is required")
    if client is None:
        raise ValueError("A Glue client is required")

    definition = client.get_table(
        DatabaseName=database, Name=table
    )["Table"]

    keys = definition.get("PartitionKeys", [])
    if len(keys) != 1 or keys[0]["Name"] != "event_date":
        raise ValueError("Expected exactly one partition key: event_date")

    sd = copy.deepcopy(definition["StorageDescriptor"])
    if sd.get("Location", "").rstrip("/") + "/" != root:
        raise ValueError("Table location does not match dataset URI")
    if "parquet" not in sd.get("InputFormat", "").lower():
        raise ValueError("Expected a Parquet table")

    sd["Location"] = root + f"event_date={event_date}/"
    sd.pop("AdditionalLocations", None)

    result = {
        "database": database,
        "table": table,
        "event_date": event_date,
        "partition_location": sd["Location"],
    }

    try:
        existing = client.get_partition(
            DatabaseName=database,
            TableName=table,
            PartitionValues=[event_date],
        )["Partition"]
    except client.exceptions.EntityNotFoundException:
        existing = None

    if existing is not None:
        verify_partition(existing, sd)

    if not apply:
        return {
            **result,
            "status": "dry_run",
            "planned_action": "reuse" if existing else "create",
        }

    if existing is not None:
        return {**result, "status": "reused"}

    response = client.batch_create_partition(
        DatabaseName=database,
        TableName=table,
        PartitionInputList=[
            {"Values": [event_date], "StorageDescriptor": sd}
        ],
    )
    errors = response.get("Errors", [])
    if any(
        error.get("ErrorDetail", {}).get("ErrorCode")
        != "AlreadyExistsException"
        for error in errors
    ):
        raise RuntimeError(json.dumps(errors))

    registered = client.get_partition(
        DatabaseName=database,
        TableName=table,
        PartitionValues=[event_date],
    )["Partition"]
    verify_partition(registered, sd)

    return {
        **result,
        "status": "reused" if errors else "created",
    }


def main():
    parser = argparse.ArgumentParser(
        description="Register a published Clean partition; dry run by default."
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--event-date", required=True)
    parser.add_argument("--dataset-uri", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    import boto3

    result = register_partition(
        database=args.database,
        table=args.table,
        event_date=args.event_date,
        dataset_uri=args.dataset_uri,
        apply=args.apply,
        client=boto3.client("glue", region_name=args.region),
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()