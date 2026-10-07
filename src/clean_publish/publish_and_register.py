import argparse
import json

from .publish_clean_artifact import publish
from .register_clean_partition import register_partition


def run(
    artifact_dir,
    bucket,
    prefix,
    database,
    table,
    region="us-east-1",
    apply=False,
):
    prefix = prefix.strip("/")

    s3_client = None
    if apply:
        import boto3
        from botocore.config import Config

        s3_client = boto3.client(
            "s3",
            region_name=region,
            config=Config(signature_version="s3v4"),
        )

    published = publish(
        directory=artifact_dir,
        bucket=bucket,
        prefix=prefix,
        apply=apply,
        client=s3_client,
    )

    if not apply:
        return {
            "status": "dry_run",
            "publish": published,
            "registration": {
                "status": "skipped",
                "reason": "Publisher is in dry-run mode",
                "database": database,
                "table": table,
                "event_date": published["event_date"],
            },
        }

    if published["status"] not in ("uploaded", "reused"):
        raise RuntimeError("Publish did not return a verified success status")

    glue_client = boto3.client("glue", region_name=region)
    registered = register_partition(
        database=database,
        table=table,
        event_date=published["event_date"],
        dataset_uri=f"s3://{bucket}/{prefix}/",
        apply=True,
        client=glue_client,
    )

    return {
        "status": "completed",
        "publish": published,
        "registration": registered,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Publish and register a prepared Clean artifact."
    )
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    result = run(**vars(args))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()