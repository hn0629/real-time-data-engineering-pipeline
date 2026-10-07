import argparse
import hashlib
import json
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq


def load_artifact(directory):
    directory = Path(directory)
    manifest = json.loads(
        (directory / "manifest.json").read_text(encoding="utf-8")
    )
    if manifest.get("format_version") != 1:
        raise ValueError("Unsupported manifest format")
    date.fromisoformat(manifest["event_date"])
    if manifest.get("output_file") != "part-00000.parquet":
        raise ValueError("Unexpected output filename")
    fingerprint = manifest.get("source_fingerprint", "")
    if (
        len(fingerprint) != 64
        or any(c not in "0123456789abcdef" for c in fingerprint)
    ):
        raise ValueError("Invalid source fingerprint")

    output = directory / manifest["output_file"]
    data = output.read_bytes()
    checksum = hashlib.sha256(data).hexdigest()
    if checksum != manifest.get("output_sha256"):
        raise ValueError("Local artifact checksum mismatch")
    if len(data) != manifest.get("output_bytes"):
        raise ValueError("Local artifact size mismatch")
    if pq.ParquetFile(output).metadata.num_rows != manifest.get("row_count"):
        raise ValueError("Local artifact row count mismatch")
    return manifest, data


def verify_remote(response, manifest):
    try:
        data = response["Body"].read()
    finally:
        response["Body"].close()

    if hashlib.sha256(data).hexdigest() != manifest["output_sha256"]:
        raise ValueError("Existing S3 object has different bytes")
    metadata = response.get("Metadata", {})
    if metadata.get("source-fingerprint") != manifest["source_fingerprint"]:
        raise ValueError("Existing S3 object has a different source fingerprint")
    if metadata.get("row-count") != str(manifest["row_count"]):
        raise ValueError("Existing S3 object has a different row count")


def publish(directory, bucket, prefix, apply=False, client=None):
    manifest, data = load_artifact(directory)
    prefix = prefix.strip("/")
    if not bucket or not prefix:
        raise ValueError("Bucket and nonempty dataset prefix are required")

    key = (
        f"{prefix}/event_date={manifest['event_date']}/"
        "part-00000.parquet"
    )
    result = {
        "status": "dry_run",
        "s3_uri": f"s3://{bucket}/{key}",
        "event_date": manifest["event_date"],
        "row_count": manifest["row_count"],
        "bytes": len(data),
        "sha256": manifest["output_sha256"],
        "source_fingerprint": manifest["source_fingerprint"],
    }
    if not apply:
        return result
    if client is None:
        raise ValueError("An S3 client is required for apply mode")

    from botocore.exceptions import ClientError

    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "NoSuchKey":
            raise
    else:
        verify_remote(response, manifest)
        return {**result, "status": "reused"}

    def prevent_overwrite(params, **kwargs):
        params["headers"]["If-None-Match"] = "*"

    hook_id = "clean-publish-prevent-overwrite"
    client.meta.events.register(
        "before-call.s3.PutObject",
        prevent_overwrite,
        unique_id=hook_id,
    )
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType="application/octet-stream",
            Metadata={
                "sha256": manifest["output_sha256"],
                "source-fingerprint": manifest["source_fingerprint"],
                "row-count": str(manifest["row_count"]),
            },
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in (
            "PreconditionFailed", "412"
        ):
            raise
        verify_remote(
            client.get_object(Bucket=bucket, Key=key), manifest
        )
        return {**result, "status": "reused"}
    finally:
        client.meta.events.unregister(
            "before-call.s3.PutObject", unique_id=hook_id
        )

    verify_remote(client.get_object(Bucket=bucket, Key=key), manifest)
    return {**result, "status": "uploaded"}


def main():
    parser = argparse.ArgumentParser(
        description="Upload a prepared Clean artifact; dry run by default."
    )
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    client = None
    if args.apply:
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "s3",
            region_name=args.region,
            config=Config(signature_version="s3v4"),
        )

    result = publish(
        args.artifact_dir,
        args.bucket,
        args.prefix,
        apply=args.apply,
        client=client,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
