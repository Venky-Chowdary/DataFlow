"""S3 object reader — stream object payloads to disk before parsing."""

from __future__ import annotations

from typing import Any

from connectors.aws_common import boto3_client
from connectors.object_store_common import ReadBatch, read_object_from_store


def read_object(
    *,
    cfg: dict[str, Any],
    bucket: str,
    key: str,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
    return read_object_from_store(
        "s3", cfg, bucket, key, offset=offset, limit=limit, known_total_rows=known_total_rows
    )


def list_objects(cfg: dict[str, Any], bucket: str, prefix: str = "") -> list[str]:
    client = boto3_client("s3", cfg)
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
        for item in page.get("Contents") or []:
            keys.append(item["Key"])
    return keys[:100]
