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


# Bound object-prefix listing so introspect cannot pretend a huge bucket is fully scanned.
_OBJECT_LIST_CAP = 2000


def list_objects(cfg: dict[str, Any], bucket: str, prefix: str = "") -> list[str]:
    return [row["name"] for row in list_object_stats(cfg, bucket, prefix=prefix)]


def list_object_stats(cfg: dict[str, Any], bucket: str, prefix: str = "") -> list[dict[str, Any]]:
    """Keys plus size, etag, and mtime — the pickup manifest identity."""
    client = boto3_client("s3", cfg)
    rows: list[dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
        for item in page.get("Contents") or []:
            rows.append(
                {
                    "name": item["Key"],
                    "size": int(item.get("Size") or 0),
                    "checksum": str(item.get("ETag") or "").strip('"'),
                    "mtime": str(item.get("LastModified") or ""),
                }
            )
            if len(rows) >= _OBJECT_LIST_CAP:
                return rows
    return rows
