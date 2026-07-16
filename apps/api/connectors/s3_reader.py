"""S3 object reader — stream object payloads to disk before parsing."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from connectors.aws_common import boto3_client
from services.object_streaming import download_object, read_rows_from_spill


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int = 0
    total_rows: int = 0


def _download_s3_object(path: Path, cfg: dict[str, Any], bucket: str, key: str) -> None:
    client = boto3_client("s3", cfg)
    obj = client.get_object(Bucket=bucket, Key=key)
    with open(path, "wb") as f:
        for chunk in obj["Body"].iter_chunks(chunk_size=8 * 1024 * 1024):
            if chunk:
                f.write(chunk)


def read_object(
    *,
    cfg: dict[str, Any],
    bucket: str,
    key: str,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
    cache_key = f"s3:{bucket}:{key}"
    path = download_object(cache_key, lambda p: _download_s3_object(p, cfg, bucket, key))
    headers, rows, total = read_rows_from_spill(
        path,
        key,
        offset=offset,
        limit=limit,
        known_total=known_total_rows,
    )
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)


def list_objects(cfg: dict[str, Any], bucket: str, prefix: str = "") -> list[str]:
    client = boto3_client("s3", cfg)
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
        for item in page.get("Contents") or []:
            keys.append(item["Key"])
    return keys[:100]
