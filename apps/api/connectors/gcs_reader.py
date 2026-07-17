"""GCS object reader — stream object payloads to disk before parsing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from connectors.gcs_common import gcs_client
from services.object_streaming import (
    download_for_object_store,
    download_object,
    read_rows_from_spill,
)


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int = 0
    total_rows: int = 0


def read_object(
    *,
    cfg: dict[str, Any],
    bucket: str,
    key: str,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
    cache_key = f"gcs:{bucket}:{key}"
    path = download_object(
        cache_key,
        lambda p: download_for_object_store("gcs", p, cfg, bucket, key),
    )
    headers, rows, total = read_rows_from_spill(
        path,
        key,
        offset=offset,
        limit=limit,
        known_total=known_total_rows,
    )
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)


def list_objects(cfg: dict[str, Any], bucket: str, prefix: str = "") -> list[str]:
    client = gcs_client(cfg)
    return [b.name for b in client.list_blobs(bucket, prefix=prefix or "", max_results=100)]
