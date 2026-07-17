"""GCS object reader — stream object payloads to disk before parsing."""

from __future__ import annotations

from typing import Any

from connectors.gcs_common import gcs_client
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
        "gcs", cfg, bucket, key, offset=offset, limit=limit, known_total_rows=known_total_rows
    )


def list_objects(cfg: dict[str, Any], bucket: str, prefix: str = "") -> list[str]:
    client = gcs_client(cfg)
    return [b.name for b in client.list_blobs(bucket, prefix=prefix or "", max_results=100)]
