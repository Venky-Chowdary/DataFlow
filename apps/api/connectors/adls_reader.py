"""Azure Blob Storage / ADLS Gen2 object reader — stream payloads to disk."""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Any

from connectors.adls_common import blob_service_client
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
    cache_key = f"adls:{bucket}:{key}"
    path = download_object(
        cache_key,
        lambda p: download_for_object_store("adls", p, cfg, bucket, key),
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
    client = blob_service_client(cfg)
    container = client.get_container_client(bucket)
    return [b.name for b in itertools.islice(container.list_blobs(prefix=prefix or ""), 100)]
