"""Shared helpers for object-store readers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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


def read_object_from_store(
    store: str,
    cfg: dict[str, Any],
    bucket: str,
    key: str,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
) -> ReadBatch:
    """Download an object from ``store`` to a local spill file and read rows."""
    cache_key = f"{store}:{bucket}:{key}"
    path = download_object(
        cache_key,
        lambda p: download_for_object_store(store, p, cfg, bucket, key),
    )
    headers, rows, total = read_rows_from_spill(
        path,
        key,
        offset=offset,
        limit=limit,
        known_total=known_total_rows,
    )
    return ReadBatch(headers=headers, rows=rows, offset=offset, total_rows=total)
