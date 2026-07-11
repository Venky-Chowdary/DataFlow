"""GCS object reader — download and parse JSON/CSV/JSONL/Parquet objects."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from connectors.gcs_common import gcs_client
from connectors.object_read_cache import get_or_parse


@dataclass
class ReadBatch:
    headers: list[str]
    rows: list[list[str]]
    offset: int = 0
    total_rows: int = 0


def _parse_object_body(body: bytes, key: str) -> tuple[list[dict], list[str], dict[str, str]]:
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    if str(root / "src") not in sys.path:
        sys.path.insert(0, str(root / "src"))
    from services.file_parser import FileParser

    result = FileParser.parse(body, key)
    if not result.success:
        raise ValueError(result.error or f"Cannot parse GCS object `{key}`")
    schema = FileParser.infer_schema(result.data)
    return result.data, result.columns, schema


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

    def _load() -> tuple[list[dict], list[str], dict[str, str]]:
        client = gcs_client(cfg)
        blob = client.bucket(bucket).blob(key)
        body = blob.download_as_bytes()
        return _parse_object_body(body, key)

    records, columns, total = get_or_parse(cache_key, _load)
    if known_total_rows is not None:
        total = known_total_rows
    slice_rows = records[offset : offset + limit]

    def cell(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, (dict, list)):
            return json.dumps(v, default=str)
        return str(v)

    rows = [[cell(r.get(c)) for c in columns] for r in slice_rows]
    return ReadBatch(headers=columns, rows=rows, offset=offset, total_rows=total)


def list_objects(cfg: dict[str, Any], bucket: str, prefix: str = "") -> list[str]:
    client = gcs_client(cfg)
    return [b.name for b in client.list_blobs(bucket, prefix=prefix or "", max_results=100)]
