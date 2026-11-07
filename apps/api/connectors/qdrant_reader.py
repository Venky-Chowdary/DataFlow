"""Qdrant collection reader — scroll payload pages, never embedding arrays.

Airbyte's Qdrant connector is dest-only. Duplex payload read is the wedge:
identity and business columns live in the point payload (``id`` / ``source_id``
plus mapped fields). Vectors stay opaque — emitting them would fingerprint
hash/32 noise as VARCHAR and invent a dest type. Scroll offset is a
continuation token, not a row OFFSET (re-reading page one would fail-close
uniqueness as duplicate keys).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from connectors.base import ReadBatch
from connectors.header_union import union_attribute_keys
from connectors.qdrant_writer import (
    _qdrant_live_payload_types,
    _qdrant_points_count,
    qdrant_rest,
)

_api_root = Path(__file__).resolve().parents[1]
if str(_api_root) not in sys.path:
    sys.path.insert(0, str(_api_root))

from services.value_serializer import (
    DF_MISSING_SENTINEL,
    cell_to_string,
    json_default,
    load_http_json,
)

# Writer bookkeeping + opaque embeddings. Business payload (id/amount/code)
# is the unique-engine fixture. Pair mappings also G13-omit these names so a
# later payload key cannot leak into dest.
QDRANT_OMIT_PAYLOAD_KEYS = frozenset(
    {
        "embedding",
        "vector",
        "chunk_index",
        "content",
        "source_id",
        "qdrant_id",
    }
)


def _cell(value: Any) -> str:
    return cell_to_string(value, preserve_sql_null=True)


def _payload_row(point: dict[str, Any]) -> dict[str, Any]:
    """One point as a source record. Identity from payload, never the vector."""
    payload = point.get("payload") if isinstance(point, dict) else None
    row: dict[str, Any] = {}
    if isinstance(payload, dict):
        for key, val in payload.items():
            name = str(key or "").strip()
            if not name or name in QDRANT_OMIT_PAYLOAD_KEYS:
                continue
            row[name] = val
    if "id" not in row:
        sid = payload.get("source_id") if isinstance(payload, dict) else None
        if sid is not None and str(sid).strip():
            row["id"] = sid
        elif isinstance(point, dict) and point.get("id") is not None:
            row["id"] = point.get("id")
    return row


def read_points_batch(
    *,
    cfg: dict[str, Any],
    collection: str | None = None,
    table: str | None = None,
    columns: list[str] | None = None,
    offset: int = 0,
    limit: int = 500,
    known_total_rows: int | None = None,
    qdrant_offset: Any = None,
    **_kwargs: Any,
) -> tuple[ReadBatch, Any]:
    """Scroll one page of payload records. ``qdrant_offset`` is the REST cursor.

    ``offset`` is ignored — Qdrant scroll is not row-OFFSET. Hand the
    ``next_page_offset`` this function returns back on the next call, or the
    reader restarts at the first point.
    """
    del offset
    name = str(collection or table or "").strip()
    if not name:
        return (
            ReadBatch(headers=[], rows=[], offset=0, total_rows=0),
            None,
        )
    session, base_url, headers = qdrant_rest(cfg)
    exists = session.get(
        f"{base_url}/collections/{name}", headers=headers, timeout=10
    )
    if exists.status_code == 404:
        return (
            ReadBatch(headers=list(columns or []), rows=[], offset=0, total_rows=0),
            None,
        )
    if exists.status_code != 200:
        raise RuntimeError(
            f"Qdrant collection probe failed: {exists.status_code} {exists.text[:300]}"
        )
    try:
        info = exists.json()
    except Exception:
        info = {}
    physical = _qdrant_points_count(info if isinstance(info, dict) else None)
    total = known_total_rows if known_total_rows is not None else physical

    body: dict[str, Any] = {
        "limit": max(1, min(int(limit or 500), 10_000)),
        "with_payload": True,
        "with_vector": False,
    }
    if qdrant_offset is not None:
        body["offset"] = qdrant_offset
    resp = session.post(
        f"{base_url}/collections/{name}/points/scroll",
        data=json.dumps(body, default=json_default),
        headers=headers,
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Qdrant scroll failed: {resp.status_code} {resp.text[:300]}"
        )
    payload = load_http_json(resp) if resp.content else {}
    result = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(result, dict):
        raise RuntimeError("Qdrant scroll returned no result object")
    points = result.get("points") or []
    if not isinstance(points, list):
        raise RuntimeError("Qdrant scroll points is not a list")
    records = [_payload_row(p) for p in points if isinstance(p, dict)]
    page_keys: list[str] = []
    seen: set[str] = set()
    for preferred in ("id", "amount", "code"):
        if any(preferred in r for r in records) and preferred not in seen:
            seen.add(preferred)
            page_keys.append(preferred)
    for rec in records:
        for key in rec.keys():
            if key not in seen:
                seen.add(key)
                page_keys.append(key)
    headers_out = union_attribute_keys(columns, page_keys) if columns else page_keys
    rows: list[list[str]] = []
    for rec in records:
        row: list[str] = []
        for h in headers_out:
            if h not in rec:
                row.append(DF_MISSING_SENTINEL)
            else:
                row.append(_cell(rec[h]))
        rows.append(row)
    meta: dict[str, Any] = {}
    if qdrant_offset is None:
        native = _qdrant_live_payload_types(info if isinstance(info, dict) else None)
        declared = {
            k: v
            for k, v in native.items()
            if k and k not in QDRANT_OMIT_PAYLOAD_KEYS and v
        }
        if declared:
            from services.dest_schema_authority import declared_native_types_meta

            meta.update(declared_native_types_meta(declared))
    nxt = result.get("next_page_offset")
    batch = ReadBatch(
        headers=headers_out,
        rows=rows,
        offset=0,
        total_rows=total,
        meta=meta or None,
    )
    return batch, nxt
