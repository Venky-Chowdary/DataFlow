"""Sync cursor watermarks — incremental and CDC transfer state."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir

STORE_PATH = data_dir() / "sync_cursors.json"

INCREMENTAL_MODES = frozenset({
    "incremental_append",
    "incremental_deduped",
    "cdc",
})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class SyncContract:
    name: str
    sync_mode: str
    cursor_field: str = ""
    primary_key: str = ""
    schema_policy: str = "manual_review"
    validation_mode: str = "strict"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SyncContract:
        return cls(
            name=str(data.get("name") or data.get("stream") or "stream"),
            sync_mode=str(data.get("sync_mode") or "full_refresh_overwrite"),
            cursor_field=str(data.get("cursor_field") or data.get("cursor") or ""),
            primary_key=str(
                data.get("primary_key")
                or (data.get("primary_keys") or [""])[0]
                if isinstance(data.get("primary_keys"), list)
                else data.get("primary_key") or ""
            ),
            schema_policy=str(data.get("schema_policy") or "manual_review"),
            validation_mode=str(data.get("validation_mode") or "strict"),
        )


def resolve_sync_contract(stream_contracts: list[dict[str, Any]] | None) -> SyncContract | None:
    """Pick the first selected stream contract."""
    for raw in stream_contracts or []:
        if raw.get("selected", True):
            return SyncContract.from_dict(raw)
    return None


def build_cursor_key(
    *,
    source_type: str,
    source_database: str,
    source_object: str,
    dest_type: str,
    dest_database: str,
    dest_object: str,
    stream_name: str = "stream",
) -> str:
    return (
        f"{source_type}:{source_database}:{source_object}"
        f"→{dest_type}:{dest_database}:{dest_object}:{stream_name}"
    )


def _load() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {"cursors": []}
    try:
        return json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"cursors": []}


def _save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STORE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(STORE_PATH)


def get_watermark(cursor_key: str) -> str | None:
    for entry in _load().get("cursors", []):
        if entry.get("key") == cursor_key:
            val = entry.get("watermark")
            return str(val) if val is not None else None
    return None


def set_watermark(cursor_key: str, watermark: str, *, metadata: dict[str, Any] | None = None) -> None:
    data = _load()
    entries = list(data.get("cursors", []))
    updated = False
    for entry in entries:
        if entry.get("key") == cursor_key:
            entry["watermark"] = watermark
            entry["updated_at"] = _now()
            if metadata:
                entry["metadata"] = {**entry.get("metadata", {}), **metadata}
            updated = True
            break
    if not updated:
        entries.append({
            "id": str(uuid.uuid4()),
            "key": cursor_key,
            "watermark": watermark,
            "updated_at": _now(),
            "metadata": metadata or {},
        })
    data["cursors"] = entries[-500:]
    _save(data)


def max_cursor_value(rows: list[list[str]], headers: list[str], cursor_column: str) -> str | None:
    """Find maximum cursor value using typed watermark comparator."""
    if not cursor_column or not rows:
        return None
    try:
        idx = headers.index(cursor_column)
    except ValueError:
        return None
    values = [rows[i][idx] for i in range(len(rows)) if idx < len(rows[i]) and rows[i][idx]]
    if not values:
        return None
    from services.cdc_engine import infer_watermark_type, max_watermark

    str_values = [str(v) for v in values]
    wm_type = infer_watermark_type(str_values)
    return max_watermark(str_values, wm_type)


def compare_cursor_values(a: str | None, b: str | None) -> int:
    """Compare two cursor values using the same typed watermark logic.

    Returns -1 if a < b, 0 if equal, 1 if a > b.  None is treated as less
    than any value.
    """
    if a is None and b is None:
        return 0
    if a is None:
        return -1
    if b is None:
        return 1
    from services.cdc_engine import compare_watermarks, infer_watermark_type

    wm_type = infer_watermark_type([str(a), str(b)])
    return compare_watermarks(str(a), str(b), wm_type)


def requires_incremental(sync_mode: str) -> bool:
    return (sync_mode or "").lower() in INCREMENTAL_MODES


def requires_upsert(sync_mode: str) -> bool:
    return (sync_mode or "").lower() in {"upsert", "incremental_deduped", "cdc", "full_refresh_mirror", "mirror"}


def map_source_to_target(column: str, mappings: list[dict[str, Any]]) -> str:
    for m in mappings:
        if str(m.get("source") or "") == column:
            return str(m.get("target") or column)
    return column
