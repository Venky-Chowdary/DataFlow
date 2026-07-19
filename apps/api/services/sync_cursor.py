"""Sync cursor watermarks — incremental and CDC transfer state.

Prefers MongoDB when a shared backend is available (multi-replica safe via
find_one_and_update). Falls back to atomic JSON file for single-instance /
test mode only.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir
from services.value_serializer import json_default

_logger = logging.getLogger(__name__)

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
    selected = resolve_selected_sync_contracts(stream_contracts)
    return selected[0] if selected else None


def resolve_selected_sync_contracts(
    stream_contracts: list[dict[str, Any]] | None,
) -> list[SyncContract]:
    """Return every selected stream contract (multi-stream foundation)."""
    out: list[SyncContract] = []
    for raw in stream_contracts or []:
        if raw.get("selected", True):
            out.append(SyncContract.from_dict(raw))
    return out


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


def _mongo_cursors():  # type: ignore[no-untyped-def]
    try:
        from services.mongodb_service import get_mongodb_service
        from services.worker_leases import requires_distributed_backend

        mongo = get_mongodb_service()
        if not mongo or type(mongo).__name__ == "MemoryMongoDBService":
            if requires_distributed_backend():
                return None
            return None
        if getattr(mongo, "client", None):
            db = mongo.get_database()
            if db is not None:
                return db["sync_cursors"]
    except Exception:
        pass
    return None


def _load() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {"cursors": []}
    try:
        return json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"cursors": []}


def _save(data: dict[str, Any]) -> None:
    write_json_atomic(STORE_PATH, data, indent=2, default=json_default)


def get_watermark(cursor_key: str) -> str | None:
    coll = _mongo_cursors()
    if coll is not None:
        try:
            doc = coll.find_one({"key": cursor_key})
            if doc and doc.get("watermark") is not None:
                return str(doc["watermark"])
            return None
        except Exception:
            _logger.exception("Mongo get_watermark failed for %s", cursor_key)

    for entry in _load().get("cursors", []):
        if entry.get("key") == cursor_key:
            val = entry.get("watermark")
            return str(val) if val is not None else None
    return None


def set_watermark(cursor_key: str, watermark: str, *, metadata: dict[str, Any] | None = None) -> None:
    """Persist watermark with CAS semantics when Mongo is available."""
    coll = _mongo_cursors()
    if coll is not None:
        try:
            now = _now()
            update: dict[str, Any] = {
                "key": cursor_key,
                "watermark": watermark,
                "updated_at": now,
            }
            if metadata:
                update["metadata"] = metadata
            coll.find_one_and_update(
                {"key": cursor_key},
                {
                    "$set": update,
                    "$setOnInsert": {"id": str(uuid.uuid4())},
                },
                upsert=True,
            )
            return
        except Exception:
            _logger.exception("Mongo set_watermark failed for %s; falling back to file", cursor_key)

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
    return (sync_mode or "").lower() in {
        "upsert",
        "incremental_deduped",
        "cdc",
        "full_refresh_mirror",
        "mirror",
    }


def map_source_to_target(column: str, mappings: list[dict[str, Any]]) -> str:
    for m in mappings:
        if str(m.get("source") or "") == column:
            return str(m.get("target") or column)
    return column
