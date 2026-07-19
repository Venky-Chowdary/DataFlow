"""Native CDC schema history — DDL / schema snapshots for decode rebuild.

Persists schema versions keyed by (source_key, table, version/offset) so change
streams can rebuild a decode schema after restart without guessing from live
catalog alone. Prefers Mongo when available; otherwise JSON under
``data/cdc_schema_history/``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir
from services.value_serializer import json_default

_logger = logging.getLogger(__name__)

STORE_DIR = data_dir() / "cdc_schema_history"
COLLECTION = "cdc_schema_history"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connection_fingerprint(cfg: dict[str, Any], *, connector_id: str = "") -> str:
    """Stable source key from connector id or connection fields."""
    if connector_id:
        return f"connector:{connector_id}"
    host = str(cfg.get("host") or "localhost")
    port = str(cfg.get("port") or "")
    database = str(cfg.get("database") or cfg.get("schema") or "")
    driver = str(cfg.get("type") or cfg.get("driver") or "")
    return f"{driver}:{host}:{port}/{database}"


def _mongo():
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if not mongo or type(mongo).__name__ == "MemoryMongoDBService":
            return None
        if getattr(mongo, "client", None):
            db = mongo.get_database()
            if db is not None:
                return db[COLLECTION]
    except Exception:
        return None
    return None


def _safe_segment(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "_", value)[:48].strip("_") or "src"
    return f"{slug}_{digest}"


def _file_path(source_key: str, table: str) -> Path:
    STORE_DIR.mkdir(parents=True, exist_ok=True)
    return STORE_DIR / f"{_safe_segment(source_key)}__{_safe_segment(table)}.json"


def _normalize_offset(offset: Any) -> Any:
    if isinstance(offset, (str, int, float)) or offset is None:
        return offset
    if isinstance(offset, dict):
        return {k: offset[k] for k in sorted(offset) if k in offset}
    return str(offset)


def _offset_sort_key(offset: Any) -> tuple:
    """Comparable key for opaque CDC offsets (LSN, binlog pos, int, version)."""
    if offset is None:
        return (0, 0, "")
    if isinstance(offset, bool):
        return (1, int(offset), "")
    if isinstance(offset, (int, float)):
        return (1, float(offset), "")
    if isinstance(offset, dict):
        file_name = str(offset.get("file") or offset.get("log_file") or "")
        pos = offset.get("pos")
        if pos is None:
            pos = offset.get("log_pos")
        try:
            pos_n = float(pos) if pos is not None else 0.0
        except (TypeError, ValueError):
            pos_n = 0.0
        return (2, pos_n, file_name)
    text = str(offset)
    # PostgreSQL LSN: "16/B374D848" — compare numerically by parts when possible.
    if "/" in text and all(p for p in text.split("/", 1)):
        left, right = text.split("/", 1)
        try:
            return (3, int(left, 16), f"{int(right, 16):016x}")
        except ValueError:
            pass
    return (4, 0.0, text)


def _offset_lte(a: Any, b: Any) -> bool:
    return _offset_sort_key(a) <= _offset_sort_key(b)


def _load_file(source_key: str, table: str) -> dict[str, Any]:
    path = _file_path(source_key, table)
    if not path.exists():
        return {"source_key": source_key, "table": table, "entries": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"source_key": source_key, "table": table, "entries": []}
        data.setdefault("entries", [])
        return data
    except Exception:
        return {"source_key": source_key, "table": table, "entries": []}


def _save_file(source_key: str, table: str, data: dict[str, Any]) -> None:
    path = _file_path(source_key, table)
    write_json_atomic(path, data, indent=2, default=json_default)


def _next_version(entries: list[dict[str, Any]]) -> int:
    if not entries:
        return 1
    return max(int(e.get("version") or 0) for e in entries) + 1


def record_ddl(
    source_key: str,
    table: str,
    ddl: str,
    offset: Any,
    schema_snapshot: dict[str, Any],
) -> dict[str, Any]:
    """Persist one DDL / schema snapshot. Returns the stored entry."""
    source_key = str(source_key or "").strip()
    table = str(table or "").strip()
    if not source_key or not table:
        raise ValueError("source_key and table are required")

    entry = {
        "id": str(uuid.uuid4()),
        "source_key": source_key,
        "table": table,
        "ddl": ddl or "",
        "offset": _normalize_offset(offset),
        "schema_snapshot": dict(schema_snapshot or {}),
        "recorded_at": _now(),
        "version": 0,
    }

    coll = _mongo()
    if coll is not None:
        try:
            prior = list(
                coll.find({"source_key": source_key, "table": table})
                .sort("version", -1)
                .limit(1)
            )
            entry["version"] = int(prior[0]["version"]) + 1 if prior else 1
            coll.insert_one({**entry, "_id": entry["id"]})
            return entry
        except Exception:
            _logger.exception("Mongo CDC schema history insert failed; falling back to file")

    data = _load_file(source_key, table)
    entries = list(data.get("entries") or [])
    entry["version"] = _next_version(entries)
    entries.append(entry)
    data["entries"] = entries
    data["source_key"] = source_key
    data["table"] = table
    data["updated_at"] = _now()
    _save_file(source_key, table, data)
    return entry


def list_history(source_key: str, table: str) -> list[dict[str, Any]]:
    """Return schema history entries ordered by version ascending."""
    source_key = str(source_key or "").strip()
    table = str(table or "").strip()
    coll = _mongo()
    if coll is not None:
        try:
            docs = list(
                coll.find({"source_key": source_key, "table": table}).sort("version", 1)
            )
            for d in docs:
                d.pop("_id", None)
            return docs
        except Exception:
            _logger.exception("Mongo CDC schema history list failed; falling back to file")

    entries = list(_load_file(source_key, table).get("entries") or [])
    return sorted(entries, key=lambda e: int(e.get("version") or 0))


def rebuild_schema(
    source_key: str,
    table: str,
    up_to_offset: Any = None,
) -> dict[str, Any]:
    """Rebuild decode schema by replaying snapshots up to ``up_to_offset``.

    When ``up_to_offset`` is None, returns the latest snapshot (or empty schema).
    """
    history = list_history(source_key, table)
    if not history:
        return {}

    applicable = history
    if up_to_offset is not None:
        # Allow callers to pass a version int as a shortcut.
        if isinstance(up_to_offset, int) and not isinstance(up_to_offset, bool):
            applicable = [e for e in history if int(e.get("version") or 0) <= up_to_offset]
        else:
            applicable = [e for e in history if _offset_lte(e.get("offset"), up_to_offset)]

    if not applicable:
        return {}

    latest = applicable[-1]
    snapshot = latest.get("schema_snapshot") or {}
    return dict(snapshot) if isinstance(snapshot, dict) else {}


def last_ddl_at(source_key: str, table: str) -> str | None:
    """Timestamp of the most recent recorded DDL for a table, if any."""
    history = list_history(source_key, table)
    if not history:
        return None
    return str(history[-1].get("recorded_at") or "") or None
