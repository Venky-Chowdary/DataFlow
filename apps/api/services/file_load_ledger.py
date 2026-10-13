"""Which file bytes already landed on a destination route.

An unchanged file replayed in append mode is a measured no-op. It is not a
failed empty load and it is not a second copy of the same rows. Overwrite
still reloads. The digest is recorded only after reconcile passes, so a
checksum-failed partial write is retried.

Wildcard folder patterns, archive-after-load, and a load that starts when a
file arrives are not this ledger. Sheet selection other than the first is
``ReadOptions.sheet`` / ``sheet_index``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import uuid
from typing import Any

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir
from services.sync_cursor import build_cursor_key, route_endpoint_identity
from services.value_serializer import json_default

_logger = logging.getLogger(__name__)

STORE_PATH = data_dir() / "file_load_ledger.json"
_LOCK = threading.Lock()


def file_bytes_digest(content: bytes | str | os.PathLike) -> str:
    """sha256 of the file bytes. Paths are streamed; bytes are hashed in place."""
    digest = hashlib.sha256()
    if isinstance(content, (bytes, bytearray)):
        digest.update(bytes(content))
        return digest.hexdigest()
    path = os.fspath(content)
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_route_key(
    *,
    filename: str,
    dest_type: str,
    dest_database: str,
    dest_object: str,
    destination: Any = None,
) -> str:
    return build_cursor_key(
        source_type="file",
        source_database="",
        source_object=os.path.basename(filename or "upload"),
        dest_type=dest_type,
        dest_database=dest_database,
        dest_object=dest_object,
        stream_name="file",
        dest_identity=route_endpoint_identity(destination),
    )


def _mongo_ledger():  # type: ignore[no-untyped-def]
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if not mongo or type(mongo).__name__ == "MemoryMongoDBService":
            return None
        if getattr(mongo, "client", None):
            db = mongo.get_database()
            if db is not None and not isinstance(db, dict):
                return db["file_load_ledger"]
    except Exception:  # noqa: BLE001 — file ledger is the fallback store
        _logger.debug("file load ledger mongo unavailable", exc_info=True)
    return None


def _load() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {"files": []}
    try:
        return json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — unreadable ledger is an empty ledger
        return {"files": []}


def _save(data: dict[str, Any]) -> None:
    write_json_atomic(STORE_PATH, data, indent=2, default=json_default)


def file_already_loaded(route_key: str, digest: str) -> bool:
    route_key = (route_key or "").strip()
    digest = (digest or "").strip()
    if not route_key or not digest:
        return False
    coll = _mongo_ledger()
    if coll is not None:
        try:
            doc = coll.find_one({"key": route_key})
            return bool(doc and str(doc.get("digest") or "") == digest)
        except Exception:  # noqa: BLE001 — file store answers the same question
            _logger.exception("file load ledger read failed for %s", route_key)
    for entry in _load().get("files", []):
        if entry.get("key") == route_key and str(entry.get("digest") or "") == digest:
            return True
    return False


def record_file_loaded(route_key: str, digest: str) -> None:
    route_key = (route_key or "").strip()
    digest = (digest or "").strip()
    if not route_key or not digest:
        return
    coll = _mongo_ledger()
    if coll is not None:
        try:
            coll.find_one_and_update(
                {"key": route_key},
                {
                    "$set": {"key": route_key, "digest": digest},
                    "$setOnInsert": {"id": str(uuid.uuid4())},
                },
                upsert=True,
            )
            return
        except Exception:  # noqa: BLE001 — file store answers the same question
            _logger.exception("file load ledger write failed for %s", route_key)

    with _LOCK:
        data = _load()
        entries = list(data.get("files", []))
        updated = False
        for entry in entries:
            if entry.get("key") == route_key:
                entry["digest"] = digest
                updated = True
                break
        if not updated:
            entries.append({"id": str(uuid.uuid4()), "key": route_key, "digest": digest})
        data["files"] = entries[-500:]
        _save(data)


def note_file_identity(
    dest_summary: dict[str, Any],
    content: bytes | str | os.PathLike,
    *,
    filename: str,
    dest_type: str,
    dest_database: str,
    dest_object: str,
    destination: Any = None,
) -> None:
    """Attach digest + route key. Recording happens after reconcile passes."""
    try:
        dest_summary["file_digest"] = file_bytes_digest(content)
        dest_summary["file_route_key"] = file_route_key(
            filename=filename,
            dest_type=dest_type,
            dest_database=dest_database,
            dest_object=dest_object,
            destination=destination,
        )
    except Exception:  # noqa: BLE001 — a hash failure must not block the load
        _logger.warning("file identity not recorded for %s", filename, exc_info=True)


def record_successful_file_load(summary: dict[str, Any] | None) -> None:
    if not isinstance(summary, dict) or summary.get("file_already_loaded"):
        return
    record_file_loaded(
        str(summary.get("file_route_key") or ""),
        str(summary.get("file_digest") or ""),
    )
