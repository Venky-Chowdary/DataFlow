"""Per-route acknowledgement: "accept possible boundary duplicates" (D335).

A route whose watermark boundary cannot be checked against its destination
(no key, a cursor the destination cannot compare) fails with a remedy. One
remedy is this acknowledgement. It is a decision with consequences - the
route may append its watermark row again - so it is recorded only through
``POST /api/v1/connectors/saved/{connector_id}/boundary-duplicates-acknowledgement``
by a user holding ``job.manage`` (the permission that approves DDL), with the
actor taken from the session and an audit event written. A value typed into a
destination's options is not an acknowledgement and is ignored.

Stored in the control-plane MongoDB when one is live, else in one JSON file
under ``data_dir`` (the same fallback the other metadata stores use).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir

COLLECTION = "boundary_duplicate_acks"
STORE_PATH = data_dir() / "boundary_duplicate_acks.json"


def _path() -> Path:
    return STORE_PATH


def _id(connector_id: str, schema: str, table: str) -> str:
    """One route: the saved destination, its schema and its table."""
    return f"{str(connector_id).strip()}:{str(schema or '').strip().casefold()}:{str(table).strip().casefold()}"


def _db() -> Any:
    from services.metadata_backend import mongo_database

    return mongo_database()


def record(
    connector_id: str, schema: str, table: str, *, actor: str, reason: str, workspace_id: str
) -> dict[str, Any]:
    """Store the acknowledgement for one route (a saved destination, schema and table)."""
    doc = {
        "id": _id(connector_id, schema, table),
        "connector_id": str(connector_id),
        "schema": str(schema or ""),
        "table": str(table),
        "actor": str(actor),
        "reason": str(reason),
        "workspace_id": str(workspace_id or ""),
        "at": datetime.now(timezone.utc).isoformat(),
    }
    db = _db()
    if db is not None:
        db[COLLECTION].replace_one({"id": doc["id"]}, dict(doc), upsert=True)
        return doc
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_path(), {"acks": {}}) as data:
        data.setdefault("acks", {})[doc["id"]] = dict(doc)
    return doc


def revoke(connector_id: str, schema: str, table: str) -> bool:
    """Withdraw the route's acknowledgement; True when one was stored."""
    key = _id(connector_id, schema, table)
    db = _db()
    if db is not None:
        return bool(db[COLLECTION].delete_one({"id": key}).deleted_count)
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_path(), {"acks": {}}) as data:
        return data.setdefault("acks", {}).pop(key, None) is not None


def lookup(connector_id: str, schema: str, table: str) -> dict[str, Any] | None:
    """The route's acknowledgement, or None."""
    if not str(connector_id or "").strip() or not str(table or "").strip():
        return None
    key = _id(connector_id, schema, table)
    db = _db()
    if db is not None:
        found = db[COLLECTION].find_one({"id": key})
        return {k: v for k, v in dict(found).items() if k != "_id"} if found else None
    from services.metadata_backend import load_json_doc

    found = load_json_doc(_path(), {"acks": {}}).get("acks", {}).get(key)
    return dict(found) if found else None
