"""Email denylist store (P2-2/P2-10) — domain, mx_host and email entries.

Mongo ``email_denylist`` with the file fallback, matching the other
metadata stores. Entries are raw deny facts; the disposable feed cache
lives in ``disposable_list`` and is checked separately.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from services.metadata_backend import json_doc_transaction, mongo_database
from services.platform_config import data_dir

DENYLIST_COLLECTION = "email_denylist"

_ENTRY_KINDS = ("domain", "mx_host", "email")
_SOURCES = ("feed", "team", "bounce")


def _path():
    return data_dir() / "email_denylist.json"


def _collection():
    db = mongo_database()
    if db is None:
        return None
    return db[DENYLIST_COLLECTION]


def list_entries() -> list[dict[str, Any]]:
    coll = _collection()
    if coll is not None:
        return [{k: v for k, v in d.items() if k != "_id"} for d in coll.find({})]
    from services.metadata_backend import load_json_doc

    return load_json_doc(_path(), {"entries": []}).get("entries", [])


def add_entry(*, value: str, kind: str, source: str = "team",
              reason: str = "", added_by: str = "") -> dict[str, Any]:
    """Idempotent upsert keyed on (kind, value)."""
    if kind not in _ENTRY_KINDS:
        raise ValueError(f"kind must be one of {_ENTRY_KINDS}")
    if source not in _SOURCES:
        raise ValueError(f"source must be one of {_SOURCES}")
    value = value.strip().lower()
    entry = {
        "id": f"den_{uuid.uuid4().hex}",
        "value": value,
        "kind": kind,
        "source": source,
        "reason": reason[:300],
        "added_by": added_by,
        "created_at": datetime.now(UTC).isoformat(),
    }
    coll = _collection()
    if coll is not None:
        coll.update_one(
            {"kind": kind, "value": value}, {"$set": entry}, upsert=True)
        return entry
    with json_doc_transaction(_path(), {"entries": []}) as doc:
        doc["entries"] = [
            e for e in doc.get("entries", [])
            if not (e.get("kind") == kind and e.get("value") == value)]
        doc["entries"].append(entry)
    return entry


def remove_entry(*, value: str, kind: str) -> bool:
    value = value.strip().lower()
    coll = _collection()
    if coll is not None:
        return coll.delete_one({"kind": kind, "value": value}).deleted_count > 0
    removed = False
    with json_doc_transaction(_path(), {"entries": []}) as doc:
        before = len(doc.get("entries", []))
        doc["entries"] = [
            e for e in doc["entries"]
            if not (e.get("kind") == kind and e.get("value") == value)]
        removed = len(doc["entries"]) != before
    return removed


def is_denied(value: str, *, kind: str) -> bool:
    value = (value or "").strip().lower()
    if not value:
        return False
    coll = _collection()
    if coll is not None:
        return coll.count_documents(
            {"kind": kind, "value": value}, limit=1) > 0
    return any(e.get("kind") == kind and e.get("value") == value
               for e in list_entries())


def deny_email_and_domain(email: str, *, source: str, reason: str,
                          added_by: str, include_domain: bool = False) -> None:
    """Ban helper: denylist the canonical email and optionally its domain."""
    from services.email_canonical import canonical_email

    canonical = canonical_email(email)
    add_entry(value=canonical, kind="email", source=source,
              reason=reason, added_by=added_by)
    if include_domain:
        domain = canonical.split("@", 1)[-1]
        add_entry(value=domain, kind="domain", source=source,
                  reason=reason, added_by=added_by)
