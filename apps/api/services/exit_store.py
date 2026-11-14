"""Persistence for Reversibility Proof: declared exit systems and their rehearsals.

Two collections, ``exit_systems`` and ``exit_rehearsals``, in the control-plane
MongoDB when one is live (:func:`services.metadata_backend.mongo_database`),
else one JSON file under ``data_dir`` read-modify-written under a file lock
(:func:`services.metadata_backend.json_doc_transaction`) — the same fallback the
other metadata stores use.

Every document carries an integer ``version``. :func:`update` is a
compare-and-swap on it, so two replicas deciding on the same rehearsal (a
sign-off, a settlement) never lose one another's write: the loser gets
:class:`StoreConflict` and re-reads.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from pathlib import Path
from typing import Any

from services.platform_config import data_dir

SYSTEMS = "exit_systems"
REHEARSALS = "exit_rehearsals"
STORE_PATH = data_dir() / "exit_inventory.json"


class StoreConflict(RuntimeError):
    """The document changed since it was read (version mismatch)."""


def _path() -> Path:
    return STORE_PATH


def _db() -> Any:
    from services.metadata_backend import mongo_database

    return mongo_database()


def _clean(doc: dict[str, Any] | None) -> dict[str, Any] | None:
    if not doc:
        return None
    from services.value_serializer import python_document_from_bson

    out = python_document_from_bson(dict(doc))
    out.pop("_id", None)
    return out


def _empty() -> dict[str, Any]:
    return {SYSTEMS: {}, REHEARSALS: {}}


def get(collection: str, doc_id: str) -> dict[str, Any] | None:
    db = _db()
    if db is not None:
        return _clean(db[collection].find_one({"id": doc_id}))
    from services.metadata_backend import load_json_doc

    doc = load_json_doc(_path(), _empty()).get(collection, {}).get(doc_id)
    return copy.deepcopy(doc) if doc else None


def list_docs(collection: str, *, where: Callable[[dict[str, Any]], bool] | None = None,
              query: dict[str, Any] | None = None, newest_first: str = "", limit: int = 0) -> list[dict[str, Any]]:
    """Documents of ``collection`` matching ``query`` (equality) and ``where``."""
    db = _db()
    if db is not None:
        cursor = db[collection].find(dict(query or {}))
        if newest_first:
            cursor = cursor.sort(newest_first, -1)
        docs = [d for d in (_clean(x) for x in cursor) if d]
    else:
        from services.metadata_backend import load_json_doc

        raw = load_json_doc(_path(), _empty()).get(collection, {}).values()
        docs = [copy.deepcopy(d) for d in raw if all(d.get(k) == v for k, v in (query or {}).items())]
        if newest_first:
            docs.sort(key=lambda d: str(d.get(newest_first) or ""), reverse=True)
    if where is not None:
        docs = [d for d in docs if where(d)]
    return docs[:limit] if limit else docs


def insert(collection: str, doc: dict[str, Any]) -> dict[str, Any]:
    doc = {**doc, "version": int(doc.get("version") or 1)}
    db = _db()
    if db is not None:
        from services.value_serializer import bson_safe_document

        db[collection].insert_one(bson_safe_document(dict(doc)))
        return doc
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_path(), _empty()) as data:
        data.setdefault(collection, {})[doc["id"]] = copy.deepcopy(doc)
    return doc


def update(collection: str, doc_id: str, mutate: Callable[[dict[str, Any]], Any], *,
           expected_version: int | None = None) -> tuple[dict[str, Any], Any]:
    """Compare-and-swap: ``mutate(doc)`` edits a copy in place; the version bumps by one.

    Raises ``KeyError`` for a missing document and :class:`StoreConflict` when
    ``expected_version`` (or the version read here, for a concurrent writer)
    is not the stored one. Returns ``(new doc, mutate's return value)``.
    """
    db = _db()
    if db is not None:
        current = _clean(db[collection].find_one({"id": doc_id}))
        if current is None:
            raise KeyError(doc_id)
        version = int(current.get("version") or 0)
        if expected_version is not None and expected_version != version:
            raise StoreConflict(f"changed since it was read (version {version}, you had {expected_version})")
        result = mutate(current)
        current["version"] = version + 1
        from services.value_serializer import bson_safe_document

        done = db[collection].replace_one({"id": doc_id, "version": version}, bson_safe_document(dict(current)))
        if not done.matched_count:
            raise StoreConflict("changed while it was being written")
        return current, result
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_path(), _empty()) as data:
        bucket = data.setdefault(collection, {})
        current = copy.deepcopy(bucket.get(doc_id))
        if current is None:
            raise KeyError(doc_id)
        version = int(current.get("version") or 0)
        if expected_version is not None and expected_version != version:
            raise StoreConflict(f"changed since it was read (version {version}, you had {expected_version})")
        result = mutate(current)
        current["version"] = version + 1
        bucket[doc_id] = copy.deepcopy(current)
    return current, result


def delete(collection: str, doc_id: str) -> bool:
    db = _db()
    if db is not None:
        return bool(db[collection].delete_one({"id": doc_id}).deleted_count)
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_path(), _empty()) as data:
        return data.setdefault(collection, {}).pop(doc_id, None) is not None
