"""Persistence for the Evidence Ledger: keyrings, log entries, checkpoints.

Three collections in the control-plane MongoDB when one is live
(:func:`services.metadata_backend.mongo_database`), else one JSON file under
``data_dir`` read-modify-written under a file lock
(:func:`services.metadata_backend.json_doc_transaction`) — the fallback the
other metadata stores use (:mod:`services.exit_store`).

What makes the log append-only and unforkable is here:

* an entry's id **is** its leaf index, and :func:`append_entry` only ever
  inserts (never updates) at ``last index + 1``. Two replicas appending at once
  both try the same id; one insert wins, the other re-reads and takes the next
  index. No position can hold two leaves, and there are no gaps.
* a checkpoint's id is its tree size and :func:`put_if_absent` never replaces.
* keyrings change by compare-and-swap on ``revision`` (:func:`replace_if`).

Entries and checkpoints are written with majority write concern, so a leaf a
receipt was issued for is not lost to a failover and replaced by another.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from services.platform_config import data_dir

KEYS = "evidence_ledger_keys"
ENTRIES = "evidence_ledger_entries"
CHECKPOINTS = "evidence_ledger_checkpoints"
META = "evidence_ledger_meta"
_DEDUPE = "evidence_ledger_dedupe"  # file store only: dedupe key -> index
STORE_FILE = "evidence_ledger.json"
APPEND_ATTEMPTS = 64

_indexed: set[str] = set()


class StoreConflict(RuntimeError):
    """The document changed since it was read, or the log could not be appended to."""


def _path() -> Path:
    return data_dir() / STORE_FILE


def _db() -> Any:
    from services.metadata_backend import mongo_database

    return mongo_database()


def namespace() -> str:
    """Identifies the store in use (cache key for the in-process tree)."""
    db = _db()
    return f"mongo:{db.name}" if db is not None else f"file:{_path()}"


def _coll(db: Any, name: str) -> Any:
    coll = db[name]
    if name in (ENTRIES, CHECKPOINTS):
        from pymongo.write_concern import WriteConcern

        coll = coll.with_options(write_concern=WriteConcern(w="majority"))
        key = f"{db.name}.{name}"
        if name == ENTRIES and key not in _indexed:
            coll.create_index("dedupe", unique=True)
            coll.create_index([("workspace_id", 1), ("_id", -1)])
            _indexed.add(key)
    return coll


def _clean(doc: dict[str, Any] | None) -> dict[str, Any] | None:
    if not doc:
        return None
    from services.value_serializer import python_document_from_bson

    out = python_document_from_bson(dict(doc))
    out.pop("_id", None)
    return out


def _empty() -> dict[str, Any]:
    return {KEYS: {}, ENTRIES: {}, CHECKPOINTS: {}, META: {}, _DEDUPE: {}}


def _file() -> dict[str, Any]:
    from services.metadata_backend import load_json_doc

    return load_json_doc(_path(), _empty())


def get(collection: str, doc_id: Any) -> dict[str, Any] | None:
    db = _db()
    if db is not None:
        return _clean(_coll(db, collection).find_one({"_id": doc_id}))
    doc = _file().get(collection, {}).get(str(doc_id))
    return copy.deepcopy(doc) if doc else None


def put_if_absent(collection: str, doc_id: Any, doc: dict[str, Any]) -> bool:
    """Insert ``doc`` under ``doc_id``. False (and nothing written) when one is already there."""
    db = _db()
    if db is not None:
        from pymongo.errors import DuplicateKeyError

        try:
            _coll(db, collection).insert_one({**copy.deepcopy(doc), "_id": doc_id})
        except DuplicateKeyError:
            return False
        return True
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_path(), _empty()) as data:
        bucket = data.setdefault(collection, {})
        if str(doc_id) in bucket:
            return False
        bucket[str(doc_id)] = copy.deepcopy(doc)
    return True


def replace_if(collection: str, doc_id: Any, doc: dict[str, Any], *, revision: int) -> bool:
    """Compare-and-swap on ``revision``: replace only the document still at that revision."""
    db = _db()
    if db is not None:
        done = _coll(db, collection).replace_one({"_id": doc_id, "revision": revision}, copy.deepcopy(doc))
        return bool(done.matched_count)
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_path(), _empty()) as data:
        bucket = data.setdefault(collection, {})
        current = bucket.get(str(doc_id))
        if not current or int(current.get("revision") or 0) != revision:
            return False
        bucket[str(doc_id)] = copy.deepcopy(doc)
    return True


def set_fields(collection: str, doc_id: Any, fields: dict[str, Any]) -> None:
    """Set side fields of a stored document (a checkpoint's anchor receipt; never signed content)."""
    db = _db()
    if db is not None:
        _coll(db, collection).update_one({"_id": doc_id}, {"$set": copy.deepcopy(fields)})
        return
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_path(), _empty()) as data:
        current = data.setdefault(collection, {}).get(str(doc_id))
        if current is not None:
            current.update(copy.deepcopy(fields))


def find(collection: str, query: dict[str, Any] | None = None, *, newest_first: bool = True,
         before: int | None = None, limit: int = 0) -> list[dict[str, Any]]:
    """Documents with integer ids matching ``query`` (equality; a list value means "any of")."""
    query = dict(query or {})
    db = _db()
    if db is not None:
        spec: dict[str, Any] = {k: ({"$in": list(v)} if isinstance(v, (list, tuple, set, frozenset)) else v)
                                for k, v in query.items()}
        if before is not None:
            spec["_id"] = {"$lt": int(before)}
        cursor = _coll(db, collection).find(spec).sort("_id", -1 if newest_first else 1)
        if limit:
            cursor = cursor.limit(int(limit))
        return [d for d in (_clean(x) for x in cursor) if d]

    def matches(doc: dict[str, Any]) -> bool:
        return all(doc.get(k) in v if isinstance(v, (list, tuple, set, frozenset)) else doc.get(k) == v
                   for k, v in query.items())

    rows = sorted(((int(k), d) for k, d in _file().get(collection, {}).items()), key=lambda kv: kv[0],
                  reverse=newest_first)
    docs = [copy.deepcopy(d) for k, d in rows if (before is None or k < before) and matches(d)]
    return docs[:limit] if limit else docs


def all_docs(collection: str) -> list[dict[str, Any]]:
    """Every document of a small collection (the keyrings)."""
    db = _db()
    if db is not None:
        return [d for d in (_clean(x) for x in _coll(db, collection).find({})) if d]
    return [copy.deepcopy(d) for d in _file().get(collection, {}).values()]


def find_keyring_of(key_id: str) -> dict[str, Any] | None:
    """The keyring holding ``key_id``."""
    db = _db()
    if db is not None:
        return _clean(_coll(db, KEYS).find_one({"keys.key_id": key_id}))
    for doc in _file().get(KEYS, {}).values():
        if any(k.get("key_id") == key_id for k in doc.get("keys") or []):
            return copy.deepcopy(doc)
    return None


def tree_size() -> int:
    """Leaves in the log: the highest index plus one."""
    db = _db()
    if db is not None:
        last = _coll(db, ENTRIES).find_one({}, projection={"_id": 1}, sort=[("_id", -1)])
        return int(last["_id"]) + 1 if last else 0
    return len(_file().get(ENTRIES, {}))


def latest_checkpoint() -> dict[str, Any] | None:
    rows = find(CHECKPOINTS, limit=1)
    return rows[0] if rows else None


def leaf_hashes(start: int = 0) -> list[str]:
    """Leaf hashes from index ``start`` on, in order. Raises on a gap (a removed entry)."""
    db = _db()
    if db is not None:
        cursor = _coll(db, ENTRIES).find({"_id": {"$gte": int(start)}},
                                         projection={"leaf_hash": 1}).sort("_id", 1)
        rows = [(int(d["_id"]), str(d.get("leaf_hash") or "")) for d in cursor]
    else:
        rows = sorted((int(k), str(d.get("leaf_hash") or "")) for k, d in _file().get(ENTRIES, {}).items()
                      if int(k) >= start)
    for offset, (index, _hash) in enumerate(rows):
        if index != start + offset:
            raise StoreConflict(f"the log has no entry at index {start + offset}")
    return [h for _i, h in rows]


def find_by_dedupe(dedupe: str) -> dict[str, Any] | None:
    db = _db()
    if db is not None:
        return _clean(_coll(db, ENTRIES).find_one({"dedupe": dedupe}))
    data = _file()
    index = data.get(_DEDUPE, {}).get(dedupe)
    doc = data.get(ENTRIES, {}).get(str(index)) if index is not None else None
    return copy.deepcopy(doc) if doc else None


def append_entry(doc: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Append ``doc`` as the next leaf. Returns ``(stored entry, created)``.

    Idempotent on ``doc["dedupe"]``: a second append of the same thing returns
    the entry already there. The index is assigned here, by insert-if-absent.
    """
    dedupe = str(doc["dedupe"])
    db = _db()
    if db is not None:
        from pymongo.errors import DuplicateKeyError

        coll = _coll(db, ENTRIES)
        for _ in range(APPEND_ATTEMPTS):
            existing = coll.find_one({"dedupe": dedupe})
            if existing:
                return _clean(existing) or {}, False
            index = tree_size()
            try:
                coll.insert_one({**copy.deepcopy(doc), "_id": index, "index": index})
            except DuplicateKeyError:
                continue  # another replica took this index (or this dedupe key): look again
            return {**copy.deepcopy(doc), "index": index}, True
        raise StoreConflict("the log is too contended to append to; try again")
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_path(), _empty()) as data:
        bucket = data.setdefault(ENTRIES, {})
        seen = data.setdefault(_DEDUPE, {})
        if dedupe in seen:
            return copy.deepcopy(bucket[str(seen[dedupe])]), False
        index = len(bucket)
        stored = {**copy.deepcopy(doc), "index": index}
        bucket[str(index)] = stored
        seen[dedupe] = index
    return copy.deepcopy(stored), True
