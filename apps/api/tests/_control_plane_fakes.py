"""A small in-memory stand-in for the two shared control-plane collections.

Implements only what ``services.shared_doc_store`` calls, with the same
duplicate-key, filtered-update and 16 MB document-size behaviour as MongoDB,
so compare-and-swap tests run on a runner with no database.
"""

from __future__ import annotations

import copy
import threading
from typing import Any

import bson
from pymongo.errors import DocumentTooLarge, DuplicateKeyError, WriteError

#: MongoDB refuses a document larger than this (BSON size).
MAX_BSON_SIZE = 16 * 1024 * 1024


def _check_size(doc: dict[str, Any], *, update: bool) -> None:
    size = len(bson.encode(doc))
    if size <= MAX_BSON_SIZE:
        return
    if update:
        raise WriteError(f"Resulting document after update is larger than {MAX_BSON_SIZE}", 17419)
    raise DocumentTooLarge(f"BSON document too large ({size} bytes)")


def _matches(doc: dict[str, Any], flt: dict[str, Any]) -> bool:
    for key, want in flt.items():
        have = doc.get(key)
        if isinstance(want, dict) and any(k.startswith("$") for k in want):
            for op, arg in want.items():
                if op == "$lte" and not (have is not None and have <= arg):
                    return False
                if op == "$lt" and not (have is not None and have < arg):
                    return False
                if op == "$exists" and (key in doc) != bool(arg):
                    return False
        elif have != want:
            return False
    return True


class _Result:
    def __init__(self, matched: int = 0, upserted_id: Any = None, deleted: int = 0) -> None:
        self.matched_count = matched
        self.upserted_id = upserted_id
        self.deleted_count = deleted


class _Cursor:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = docs

    def sort(self, spec: Any) -> "_Cursor":
        for key, direction in reversed(list(spec)):
            self._docs.sort(key=lambda d: str(d.get(key)) if key == "_id" else d.get(key), reverse=direction < 0)
        return self

    def skip(self, n: int) -> "_Cursor":
        self._docs = self._docs[n:]
        return self

    def limit(self, n: int) -> "_Cursor":
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        return iter(copy.deepcopy(self._docs))


class FakeCollection:
    def __init__(self, database: "FakeDatabase", name: str) -> None:
        self.database = database
        self.name = name
        self._docs: dict[Any, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._auto = 0
        self.calls = 0

    def create_index(self, *_a: Any, **_k: Any) -> None:
        return None

    def find_one(self, flt: dict[str, Any]) -> dict[str, Any] | None:
        self.calls += 1
        with self._lock:
            for doc in self._docs.values():
                if _matches(doc, flt):
                    return copy.deepcopy(doc)
        return None

    def find(self, flt: dict[str, Any] | None = None) -> _Cursor:
        self.calls += 1
        with self._lock:
            return _Cursor([copy.deepcopy(d) for d in self._docs.values() if _matches(d, flt or {})])

    def count_documents(self, flt: dict[str, Any]) -> int:
        with self._lock:
            return sum(1 for d in self._docs.values() if _matches(d, flt))

    def insert_one(self, doc: dict[str, Any]) -> _Result:
        self.calls += 1
        with self._lock:
            doc = copy.deepcopy(doc)
            if "_id" not in doc:
                self._auto += 1
                doc["_id"] = f"auto-{self._auto:012d}"
            if doc["_id"] in self._docs:
                raise DuplicateKeyError(f"duplicate _id {doc['_id']!r}")
            _check_size(doc, update=False)
            self._docs[doc["_id"]] = doc
        return _Result(upserted_id=doc["_id"])

    def update_one(self, flt: dict[str, Any], update: dict[str, Any], upsert: bool = False) -> _Result:
        self.calls += 1
        with self._lock:
            found = next((d for d in self._docs.values() if _matches(d, flt)), None)
            created = None
            if found is None:
                if not upsert:
                    return _Result(0)
                found = {k: v for k, v in flt.items() if not isinstance(v, dict)}
                created = found["_id"]
                if created in self._docs:  # the filter missed on another field: a duplicate _id
                    raise DuplicateKeyError(f"duplicate _id {created!r}")
            target = copy.deepcopy(found)
            if created is not None:
                for key, value in (update.get("$setOnInsert") or {}).items():
                    target[key] = copy.deepcopy(value)
            for key, value in (update.get("$set") or {}).items():
                target[key] = copy.deepcopy(value)
            for key in (update.get("$unset") or {}):
                target.pop(key, None)
            for key, value in (update.get("$inc") or {}).items():
                target[key] = int(target.get(key) or 0) + value
            for key, value in (update.get("$addToSet") or {}).items():
                target.setdefault(key, [])
                if value not in target[key]:
                    target[key].append(value)
            _check_size(target, update=True)
            self._docs[target["_id"]] = target
            return _Result(0 if created else 1, created)

    def delete_one(self, flt: dict[str, Any]) -> _Result:
        self.calls += 1
        with self._lock:
            for key, doc in self._docs.items():
                if _matches(doc, flt):
                    del self._docs[key]
                    return _Result(deleted=1)
        return _Result(deleted=0)

    def delete_many(self, flt: dict[str, Any]) -> None:
        with self._lock:
            for key in [k for k, d in self._docs.items() if _matches(d, flt)]:
                del self._docs[key]


class FakeDatabase:
    def __init__(self) -> None:
        self._collections: dict[str, FakeCollection] = {}

    def __getitem__(self, name: str) -> FakeCollection:
        if name not in self._collections:
            self._collections[name] = FakeCollection(self, name)
        return self._collections[name]


def install_shared_store(monkeypatch) -> FakeDatabase:
    """Point ``services.shared_doc_store`` at one in-memory database (two "pods" share it)."""
    from services import shared_doc_store

    db = FakeDatabase()
    monkeypatch.setattr(shared_doc_store, "_collection", lambda name: db[name])
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0")
    monkeypatch.delenv("DATAWRAP_CONTROL_STORE_CACHE_SECONDS", raising=False)
    return db
