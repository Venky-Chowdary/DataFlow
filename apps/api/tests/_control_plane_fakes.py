"""A small in-memory stand-in for the two shared control-plane collections.

Implements only what ``services.shared_doc_store`` calls, with the same
duplicate-key and filtered-update behaviour as MongoDB, so compare-and-swap
tests run on a runner with no database.
"""

from __future__ import annotations

import copy
import threading
from typing import Any

from pymongo.errors import DuplicateKeyError


def _matches(doc: dict[str, Any], flt: dict[str, Any]) -> bool:
    for key, want in flt.items():
        have = doc.get(key)
        if isinstance(want, dict) and any(k.startswith("$") for k in want):
            for op, arg in want.items():
                if op == "$lte" and not (have is not None and have <= arg):
                    return False
                if op == "$lt" and not (have is not None and have < arg):
                    return False
        elif have != want:
            return False
    return True


class _Result:
    def __init__(self, matched: int = 0, upserted_id: Any = None) -> None:
        self.matched_count = matched
        self.upserted_id = upserted_id


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
            self._docs[doc["_id"]] = doc
        return _Result(upserted_id=doc["_id"])

    def update_one(self, flt: dict[str, Any], update: dict[str, Any], upsert: bool = False) -> _Result:
        self.calls += 1
        with self._lock:
            target = next((d for d in self._docs.values() if _matches(d, flt)), None)
            created = None
            if target is None:
                if not upsert:
                    return _Result(0)
                target = {k: v for k, v in flt.items() if not isinstance(v, dict)}
                self._docs[target["_id"]] = target
                created = target["_id"]
            for key, value in (update.get("$set") or {}).items():
                target[key] = copy.deepcopy(value)
            for key, value in (update.get("$inc") or {}).items():
                target[key] = int(target.get(key) or 0) + value
            for key, value in (update.get("$addToSet") or {}).items():
                target.setdefault(key, [])
                if value not in target[key]:
                    target[key].append(value)
            return _Result(0 if created else 1, created)

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
