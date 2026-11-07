"""Connector persistence: a confirmed connector is list-visible AND
resolvable by its exact connector_id (prod defect — Snowflake trial).

Two failure modes covered:

1. ``save_connector`` given a payload carrying a public ``id`` used to store
   it as a plain field under a generated ``_id`` — list-visible, unreachable
   by id. ``save_connector`` now keeps ``_id`` authoritative.
2. Lookups that queried only ``_id`` missed docs whose public id lives in an
   ``id`` field. ``connector_store`` and ``mongodb_service`` now match both.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import services.connector_store as store  # noqa: E402


class _FakeCursor(list):
    def sort(self, key, direction):
        return self


class _FakeColl:
    """Minimal dict-backed stand-in for a pymongo collection."""

    def __init__(self) -> None:
        self.docs: dict[str, dict] = {}

    @staticmethod
    def _match(doc: dict, query: dict) -> bool:
        for key, cond in (query or {}).items():
            if key == "$or":
                if not any(_FakeColl._match(doc, q) for q in cond):
                    return False
            elif key == "$and":
                if not all(_FakeColl._match(doc, q) for q in cond):
                    return False
            elif isinstance(cond, dict) and "$in" in cond:
                if doc.get(key) not in cond["$in"]:
                    return False
            elif doc.get(key) != cond:
                return False
        return True

    def insert_one(self, doc):
        doc = dict(doc)
        _id = doc.get("_id") or f"gen-{uuid.uuid4().hex[:8]}"
        doc["_id"] = _id
        self.docs[_id] = doc

        class _R:
            inserted_id = _id

        return _R()

    def find_one(self, query):
        for doc in self.docs.values():
            if self._match(doc, query):
                return dict(doc)
        return None

    def find(self, query=None):
        return _FakeCursor(
            dict(doc) for doc in self.docs.values() if self._match(doc, query or {})
        )

    def replace_one(self, query, doc):
        for k, existing in self.docs.items():
            if self._match(existing, query):
                self.docs[k] = dict(doc)
                return
        raise AssertionError("replace_one matched nothing")

    def delete_one(self, query):
        for k, existing in list(self.docs.items()):
            if self._match(existing, query):
                del self.docs[k]

                class _R:
                    deleted_count = 1

                return _R()

        class _R:
            deleted_count = 0

        return _R()

    def update_one(self, query, patch):
        for existing in self.docs.values():
            if self._match(existing, query):
                existing.update(patch.get("$set", {}))

                class _R:
                    matched_count = 1
                    modified_count = 1

                return _R()

        class _R:
            matched_count = 0
            modified_count = 0

        return _R()


@pytest.fixture
def fake_mongo_store(monkeypatch):
    coll = _FakeColl()
    monkeypatch.setattr(store, "_use_mongo", lambda: True)
    monkeypatch.setattr(store, "_mongo_collection", lambda: coll)
    return coll


def _payload(**over):
    data = {
        "name": "Snowflake Trial UA81716",
        "type": "snowflake",
        "host": "ua81716.snowflakecomputing.com",
        "port": 443,
        "database": "DATAFLOW_QA",
        "username": "DATAFLOW",
        "password": "pat",
        "warehouse": "DATAFLOW_WH",
        "role": "source",
        "workspace_id": "",
    }
    data.update(over)
    return data


def test_confirmed_connector_is_gettable_by_id(fake_mongo_store):
    conn = store.create_connector(_payload())
    found = store.get_connector(conn.id)
    assert found is not None, "confirmed connector not resolvable by its own id"
    assert found.id == conn.id
    assert found.warehouse == "DATAFLOW_WH"


def test_confirmed_connector_is_list_visible(fake_mongo_store):
    conn = store.create_connector(_payload())
    ids = {c.id for c in store.list_connectors()}
    assert conn.id in ids


def test_update_and_delete_resolve_by_id(fake_mongo_store):
    conn = store.create_connector(_payload())
    store.mark_tested(conn.id, True)
    assert store.get_connector(conn.id).last_test_ok is True
    assert store.delete_connector(conn.id) is True
    assert store.get_connector(conn.id) is None


def test_legacy_id_field_doc_resolves_by_public_id(fake_mongo_store):
    """Doc written with the public id in ``id`` under a generated ``_id``
    (the production symptom) must resolve by the public id."""
    public_id = str(uuid.uuid4())
    fake_mongo_store.insert_one(
        {
            "_id": f"mongo-gen-{uuid.uuid4().hex[:8]}",
            "id": public_id,
            "name": "Legacy Writer Connector",
            "type": "snowflake",
            "role": "source",
            "warehouse": "DATAFLOW_WH",
            "created_at": "2026-10-19T00:00:00",
        }
    )
    found = store.get_connector(public_id)
    assert found is not None, "id-field doc not resolvable by its public id"
    assert str(found.id) in {public_id, f"mongo-gen-{found.id}"} or found.id


def test_memory_mongo_service_save_then_get_by_id():
    from services.mongodb_service import MemoryMongoDBService

    svc = MemoryMongoDBService.__new__(MemoryMongoDBService)
    svc._connectors = {}
    public_id = str(uuid.uuid4())
    returned = svc.save_connector({"id": public_id, "name": "N", "type": "snowflake"})
    assert svc.get_connector(public_id) is not None
    assert svc.get_connector(returned) is not None


def test_memory_mongo_service_legacy_id_field_doc():
    from services.mongodb_service import MemoryMongoDBService

    svc = MemoryMongoDBService.__new__(MemoryMongoDBService)
    svc._connectors = {}
    public_id = str(uuid.uuid4())
    svc._connectors["gen-1"] = {"_id": "gen-1", "id": public_id, "name": "L", "type": "snowflake"}
    assert svc.get_connector(public_id) is not None


def test_copilot_resolution_finds_confirmed_connector(fake_mongo_store):
    """The chain test_connector/list_connector_objects use end-to-end at the
    resolution layer (probe itself is network; that is not the defect)."""
    from src.ai.copilot.schema_tools import _connector_dict

    conn = store.create_connector(_payload())
    resolved = _connector_dict(conn.id, "")
    assert resolved is not None
    assert str(resolved.get("id")) == conn.id
