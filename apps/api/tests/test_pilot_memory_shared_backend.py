"""Pilot working memory is shared across replicas when the ack ledger is."""

from __future__ import annotations

import os
import uuid

import pytest

from src.ai.copilot import working_memory as wm
from src.ai.copilot.working_memory import (
    MongoWorkingMemory,
    PendingSlot,
    PilotFocus,
    PilotProposal,
    PilotWorkingMemory,
)


class _FakeColl:
    """The handful of pymongo collection calls the shared memory uses."""

    def __init__(self):
        self.rows: dict[str, dict] = {}

    def find_one(self, flt):
        row = self.rows.get(flt["_id"])
        return dict(row) if row else None

    def replace_one(self, flt, doc, upsert=False):
        self.rows[flt["_id"]] = dict(doc)

    def find_one_and_delete(self, flt):
        return self.rows.pop(flt["_id"], None)

    def delete_many(self, flt):
        cutoff = (flt.get("stamp") or {}).get("$lte")
        for key in [
            k for k, v in self.rows.items()
            if v["kind"] == flt["kind"] and (cutoff is None or v["stamp"] <= cutoff)
        ]:
            self.rows.pop(key)


def _focus() -> PilotFocus:
    return PilotFocus(connector_name="Orders Postgres", table="orders", columns=["id", "amount"])


def test_a_second_replica_sees_focus_pending_and_proposal():
    coll = _FakeColl()
    a, b = MongoWorkingMemory(collection=coll), MongoWorkingMemory(collection=coll)
    a.remember_focus("u|s1", _focus())
    a.remember_pending("u|s1", PendingSlot(tool="run_query", missing="connector"))
    a.remember_proposal("u|s1", PilotProposal(tool="create_transform_recipe", args={"template": "scd2-history"}))
    assert b.get_focus("u|s1").table == "orders"
    assert b.get_pending("u|s1").tool == "run_query"
    assert b.get_proposal("u|s1").args == {"template": "scd2-history"}
    b.clear_proposal("u|s1")
    assert a.get_proposal("u|s1") is None
    assert a.get_focus("u|other") is None


def test_expired_slots_are_not_returned(monkeypatch):
    coll = _FakeColl()
    mem = MongoWorkingMemory(collection=coll, ttl_sec=60)
    mem.remember_focus("u|s2", _focus())
    now = wm._now()
    monkeypatch.setattr(wm, "_now", lambda: now + 61)
    assert mem.get_focus("u|s2") is None


def test_backend_follows_the_ack_ledger(monkeypatch):
    coll = _FakeColl()
    monkeypatch.setattr(wm, "_memory", None)
    monkeypatch.setattr("services.control_plane_store.mongo_collection", lambda name: coll)
    monkeypatch.setenv("DATAFLOW_ACK_BACKEND", "mongo")
    assert isinstance(wm.get_working_memory(), MongoWorkingMemory)
    monkeypatch.setenv("DATAFLOW_ACK_BACKEND", "file")
    mem = wm.get_working_memory()
    assert isinstance(mem, PilotWorkingMemory) and not isinstance(mem, MongoWorkingMemory)


def test_no_shared_collection_refuses_a_pod_local_fallback(monkeypatch):
    monkeypatch.setattr("services.control_plane_store.mongo_collection", lambda name: None)
    with pytest.raises(RuntimeError, match="pilot_memory"):
        MongoWorkingMemory()


def test_real_mongo_round_trip():
    pymongo = pytest.importorskip("pymongo")
    uri = os.getenv("MONGODB_URI") or "mongodb://localhost:27017"
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=1500)
    try:
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001 - no local Mongo is a skip, not a failure
        pytest.skip(f"Mongo not reachable: {exc}")
    coll = client["dataflow_test"][f"pilot_memory_{uuid.uuid4().hex[:8]}"]
    try:
        a, b = MongoWorkingMemory(collection=coll), MongoWorkingMemory(collection=coll)
        a.remember_focus("u|s3", _focus())
        assert b.get_focus("u|s3").columns == ["id", "amount"]
        b.update_focus("u|s3", table="customers")
        assert a.get_focus("u|s3").table == "customers"
    finally:
        coll.drop()
