"""Ops review round 2, High 1: duplicates across replicas must be impossible.

The per-tenant fence did not serialize check-then-insert once the fence
existed: a writer advanced the fence first and inserted its record after, so a
competing writer could reload in that gap, see the new fence but no record,
win the next fence swap and insert a second copy (two tenants for one
workspace, two grants for one principal). The invariants are now enforced by
the database: every record family declares unique claims (a partial unique
index on ``(store, uniq)``), so the second insert fails whatever the timing,
and the losing writer re-reads (or gets 409 ``concurrent_update``).
"""

from __future__ import annotations

import json
import threading
import uuid

import pytest

from services import (
    byok_key_manager,
    control_plane_store,
    mongodb_service,
    notification_store,
    resource_acl,
    shape_draft_store,
    shared_doc_store,
    tenant_store,
)
from services.shared_doc_store import SharedDocConflict
from tests._control_plane_fakes import install_shared_store

ITEMS = "control_plane_items"


class _NoLock:
    """Replicas share no process lock."""

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def _pods(monkeypatch, root) -> None:
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(tenant_store, "STORE_PATH", root / "tenants.json")
    monkeypatch.setattr(byok_key_manager, "STORE_PATH", root / "byok_keys.json")
    monkeypatch.setattr(resource_acl, "STORE_PATH", root / "resource_acls.jsonl")
    monkeypatch.setattr(notification_store, "STORE_PATH", root / "notifications.json")
    monkeypatch.setattr(shape_draft_store, "STORE_PATH", root / "shape_drafts.json")
    for prefix in ("DATAFLOW_", "DATAWRAP_"):
        for name in ("RESOURCE_ACL_STORE", "NOTIFICATION_STORE"):
            monkeypatch.delenv(f"{prefix}{name}", raising=False)
    for store in (tenant_store, resource_acl, byok_key_manager):
        monkeypatch.setattr(store._DOC, "_lock", _NoLock())


def _attempt(fn) -> str:
    try:
        fn()
        return "ok"
    except (ValueError, SharedDocConflict) as exc:
        return f"{type(exc).__name__}: {exc}"


def _race_in_the_gap(monkeypatch, db, store: str, other) -> dict:
    """Run ``other`` inside this writer's save, just before its record insert lands.

    That is the gap the round-2 review found: the fence was already advanced,
    the record not yet written.
    """
    items = db[ITEMS]
    real_insert = items.insert_one
    state: dict = {"raced": False}

    def insert_then_race(doc, *args, **kwargs):
        if str(doc.get("_id", "")).startswith(f"{store}:") and not state["raced"]:
            state["raced"] = True
            state["other"] = _attempt(other)
        return real_insert(doc, *args, **kwargs)

    monkeypatch.setattr(items, "insert_one", insert_then_race)
    return state


def test_second_tenant_for_a_workspace_is_impossible_once_the_fence_exists(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    # As in the review's probe: an earlier tenant was created and deleted, so the fence exists.
    tenant_store.delete_tenant(tenant_store.create_tenant("ws-same", "earlier, since deleted").id)
    state = _race_in_the_gap(monkeypatch, db, "tenant_settings", lambda: tenant_store.create_tenant("ws-same", "A"))
    mine = _attempt(lambda: tenant_store.create_tenant("ws-same", "B"))
    assert state["raced"]
    outcomes = [mine, state["other"]]
    assert outcomes.count("ok") == 1, outcomes
    lost = next(o for o in outcomes if o != "ok")
    assert "already has a tenant" in lost or "SharedDocConflict" in lost
    assert [t.workspace_id for t in tenant_store.list_tenants()] == ["ws-same"]


def test_second_grant_for_a_principal_is_impossible_once_the_fence_exists(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    resource_acl.upsert_grant(tenant_id="t", resource_type="connector", resource_id="r", principal="seed@x.io", role="viewer")
    state = _race_in_the_gap(
        monkeypatch, db, "resource_acls",
        lambda: resource_acl.upsert_grant(tenant_id="t", resource_type="connector", resource_id="r", principal="eve@x.io", role="editor"),
    )
    mine = _attempt(
        lambda: resource_acl.upsert_grant(tenant_id="t", resource_type="connector", resource_id="r", principal="eve@x.io", role="viewer")
    )
    assert state["raced"] and mine == "ok" and state["other"] == "ok"
    eve = resource_acl.list_grants(tenant_id="t", resource_type="connector", resource_id="r", principal="eve@x.io")
    assert len(eve) == 1, "one grant per principal and resource"
    assert eve[0].role == "viewer", "the later upsert updated the winner's grant"


def test_a_unique_claim_lost_after_every_retry_is_a_409_conflict(tmp_path, monkeypatch):
    """The losing writer never stores a duplicate; it re-reads, and a claim it cannot see is a conflict."""
    from services.shared_record_store import UniqueConflict

    db = install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    monkeypatch.setattr(shared_doc_store, "RETRY_BUDGET_SECONDS", 0.2)
    tenant_store.create_tenant("ws-hidden", "Winner")
    # The caller's check cannot see the winner (it reads an empty store), so only the index stops it.
    monkeypatch.setattr(tenant_store, "_tenants", lambda data=None: [])
    with pytest.raises(SharedDocConflict) as raised:
        tenant_store.create_tenant("ws-hidden", "Loser")
    assert isinstance(raised.value, UniqueConflict)
    assert len([d for d in db[ITEMS].find({"store": "tenant_settings"})]) == 1


def test_records_written_before_the_claims_existed_are_backfilled_and_duplicates_reported(tmp_path, monkeypatch, caplog):
    """A database that already holds two tenants for one workspace (written by the racy build)."""
    db = install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    for n, name in enumerate(("first", "second")):
        body = {"id": f"t{n}", "workspace_id": "ws-dup", "name": name}
        db[ITEMS].insert_one({"_id": f"tenant_settings:t{n}", "store": "tenant_settings", "key": f"t{n}",
                              "part": "ws-dup", "rev": 1, "seq": n, "body": json.dumps(body)})
    monkeypatch.setattr(tenant_store._DOC, "_backfilled", False, raising=False)
    caplog.set_level("WARNING")
    assert tenant_store.get_tenant_for_workspace("ws-dup").name == "first"
    claimed = [d["_id"] for d in db[ITEMS].find({"store": "tenant_settings"}) if d.get("uniq")]
    assert claimed == ["tenant_settings:t0"], "the earliest record keeps the claim"
    status = tenant_store._DOC.status()
    assert [row["key"] for row in status["duplicate_records"]] == ["t1"]
    assert any("duplicate" in r.getMessage() for r in caplog.records)
    with pytest.raises(ValueError, match="already has a tenant"):
        tenant_store.create_tenant("ws-dup", "third")


@pytest.fixture
def live_mongo(monkeypatch):
    svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    svc.db_name = f"opsfix_r2f1_{uuid.uuid4().hex[:10]}"
    monkeypatch.setattr(mongodb_service, "_mongodb_service", svc)
    monkeypatch.setattr(control_plane_store, "_remember_seen", lambda: None)
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "mongo")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0")
    shared_doc_store.reset_outage_state()
    for store in (tenant_store, resource_acl, byok_key_manager):
        monkeypatch.setattr(store._DOC, "_indexed", False, raising=False)
        monkeypatch.setattr(store._DOC, "_backfilled", False, raising=False)
    try:
        yield svc
    finally:
        svc.client.drop_database(svc.db_name)
        svc.disconnect()


def test_eight_writers_racing_on_live_mongodb_leave_no_duplicate(live_mongo, tmp_path, monkeypatch):
    """The review's probe in one process: 8 writers per round, fences (old build) or claims already present."""
    _pods(monkeypatch, tmp_path)
    resource_acl.upsert_grant(tenant_id="t", resource_type="connector", resource_id="r", principal="seed@x.io", role="viewer")
    tenant_store.delete_tenant(tenant_store.create_tenant("ws-r0", "earlier, since deleted").id)
    outcomes: list[str] = []
    for rnd in range(4):
        go = threading.Barrier(8)

        def writer(i: int, rnd: int = rnd, go: threading.Barrier = go) -> None:
            go.wait()
            outcomes.append(_attempt(lambda: tenant_store.create_tenant(f"ws-r{rnd}", f"T{i}")))
            outcomes.append(_attempt(lambda: resource_acl.upsert_grant(
                tenant_id="t", resource_type="connector", resource_id="r", principal=f"eve{rnd}@x.io",
                role=["viewer", "editor"][i % 2])))

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(120)
    tenants = tenant_store.list_tenants()
    for rnd in range(4):
        assert sum(1 for t in tenants if t.workspace_id == f"ws-r{rnd}") == 1
        assert len(resource_acl.list_grants(tenant_id="t", resource_type="connector", resource_id="r",
                                            principal=f"eve{rnd}@x.io")) == 1
    assert all(o == "ok" or "already has a tenant" in o or "SharedDocConflict" in o for o in outcomes), outcomes
    indexes = live_mongo.get_database()[ITEMS].index_information()
    unique = [spec for spec in indexes.values() if spec.get("unique")]
    assert unique and unique[0]["key"] == [("store", 1), ("uniq", 1)]
    assert unique[0]["partialFilterExpression"] == {"uniq": {"$exists": True}}
