"""Ops review High 3: one tenant must not be able to fill a shared document for all.

Each record-list control-plane store (tenants, BYOK key records, ACL grants,
notification channels, shape drafts) was one MongoDB document of at most
16 MB holding every tenant's records, so one workspace saving large canvas
layouts stopped every other workspace's saves. Records now live one document
per record, keyed by store and record id, written by compare-and-swap on the
record (and a small per-tenant fence, so a check-then-insert inside one
tenant stays serialized). Client-supplied sizes are capped.
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
from tests._control_plane_fakes import install_shared_store

ITEMS = "control_plane_items"
RECIPE = {"steps": []}
CANVAS_CAP = getattr(shape_draft_store, "MAX_CANVAS_LAYOUT_BYTES", 256 * 1024)


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


def _layout(size: int) -> dict:
    return {"nodes": "x" * size}


def test_one_workspace_filling_its_draft_quota_cannot_block_another(tmp_path, monkeypatch):
    """p4: the shared database enforces MongoDB's 16 MB document limit."""
    db = install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    near_cap = CANVAS_CAP - 1024
    for ws in ("ws-A", "ws-A2"):
        for n in range(shape_draft_store.MAX_DRAFTS):
            shape_draft_store.save_shape_draft(
                route_key=f"r{n}->d", recipe=RECIPE, canvas_layout=_layout(near_cap), workspace_id=ws
            )
    saved = shape_draft_store.save_shape_draft(
        route_key="c->d", recipe=RECIPE, canvas_layout=_layout(4000), workspace_id="ws-B"
    )
    assert saved["workspace_id"] == "ws-B"
    assert shape_draft_store.load_shape_draft("c->d", workspace_id="ws-B") is not None
    drafts = [d for d in db[ITEMS].find({"store": "shape_drafts"}) if "fence" not in d]
    assert len(drafts) == 2 * shape_draft_store.MAX_DRAFTS + 1, "one document per draft"
    assert max(len(d["body"]) for d in drafts) < 2 * CANVAS_CAP


def test_oversized_canvas_layout_is_refused_with_a_clear_413(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.routers import shape_router

    install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    monkeypatch.setattr(shape_router, "resolve_write_workspace", lambda _request, _ws: "ws-A")
    app = FastAPI()
    app.include_router(shape_router.router, prefix="/api/v1")
    too_big = _layout(CANVAS_CAP + 1)
    with TestClient(app) as client:
        response = client.put("/api/v1/shape/drafts", json={"route_key": "a->b", "recipe": RECIPE, "canvas_layout": too_big})
        assert response.status_code == 413
        assert "canvas layout" in response.json()["detail"].lower() and "KB" in response.json()["detail"]
        ok = client.put("/api/v1/shape/drafts", json={"route_key": "a->b", "recipe": RECIPE, "canvas_layout": _layout(100)})
        assert ok.status_code == 200


def test_a_record_over_the_record_limit_is_refused_before_any_write(tmp_path, monkeypatch):
    from services.shared_record_store import MAX_RECORD_BYTES, RecordTooLarge

    db = install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    with pytest.raises(RecordTooLarge):
        resource_acl.upsert_grant(
            tenant_id="t", resource_type="job", resource_id="j" * (MAX_RECORD_BYTES + 1), principal="a@x.io", role="viewer"
        )
    assert list(db[ITEMS].find({"store": "resource_acls"})) == []


def test_writes_to_different_tenants_never_conflict(tmp_path, monkeypatch):
    """A write that lands between another writer's read and write, for another tenant, is no conflict."""
    install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    resource_acl.upsert_grant(tenant_id="t1", resource_type="job", resource_id="j", principal="a@x.io", role="viewer")
    stale = resource_acl._DOC.load()  # this replica's read
    resource_acl.upsert_grant(tenant_id="t2", resource_type="job", resource_id="j", principal="b@x.io", role="viewer")
    stale["grants"][0]["role"] = "owner"
    resource_acl._DOC.save(stale)  # no SharedDocConflict: the other write was another tenant's record
    roles = {g.principal: g.role for t in ("t1", "t2") for g in resource_acl.list_grants(tenant_id=t)}
    assert roles == {"a@x.io": "owner", "b@x.io": "viewer"}


def test_same_record_stale_write_is_still_refused(tmp_path, monkeypatch):
    install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    grant = resource_acl.upsert_grant(tenant_id="t1", resource_type="job", resource_id="j", principal="a@x.io", role="viewer")
    first, second = resource_acl._DOC.load(), resource_acl._DOC.load()
    first["grants"][0]["role"] = "editor"
    resource_acl._DOC.save(first)
    second["grants"][0]["role"] = "owner"
    with pytest.raises(shared_doc_store.SharedDocConflict):
        resource_acl._DOC.save(second)
    assert [g.role for g in resource_acl.list_grants(tenant_id="t1") if g.id == grant.id] == ["editor"]


class _NoLock:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def test_check_then_insert_inside_one_tenant_stays_serialized(tmp_path, monkeypatch):
    """Two replicas creating a tenant for the same workspace: exactly one wins."""
    install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    monkeypatch.setattr(tenant_store._DOC, "_lock", _NoLock())  # replicas share no process lock
    real_validate = tenant_store._validate_name
    raced = threading.Event()
    other: dict = {}

    def validate_then_race(name):
        if not raced.is_set():
            raced.set()

            def replica_b():
                try:
                    other["tenant"] = tenant_store.create_tenant("ws-same", "B")
                except ValueError as exc:
                    other["error"] = exc

            worker = threading.Thread(target=replica_b)
            worker.start()
            worker.join(10)
        return real_validate(name)

    monkeypatch.setattr(tenant_store, "_validate_name", validate_then_race)
    with pytest.raises(ValueError, match="already has a tenant"):
        tenant_store.create_tenant("ws-same", "A")
    assert other.get("tenant") is not None
    assert [t.workspace_id for t in tenant_store.list_tenants()] == ["ws-same"]


def test_whole_store_document_from_an_earlier_build_is_split_once(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    grants = [
        {"id": f"g{n}", "tenant_id": "t", "resource_type": "job", "resource_id": "j", "principal": f"p{n}@x.io",
         "role": "viewer", "created_at": "2026-01-01T00:00:00+00:00", "created_by": "system"}
        for n in range(3)
    ]
    db[shared_doc_store.DOCS_COLLECTION].insert_one(
        {"_id": "resource_acls", "rev": 7, "body": json.dumps({"grants": grants}), "import_sources": ["abc"]}
    )
    resource_acl._DOC._split_checked = False  # a process starting on this database
    assert sorted(g.id for g in resource_acl.list_grants(tenant_id="t")) == ["g0", "g1", "g2"]
    meta = db[shared_doc_store.DOCS_COLLECTION].find_one({"_id": "resource_acls"})
    assert "body" not in meta and meta["import_sources"] == ["abc"]
    items = sorted(d["_id"] for d in db[ITEMS].find({"store": "resource_acls"}) if "fence" not in d)
    assert items == ["resource_acls:g0", "resource_acls:g1", "resource_acls:g2"]

    # Idempotent: a second process splitting again (or a crashed split re-run) changes nothing.
    resource_acl._DOC._split_checked = False
    assert resource_acl.revoke_grant("g1", tenant_id="t") is True
    resource_acl._DOC._split_checked = False
    assert sorted(g.id for g in resource_acl.list_grants(tenant_id="t")) == ["g0", "g2"]


@pytest.fixture
def live_mongo(monkeypatch):
    svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    svc.db_name = f"opsfix_h3_{uuid.uuid4().hex[:10]}"
    monkeypatch.setattr(mongodb_service, "_mongodb_service", svc)
    monkeypatch.setattr(control_plane_store, "_remember_seen", lambda: None)
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "mongo")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0")
    shared_doc_store.reset_outage_state()
    try:
        yield svc
    finally:
        svc.client.drop_database(svc.db_name)
        svc.disconnect()


def test_every_record_list_store_is_one_document_per_record_on_live_mongodb(live_mongo, tmp_path, monkeypatch):
    _pods(monkeypatch, tmp_path)
    tenant = tenant_store.create_tenant("ws-live", "Live", ip_allowlist=["203.0.113.9/32"])
    key = byok_key_manager.create_key(tenant.id, "primary")
    grant = resource_acl.upsert_grant(tenant_id="ws-live", resource_type="job", resource_id="j", principal="a@x.io", role="viewer")
    channel = notification_store.create_channel(label="ops", kind="webhook", workspace_id="ws-live", config={"url": "https://hooks.example.com/x"})
    shape_draft_store.save_shape_draft(route_key="a->b", recipe=RECIPE, workspace_id="ws-live")
    items = live_mongo.get_database()[ITEMS]
    for store, record_id in (
        ("tenant_settings", tenant.id), ("byok_keys", key.id), ("resource_acls", grant.id),
        ("notification_channels", channel.id), ("shape_drafts", "ws-live::a->b"),
    ):
        doc = items.find_one({"_id": f"{store}:{record_id}"})
        assert doc is not None and doc["store"] == store and doc["rev"] == 1, store
    docs = live_mongo.get_database()[shared_doc_store.DOCS_COLLECTION]
    assert docs.count_documents({"body": {"$exists": True}}) == 0, "no store is one whole document any more"
    tenant_store.update_tenant(tenant.id, sso_enforced=True)
    assert items.find_one({"_id": f"tenant_settings:{tenant.id}"})["rev"] == 2
    assert tenant_store.get_tenant(tenant.id).sso_enforced is True
