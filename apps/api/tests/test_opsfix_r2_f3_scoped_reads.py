"""Ops review round 2, High 3: a lookup must not load every tenant's records.

Every read of a record-list store fetched and parsed the whole store
(``find({"store": ...})``) whatever it looked for, so the cost of a tenant
lookup, an ACL check or a draft read grew with the number of tenants, and one
large workspace slowed every other one. Lookups now query only the records
they need (by partition, record id or an indexed lookup tag), the read cache
is kept per query, and the deployment-wide lists are paginated.
"""

from __future__ import annotations

import json
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
N = 300


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


def _seed(items, n: int = N) -> None:
    """Records as an earlier build of this feature wrote them (no claims, no lookup tags)."""
    for i in range(n):
        tenant = {"id": f"t-{i}", "workspace_id": f"ws-{i}", "name": f"T{i}", "custom_domain": f"d{i}.example.com",
                  "created_at": f"2026-01-01T00:00:{i % 60:02d}.{i:06d}"}
        items.insert_one({"_id": f"tenant_settings:t-{i}", "store": "tenant_settings", "key": f"t-{i}",
                          "part": f"ws-{i}", "rev": 1, "seq": i, "body": json.dumps(tenant)})
        grant = {"id": f"g-{i}", "tenant_id": "t", "resource_type": "job", "resource_id": f"r{i}",
                 "principal": f"p{i}@x.io", "role": "viewer"}
        items.insert_one({"_id": f"resource_acls:g-{i}", "store": "resource_acls", "key": f"g-{i}", "part": "t",
                          "rev": 1, "seq": i, "body": json.dumps(grant)})


def test_lookups_read_only_the_records_they_need(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    items = db[ITEMS]
    _seed(items)
    # First touch per store: indexes and the one-time backfill of claims and lookup tags.
    tenant_store.get_tenant_for_workspace("ws-0")
    resource_acl.list_grants(tenant_id="t", resource_type="job", resource_id="r0")
    shape_draft_store.load_shape_draft("x", workspace_id="ws-0")

    returned = {"n": 0}
    real_find = items.find

    def counting_find(*args, **kwargs):
        cursor = real_find(*args, **kwargs)
        rows = list(cursor)
        returned["n"] += len(rows)
        return type(cursor)(rows)

    monkeypatch.setattr(items, "find", counting_find)
    assert tenant_store.get_tenant_for_workspace("ws-7").name == "T7"
    assert tenant_store.get_tenant("t-9").workspace_id == "ws-9"
    assert tenant_store.get_tenant_by_domain("d5.example.com").id == "t-5"
    assert {t.id for t in tenant_store.tenants_for_workspaces({"ws-3", "ws-4"})} == {"t-3", "t-4"}
    assert resource_acl.principal_role_for_resource(
        tenant_id="t", resource_type="job", resource_id="r5", principal="p5@x.io") == "viewer"
    assert resource_acl.principal_role_for_resource(
        tenant_id="t", resource_type="job", resource_id="r5", principal="mallory@x.io") is None
    assert shape_draft_store.load_shape_draft("x", workspace_id="ws-other") is None
    assert returned["n"] <= 12, f"{returned['n']} records read for 7 lookups among {2 * N} records"


def test_writes_inside_one_tenant_read_only_that_tenant(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    items = db[ITEMS]
    _seed(items)
    tenant_store.get_tenant_for_workspace("ws-0")
    resource_acl.list_grants(tenant_id="t")
    returned = {"n": 0}
    real_find = items.find

    def counting_find(*args, **kwargs):
        cursor = real_find(*args, **kwargs)
        rows = list(cursor)
        returned["n"] += len(rows)
        return type(cursor)(rows)

    monkeypatch.setattr(items, "find", counting_find)
    tenant_store.update_tenant("t-11", sso_enforced=True)
    tenant_store.create_tenant("ws-new", "New")
    resource_acl.upsert_grant(tenant_id="t", resource_type="job", resource_id="r8", principal="q@x.io", role="editor")
    assert returned["n"] <= 12, f"{returned['n']} records read for three writes"
    assert tenant_store.get_tenant("t-11").sso_enforced is True
    assert len(resource_acl.list_grants(tenant_id="t", resource_type="job", resource_id="r8")) == 2


def test_the_read_cache_is_per_query(tmp_path, monkeypatch):
    """A cached answer covers its own query only: it is not the whole store, served for every lookup."""
    db = install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    items = db[ITEMS]
    _seed(items, n=20)
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "2")
    assert tenant_store.get_tenant_for_workspace("ws-1").name == "T1"
    reads: list[int] = []
    real_find = items.find

    def counting_find(*args, **kwargs):
        cursor = real_find(*args, **kwargs)
        rows = list(cursor)
        reads.append(len(rows))
        return type(cursor)(rows)

    monkeypatch.setattr(items, "find", counting_find)
    assert tenant_store.get_tenant_for_workspace("ws-1").name == "T1"
    assert reads == [], "the same query is answered from the cache"
    assert tenant_store.get_tenant_for_workspace("ws-2").name == "T2"
    assert reads == [1], "another workspace is its own query: one record read"


def test_the_tenant_list_is_paginated(tmp_path, monkeypatch):
    install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    for i in range(5):
        tenant_store.create_tenant(f"ws-{i}", f"T{i}")
    first, after = tenant_store.list_tenants_page(limit=2)
    second, after2 = tenant_store.list_tenants_page(limit=2, offset=after)
    third, after3 = tenant_store.list_tenants_page(limit=2, offset=after2)
    names = [t.name for t in first + second + third]
    assert names == ["T4", "T3", "T2", "T1", "T0"], "newest first, each tenant once"
    assert after == 2 and after2 == 4 and after3 is None


def test_the_tenants_route_returns_one_page(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.routers import workspace_router

    install_shared_store(monkeypatch)
    _pods(monkeypatch, tmp_path)
    for i in range(3):
        tenant_store.create_tenant(f"ws-{i}", f"T{i}")
    monkeypatch.setattr(workspace_router, "_is_platform_admin", lambda _request: True)
    app = FastAPI()
    app.include_router(workspace_router.router, prefix="/api/v1")
    with TestClient(app) as client:
        page = client.get("/api/v1/workspace/tenants", params={"limit": 2}).json()
        assert [t["name"] for t in page["tenants"]] == ["T2", "T1"] and page["next_offset"] == 2
        rest = client.get("/api/v1/workspace/tenants", params={"limit": 2, "offset": 2}).json()
        assert [t["name"] for t in rest["tenants"]] == ["T0"] and rest["next_offset"] is None
        assert client.get("/api/v1/workspace/tenants", params={"limit": 100000}).status_code == 422


@pytest.fixture
def live(monkeypatch, tmp_path):
    svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    svc.db_name = f"opsfix_r2f3_{uuid.uuid4().hex[:10]}"
    monkeypatch.setattr(mongodb_service, "_mongodb_service", svc)
    monkeypatch.setattr(control_plane_store, "_remember_seen", lambda: None)
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "mongo")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_TIMEOUT_SECONDS", "5")  # the one-time backfill of 2000 records
    shared_doc_store.reset_outage_state()
    for store in (tenant_store, resource_acl, shape_draft_store):
        monkeypatch.setattr(store._DOC, "_indexed", None, raising=False)
        monkeypatch.setattr(store._DOC, "_backfilled", None, raising=False)
    _pods(monkeypatch, tmp_path)
    try:
        yield svc
    finally:
        shared_doc_store.reset_outage_state()
        svc.client.drop_database(svc.db_name)
        svc.disconnect()


def test_documents_examined_per_lookup_on_live_mongodb(live):
    """The database's own count: a lookup examines a handful of documents, not the store."""
    import time

    db = live.get_database()
    _seed(db[ITEMS], n=1000)
    tenant_store.get_tenant_for_workspace("ws-0")
    resource_acl.list_grants(tenant_id="t", resource_type="job", resource_id="r0")
    db.command("profile", 2)
    try:
        started = time.time()
        tenant_store.get_tenant_for_workspace("ws-123")
        tenant_store.get_tenant_by_domain("d456.example.com")
        tenant_store.get_tenant("t-789")
        resource_acl.principal_role_for_resource(tenant_id="t", resource_type="job", resource_id="r42", principal="p42@x.io")
    finally:
        db.command("profile", 0)
    rows = [r for r in db["system.profile"].find({"ns": f"{live.db_name}.{ITEMS}"}) if r["ts"].timestamp() >= started - 1]
    examined = [r.get("docsExamined", 0) for r in rows if r.get("op") == "query"]
    assert examined and max(examined) <= 3, examined


def test_a_slow_query_fails_that_read_without_failing_every_store(monkeypatch):
    """The server answered (it ran out of time on the query): the database is not unreachable."""
    from pymongo.errors import ExecutionTimeout, NetworkTimeout

    from services import control_plane_bounds
    from services.control_plane_store import ControlPlaneUnavailable

    class _Coll:
        def find_one(self, *_args, **_kwargs):
            raise ExecutionTimeout("operation exceeded time limit", 50)

        def count_documents(self, *_args, **_kwargs):
            raise NetworkTimeout("127.0.0.1:27017: timed out")

    control_plane_bounds.reset_outage_state()
    coll = control_plane_bounds.bounded(_Coll())
    with pytest.raises(ControlPlaneUnavailable):
        coll.find_one({"store": "shape_drafts"})
    assert control_plane_bounds.outage_retry_at() == 0.0, "one slow query made every store fail for 2 s"
    with pytest.raises(ControlPlaneUnavailable):
        coll.count_documents({})
    assert control_plane_bounds.outage_retry_at() > 0.0, "an unreachable database is still remembered"
    control_plane_bounds.reset_outage_state()
