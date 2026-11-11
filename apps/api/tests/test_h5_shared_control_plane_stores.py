"""Audit H-5: control-plane stores must agree across API replicas.

Two "pods" here are two data directories that share one control-plane
database (an in-memory stand-in with MongoDB's duplicate-key and
filtered-update behaviour, plus a live-MongoDB compare-and-swap test).
"""

from __future__ import annotations

import json
import threading
import uuid
from pathlib import Path

import pytest

from services import byok_key_manager, resource_acl, shared_doc_store, tenant_store
from services.control_plane_store import ControlPlaneUnavailable
from services.process_role import topology_errors
from services.shared_doc_store import SharedDoc, SharedDocConflict, SharedLog
from tests._control_plane_fakes import FakeDatabase, install_shared_store


def _on_pod(monkeypatch, root: Path) -> None:
    """Switch this process to another pod's data directory (its own files, empty caches)."""
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(tenant_store, "STORE_PATH", root / "tenants.json")
    monkeypatch.setattr(byok_key_manager, "STORE_PATH", root / "byok_keys.json")
    monkeypatch.setattr(resource_acl, "STORE_PATH", root / "resource_acls.jsonl")
    monkeypatch.delenv("RESOURCE_ACL_STORE", raising=False)
    monkeypatch.delenv("DATAFLOW_RESOURCE_ACL_STORE", raising=False)
    monkeypatch.delenv("DATAWRAP_RESOURCE_ACL_STORE", raising=False)


# ---------------------------------------------------------------------------
# The security-relevant stores are the same on every replica
# ---------------------------------------------------------------------------


def test_tenant_ip_allowlist_and_require_sso_saved_on_one_pod_apply_on_another(tmp_path, monkeypatch):
    install_shared_store(monkeypatch)
    _on_pod(monkeypatch, tmp_path / "pod-a")
    tenant = tenant_store.create_tenant("ws-h5", "Acme", ip_allowlist=["203.0.113.0/24"], sso_enforced=True)

    _on_pod(monkeypatch, tmp_path / "pod-b")
    seen = tenant_store.get_tenant_for_workspace("ws-h5")
    assert seen is not None, "pod B does not know the tenant pod A saved"
    assert seen.id == tenant.id
    assert seen.ip_allowlist == ["203.0.113.0/24"]
    assert seen.sso_enforced is True
    assert tenant_store.is_request_ip_allowed("198.51.100.7", seen) is False

    tenant_store.update_tenant(tenant.id, ip_allowlist=[])
    _on_pod(monkeypatch, tmp_path / "pod-a")
    assert tenant_store.get_tenant(tenant.id).ip_allowlist == []


def test_byok_key_created_on_one_pod_decrypts_on_another(tmp_path, monkeypatch):
    install_shared_store(monkeypatch)
    _on_pod(monkeypatch, tmp_path / "pod-a")
    key = byok_key_manager.create_key("tenant-h5", "primary")
    token = byok_key_manager.tenant_encrypt("tenant-h5", "s3cr3t-value")

    _on_pod(monkeypatch, tmp_path / "pod-b")
    active = byok_key_manager.get_active_key_for_tenant("tenant-h5")
    assert active is not None and active.id == key.id, "pod B has no record of the tenant key"
    assert byok_key_manager.tenant_decrypt("tenant-h5", token) == "s3cr3t-value"


def test_resource_acl_granted_on_one_pod_restricts_on_another(tmp_path, monkeypatch):
    install_shared_store(monkeypatch)
    _on_pod(monkeypatch, tmp_path / "pod-a")
    resource_acl.upsert_grant(
        tenant_id="ws-h5", resource_type="connector", resource_id="c1", principal="owner@example.com", role="owner"
    )

    _on_pod(monkeypatch, tmp_path / "pod-b")
    # With no grants a resource is unrestricted; pod B must see the grant and refuse.
    with pytest.raises(PermissionError):
        resource_acl.assert_resource_acl(
            tenant_id="ws-h5", resource_type="connector", resource_id="c1",
            principal="mallory@example.com", min_role="viewer",
        )


def test_resource_acl_store_fails_closed_when_shared_store_is_down(tmp_path, monkeypatch):
    _on_pod(monkeypatch, tmp_path / "pod-a")

    def down(_name):
        raise ControlPlaneUnavailable("the control-plane database is configured but unreachable")

    monkeypatch.setattr(shared_doc_store, "_collection", down)
    with pytest.raises(RuntimeError, match="unreadable"):
        resource_acl.list_grants(tenant_id="ws-h5")


# ---------------------------------------------------------------------------
# Compare-and-swap
# ---------------------------------------------------------------------------


def _doc(tmp_path: Path, name: str = "") -> SharedDoc:
    path = tmp_path / "doc.json"
    return SharedDoc(
        name or f"test_doc_{uuid.uuid4().hex[:8]}",
        file_load=lambda: json.loads(path.read_text()) if path.exists() else {"items": []},
        file_save=lambda data: path.write_text(json.dumps(data)),
        file_path=lambda: path,
        empty=lambda: {"items": []},
    )


def test_stale_write_is_refused_not_silently_applied(tmp_path, monkeypatch):
    install_shared_store(monkeypatch)
    doc = _doc(tmp_path)
    first = doc.load()
    second = doc.load()
    first["items"].append("a")
    doc.save(first)
    second["items"].append("b")
    with pytest.raises(SharedDocConflict):
        doc.save(second)
    assert doc.load()["items"] == ["a"]


def test_concurrent_tenant_updates_keep_both_changes(tmp_path, monkeypatch):
    """A write that lands between another writer's read and write is not lost."""
    install_shared_store(monkeypatch)
    _on_pod(monkeypatch, tmp_path / "pod-a")
    one = tenant_store.create_tenant("ws-one", "One")
    two = tenant_store.create_tenant("ws-two", "Two")

    real_validate = tenant_store._validate_name
    raced = threading.Event()

    def validate_then_race(name):
        # Runs after update_tenant read the store and before it writes.
        if not raced.is_set():
            raced.set()
            worker = threading.Thread(
                target=lambda: _other_replica_writes(two.id), name="other-replica"
            )
            worker.start()
            worker.join(10)
        return real_validate(name)

    def _other_replica_writes(tenant_id):
        # A second replica has its own process lock; talk to the shared store directly.
        coll = shared_doc_store._collection(shared_doc_store.DOCS_COLLECTION)
        current = coll.find_one({"_id": "tenant_settings"})
        body = json.loads(current["body"])
        for raw in body["tenants"]:
            if raw["id"] == tenant_id:
                raw["sso_enforced"] = True
        coll.update_one(
            {"_id": "tenant_settings", "rev": current["rev"]},
            {"$set": {"body": json.dumps(body), "rev": current["rev"] + 1}},
        )

    monkeypatch.setattr(tenant_store, "_validate_name", validate_then_race)
    tenant_store.update_tenant(one.id, name="One renamed")

    assert tenant_store.get_tenant(one.id).name == "One renamed"
    assert tenant_store.get_tenant(two.id).sso_enforced is True, "the other replica's require-SSO write was dropped"


def _live_mongo_collection():
    try:
        from pymongo import MongoClient

        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=800)
        client.admin.command("ping")
    except Exception:  # noqa: BLE001 - any failure means "no live database on this runner"
        return None, None
    return client, client[f"h5_cas_{uuid.uuid4().hex[:10]}"]


def test_compare_and_swap_on_live_mongodb_loses_no_increment(tmp_path, monkeypatch):
    client, db = _live_mongo_collection()
    if db is None:
        pytest.skip("MongoDB not reachable on this runner")
    try:
        monkeypatch.setattr(shared_doc_store, "_collection", lambda name: db[name])
        doc = _doc(tmp_path)

        @doc.retrying
        def bump(tag):
            data = doc.load()
            data.setdefault("items", []).append(tag)
            doc.save(data)

        # Each thread stands in for a replica: bypass the per-process lock so the
        # database's compare-and-swap is what keeps the writes apart.
        monkeypatch.setattr(doc, "_lock", _NoLock())
        monkeypatch.setattr(shared_doc_store, "_MAX_ATTEMPTS", 200)
        threads = [
            threading.Thread(target=lambda n=n: [bump(f"{n}-{i}") for i in range(10)]) for n in range(6)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        items = doc.load()["items"]
        assert len(items) == 60 and len(set(items)) == 60
    finally:
        client.drop_database(db.name)
        client.close()


class _NoLock:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


# ---------------------------------------------------------------------------
# One-time import of the existing file
# ---------------------------------------------------------------------------


def test_existing_tenant_file_is_imported_once_and_never_deleted(tmp_path, monkeypatch):
    _on_pod(monkeypatch, tmp_path / "pod-a")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    monkeypatch.delenv("DATAWRAP_CONTROL_STORE_BACKEND", raising=False)
    legacy = tenant_store.create_tenant("ws-legacy", "Legacy", ip_allowlist=["192.0.2.0/24"])
    file_bytes = tenant_store.STORE_PATH.read_bytes()
    monkeypatch.delenv("DATAFLOW_CONTROL_STORE_BACKEND")

    db = install_shared_store(monkeypatch)
    assert tenant_store.get_tenant(legacy.id).ip_allowlist == ["192.0.2.0/24"]
    shared = db[shared_doc_store.DOCS_COLLECTION].find_one({"_id": "tenant_settings"})
    assert shared["rev"] == 1 and len(shared["import_sources"]) == 1
    assert tenant_store.STORE_PATH.read_bytes() == file_bytes, "the import must leave the file as it was"

    # A change through the shared store is not undone by importing the same file again.
    tenant_store.update_tenant(legacy.id, ip_allowlist=[])
    tenant_store._DOC._imported.clear()  # a restarted process has no memory of the import
    tenant_store._DOC._seen_stamps.clear()
    assert tenant_store._DOC.import_local_file() is None
    assert tenant_store.get_tenant(legacy.id).ip_allowlist == []
    assert tenant_store.STORE_PATH.read_bytes() == file_bytes


def test_second_pods_file_adds_only_records_the_shared_store_lacks(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    monkeypatch.delenv("DATAWRAP_CONTROL_STORE_BACKEND", raising=False)
    _on_pod(monkeypatch, tmp_path / "pod-a")
    a = tenant_store.create_tenant("ws-a", "A")
    _on_pod(monkeypatch, tmp_path / "pod-b")
    b = tenant_store.create_tenant("ws-b", "B")
    monkeypatch.delenv("DATAFLOW_CONTROL_STORE_BACKEND")

    install_shared_store(monkeypatch)
    _on_pod(monkeypatch, tmp_path / "pod-a")
    assert [t.id for t in tenant_store.list_tenants()] == [a.id]
    tenant_store.update_tenant(a.id, sso_enforced=True)
    _on_pod(monkeypatch, tmp_path / "pod-b")
    assert {t.id for t in tenant_store.list_tenants()} == {a.id, b.id}
    assert tenant_store.get_tenant(a.id).sso_enforced is True


def test_log_import_is_idempotent_and_merges_two_pods(tmp_path, monkeypatch):
    db = install_shared_store(monkeypatch)
    path_a, path_b = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    path_a.write_text('{"id": "1", "n": 1}\n{"id": "2", "n": 2}\n')
    path_b.write_text('{"id": "2", "n": 2}\n{"id": "3", "n": 3}\n')
    current = {"path": path_a}
    log = SharedLog(f"test_log_{uuid.uuid4().hex[:8]}", file_path=lambda: current["path"])

    coll = log.backend()
    assert [r["id"] for r in log.records(coll)] == ["1", "2"]
    log._imported.clear()
    log._seen_stamps.clear()
    log.backend()
    assert len(log.records(coll)) == 2, "a second import of the same file must add nothing"

    current["path"] = path_b
    log.backend()
    assert sorted(r["id"] for r in log.records(coll)) == ["1", "2", "3"]
    log.append(coll, {"id": "4", "n": 4})
    assert log.records(coll)[-1]["id"] == "4"
    assert log.update(coll, "4", lambda rec: {**rec, "n": 40})["n"] == 40
    assert log.get(coll, "4")["n"] == 40
    log.trim(coll, 2)
    assert [r["id"] for r in log.records(coll)][-1] == "4" and len(log.records(coll)) == 2
    assert path_a.exists() and path_b.exists()
    assert isinstance(db, FakeDatabase)


# ---------------------------------------------------------------------------
# Backend choice and the startup refusal
# ---------------------------------------------------------------------------


def _clear_backend_env(monkeypatch) -> None:
    for key in (
        "CONTROL_STORE_BACKEND", "MULTI_REPLICA", "PROCESS_ROLE", "POD_LOCAL_STORES_ACK",
        "SCHEDULE_LOOP", "API_CLAIM_LOOP", "ACK_BACKEND", "WORKER_FLEET",
    ):
        monkeypatch.delenv(f"DATAFLOW_{key}", raising=False)
        monkeypatch.delenv(f"DATAWRAP_{key}", raising=False)


def test_single_node_without_a_database_keeps_its_files(tmp_path, monkeypatch):
    _clear_backend_env(monkeypatch)
    _on_pod(monkeypatch, tmp_path / "solo")
    assert shared_doc_store.control_store_backend() == "file"
    tenant_store.create_tenant("ws-solo", "Solo")
    assert json.loads(tenant_store.STORE_PATH.read_text())["tenants"][0]["workspace_id"] == "ws-solo"


def test_multi_replica_never_falls_back_to_the_pod_local_file(tmp_path, monkeypatch):
    _clear_backend_env(monkeypatch)
    _on_pod(monkeypatch, tmp_path / "pod-a")
    tenant_store.STORE_PATH.write_text(json.dumps({"tenants": [{"id": "t1", "workspace_id": "w", "name": "stale"}]}))
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    assert shared_doc_store.control_store_backend() == "mongo"
    # This test process has no real MongoDB client (memory job store).
    with pytest.raises(ControlPlaneUnavailable, match="DATAFLOW_CONTROL_STORE_BACKEND"):
        tenant_store.list_tenants()


def _honest_multi_replica_api(monkeypatch) -> None:
    _clear_backend_env(monkeypatch)
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "1")
    monkeypatch.setenv("DATAFLOW_PROCESS_ROLE", "api")
    monkeypatch.setenv("DATAFLOW_SCHEDULE_LOOP", "0")
    monkeypatch.setenv("DATAFLOW_API_CLAIM_LOOP", "0")
    monkeypatch.setenv("DATAFLOW_ACK_BACKEND", "mongo")
    monkeypatch.setenv("DATAFLOW_POD_LOCAL_STORES_ACK", "transfer_plans,pilot_results")


def test_multi_replica_with_file_control_stores_refuses_to_start(monkeypatch):
    _honest_multi_replica_api(monkeypatch)
    assert topology_errors() == []
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    errors = topology_errors()
    assert len(errors) == 1
    message = errors[0]
    assert "DATAFLOW_CONTROL_STORE_BACKEND" in message and "mongo" in message
    for store in ("tenant_settings", "byok_keys", "resource_acls", "tenants.json"):
        assert store in message


def test_store_without_a_shared_backend_is_named_until_acknowledged(monkeypatch):
    _honest_multi_replica_api(monkeypatch)
    monkeypatch.setenv("DATAFLOW_POD_LOCAL_STORES_ACK", "pilot_results")
    errors = topology_errors()
    assert len(errors) == 1
    assert "transfer_plans" in errors[0] and "DATAFLOW_POD_LOCAL_STORES_ACK" in errors[0]
    monkeypatch.delenv("DATAFLOW_POD_LOCAL_STORES_ACK")
    assert {name for name in ("transfer_plans", "pilot_results") if any(name in e for e in topology_errors())} == {
        "transfer_plans", "pilot_results",
    }


def test_one_replica_api_that_names_no_role_still_boots(monkeypatch):
    """The Railway API sets MULTI_REPLICA=1 and no role; it must not start refusing."""
    _clear_backend_env(monkeypatch)
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    monkeypatch.setenv("DATAFLOW_WORKER_FLEET", "1")
    assert topology_errors() == []


def test_registry_matches_the_stores_that_really_have_a_shared_backend():
    import importlib

    for module in (
        "services.notification_store", "services.shape_draft_store", "services.preflight_run_store",
        "services.cdc_mapping_review", "services.mcp_invocation_log", "services.audit_anchor",
    ):
        importlib.import_module(module)
    registered = set(shared_doc_store.registered_stores())
    declared_shared = {name for name, _file, shared in shared_doc_store.CONTROL_PLANE_STORES if shared}
    declared_local = {name for name, _file, shared in shared_doc_store.CONTROL_PLANE_STORES if not shared}
    assert declared_shared <= registered, f"declared shared but still file-only: {declared_shared - registered}"
    assert not (declared_local & registered)


# ---------------------------------------------------------------------------
# The remaining stores behind the same backend
# ---------------------------------------------------------------------------


def _other_stores_on_pod(monkeypatch, root: Path) -> None:
    from services import audit_anchor, cdc_mapping_review, mcp_invocation_log, notification_store
    from services import preflight_run_store, shape_draft_store

    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(notification_store, "STORE_PATH", root / "notifications.json")
    monkeypatch.setattr(shape_draft_store, "STORE_PATH", root / "shape_drafts.json")
    monkeypatch.setattr(preflight_run_store, "STORE_PATH", root / "preflight_runs.jsonl")
    monkeypatch.setattr(cdc_mapping_review, "STORE_PATH", root / "cdc_mapping_reviews.jsonl")
    monkeypatch.setattr(mcp_invocation_log, "STORE_PATH", root / "mcp_invocations.jsonl")
    monkeypatch.setattr(audit_anchor, "ANCHOR_STORE", root / "audit_tip_anchors.jsonl")
    for name in ("NOTIFICATION_STORE", "CDC_MAPPING_REVIEW_STORE", "AUDIT_ANCHOR_STORE"):
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)


def test_notification_channel_saved_on_one_pod_is_listed_on_another(tmp_path, monkeypatch):
    from services import notification_store

    install_shared_store(monkeypatch)
    _other_stores_on_pod(monkeypatch, tmp_path / "pod-a")
    channel = notification_store.create_channel(
        label="ops", kind="webhook", workspace_id="ws-h5", config={"url": "https://hooks.example.com/x"}
    )
    _other_stores_on_pod(monkeypatch, tmp_path / "pod-b")
    assert [c.id for c in notification_store.list_channels("ws-h5")] == [channel.id]
    assert notification_store.delete_channel(channel.id) is True
    _other_stores_on_pod(monkeypatch, tmp_path / "pod-a")
    assert notification_store.list_channels("ws-h5") == []


def test_shape_draft_and_preflight_run_follow_the_user_to_another_pod(tmp_path, monkeypatch):
    from services import preflight_run_store, shape_draft_store

    install_shared_store(monkeypatch)
    _other_stores_on_pod(monkeypatch, tmp_path / "pod-a")
    shape_draft_store.save_shape_draft(route_key="pg→mysql", recipe={"steps": []}, workspace_id="ws-h5")
    saved = preflight_run_store.save_preflight_run({"passed": True, "gates": [], "blockers": []})

    _other_stores_on_pod(monkeypatch, tmp_path / "pod-b")
    assert shape_draft_store.load_shape_draft("pg→mysql", workspace_id="ws-h5") is not None
    found = preflight_run_store.get_preflight_run(saved["run_id"])
    assert found is not None and found["passed"] is True
    assert [r["run_id"] for r in preflight_run_store.list_preflight_runs()] == [saved["run_id"]]


def test_cdc_mapping_review_flagged_by_a_worker_is_acknowledged_by_an_api_pod(tmp_path, monkeypatch):
    from services import cdc_mapping_review

    install_shared_store(monkeypatch)
    _other_stores_on_pod(monkeypatch, tmp_path / "worker")
    signal = cdc_mapping_review.flag_mapping_review(source_key="src-1", table="public.orders", reason="ddl")

    _other_stores_on_pod(monkeypatch, tmp_path / "api")
    assert [r["id"] for r in cdc_mapping_review.list_reviews(source_key="src-1")] == [signal["id"]]
    assert cdc_mapping_review.acknowledge_review(signal["id"])["status"] == "acknowledged"
    assert cdc_mapping_review.acknowledge_review(signal["id"]) is None, "a second acknowledge finds nothing open"

    _other_stores_on_pod(monkeypatch, tmp_path / "worker")
    assert cdc_mapping_review.list_reviews(source_key="src-1") == []


def test_mcp_invocations_and_audit_anchors_are_deployment_wide(tmp_path, monkeypatch):
    from services import audit_anchor, mcp_invocation_log

    install_shared_store(monkeypatch)
    _other_stores_on_pod(monkeypatch, tmp_path / "pod-a")
    monkeypatch.setattr(mcp_invocation_log, "MAX_ROWS", 3)
    for n in range(5):
        mcp_invocation_log.log_mcp_invocation(tool=f"tool_{n}", client="t", actor="a@example.com", status="ok", duration_ms=1.0)
    receipt = audit_anchor.anchor_tip("abc123")

    _other_stores_on_pod(monkeypatch, tmp_path / "pod-b")
    assert [r["tool"] for r in mcp_invocation_log.list_mcp_invocations(limit=10)] == ["tool_4", "tool_3", "tool_2"]
    assert audit_anchor.latest_anchor()["id"] == receipt["id"]
    # Each logged invocation also anchored an audit tip; the newest is listed first.
    assert audit_anchor.list_anchors()[0]["id"] == receipt["id"]


def test_startup_import_covers_every_store_and_is_safe_to_repeat(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    monkeypatch.delenv("DATAWRAP_CONTROL_STORE_BACKEND", raising=False)
    _on_pod(monkeypatch, tmp_path / "pod-a")
    _other_stores_on_pod(monkeypatch, tmp_path / "pod-a")
    assert shared_doc_store.import_local_files() == {}, "file mode imports nothing"
    tenant_store.create_tenant("ws-boot", "Boot")
    resource_acl.upsert_grant(
        tenant_id="ws-boot", resource_type="job", resource_id="j1", principal="a@example.com", role="viewer"
    )
    before = {p.name: p.read_bytes() for p in (tmp_path / "pod-a").iterdir() if p.is_file()}
    monkeypatch.delenv("DATAFLOW_CONTROL_STORE_BACKEND")

    install_shared_store(monkeypatch)
    monkeypatch.setattr(shared_doc_store, "control_store_backend", lambda: "mongo")
    first = shared_doc_store.import_local_files()
    assert {"tenant_settings", "resource_acls"} <= set(first)
    assert all(report["action"] == "imported" for report in first.values())
    assert shared_doc_store.import_local_files() == {}
    after = {p.name: p.read_bytes() for p in (tmp_path / "pod-a").iterdir() if p.is_file()}
    assert {k: after[k] for k in before} == before, "import must not delete or rewrite a file"


def test_tenant_middleware_refuses_requests_but_answers_probes_when_store_is_down(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.middleware import tenant_middleware

    def down(_host):
        raise ControlPlaneUnavailable("the control-plane database is configured but unreachable")

    monkeypatch.setattr(tenant_middleware, "resolve_tenant_from_host", down)
    app = FastAPI()
    app.add_middleware(tenant_middleware.TenantMiddleware)

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/api/v1/thing")
    def thing():
        return {"ok": True}

    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        refused = client.get("/api/v1/thing")
        assert refused.status_code == 503 and refused.headers["retry-after"] == "5"


def test_real_backend_path_on_live_mongodb(tmp_path, monkeypatch):
    """MULTI_REPLICA=1 with a real client: the store lands in control_plane_docs, not the file."""
    from services import mongodb_service

    svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    svc.db_name = f"h5_e2e_{uuid.uuid4().hex[:10]}"
    try:
        monkeypatch.setattr(mongodb_service, "_mongodb_service", svc)
        _clear_backend_env(monkeypatch)
        monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
        monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0")
        from services import control_plane_store

        monkeypatch.setattr(control_plane_store, "_remember_seen", lambda: None)
        _on_pod(monkeypatch, tmp_path / "pod-a")
        tenant = tenant_store.create_tenant("ws-live", "Live", ip_allowlist=["203.0.113.9/32"])
        assert not tenant_store.STORE_PATH.exists(), "multi-replica must not write the pod-local file"
        stored = svc.get_database()[shared_doc_store.DOCS_COLLECTION].find_one({"_id": "tenant_settings"})
        assert stored["rev"] == 1 and "203.0.113.9/32" in stored["body"]
        _on_pod(monkeypatch, tmp_path / "pod-b")
        assert tenant_store.get_tenant(tenant.id).ip_allowlist == ["203.0.113.9/32"]
        tenant_store.update_tenant(tenant.id, sso_enforced=True)
        assert svc.get_database()[shared_doc_store.DOCS_COLLECTION].find_one({"_id": "tenant_settings"})["rev"] == 2
    finally:
        svc.client.drop_database(svc.db_name)
        svc.disconnect()
