"""Ops review High 2: a node's file must never bring back revoked records.

The shared control-plane store imports a node's local file only while the
store has no shared records yet (the upgrade). A file that changes later (a
rollback to the old build wrote it) is conflicting local data: it is logged
once, recorded in the store's status, and never merged, so the shared state
stays authoritative.
"""

from __future__ import annotations

import logging
import time
import uuid

import pytest

from services import control_plane_store, mongodb_service, resource_acl, shared_doc_store
from services.shared_doc_store import SharedLog
from tests._control_plane_fakes import install_shared_store

RT = "connector"


def _principals() -> list[str]:
    return sorted(g.principal for g in resource_acl.list_grants(tenant_id="t", resource_type=RT, resource_id="r"))


def _role(principal: str) -> list[str]:
    return [
        g.role
        for g in resource_acl.list_grants(tenant_id="t", resource_type=RT, resource_id="r")
        if g.principal == principal
    ]


def _pod(monkeypatch, tmp_path) -> None:
    root = tmp_path / "pod"
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(resource_acl, "STORE_PATH", root / "resource_acls.jsonl")
    for prefix in ("DATAFLOW_", "DATAWRAP_"):
        monkeypatch.delenv(f"{prefix}RESOURCE_ACL_STORE", raising=False)
        monkeypatch.delenv(f"{prefix}CONTROL_STORE_BACKEND", raising=False)
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0")


def _restart(store) -> None:
    """A restarted process remembers nothing about earlier imports."""
    store._imported.clear()
    store._seen_stamps.clear()


@pytest.fixture
def live_mongo(monkeypatch):
    svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    svc.db_name = f"opsfix_h2_{uuid.uuid4().hex[:10]}"
    monkeypatch.setattr(mongodb_service, "_mongodb_service", svc)
    monkeypatch.setattr(control_plane_store, "_remember_seen", lambda: None)
    shared_doc_store.reset_outage_state()
    try:
        yield svc
    finally:
        svc.client.drop_database(svc.db_name)
        svc.disconnect()


def test_rollback_and_roll_forward_does_not_resurrect_a_revoked_grant(live_mongo, tmp_path, monkeypatch, caplog):
    """p5's exact sequence on a live MongoDB."""
    _pod(monkeypatch, tmp_path)
    _restart(resource_acl._DOC)
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")  # the old release
    resource_acl.upsert_grant(tenant_id="t", resource_type=RT, resource_id="r", principal="alice@x.io", role="viewer")
    gone = resource_acl.upsert_grant(tenant_id="t", resource_type=RT, resource_id="r", principal="mallory@x.io", role="editor")

    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "mongo")  # upgrade: the file is imported
    assert _principals() == ["alice@x.io", "mallory@x.io"]
    resource_acl.revoke_grant(gone.id, tenant_id="t")
    assert _principals() == ["alice@x.io"]

    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")  # rollback: the old build serves its file
    assert _principals() == ["alice@x.io", "mallory@x.io"]
    resource_acl.upsert_grant(tenant_id="t", resource_type=RT, resource_id="r", principal="alice@x.io", role="owner")
    file_bytes = resource_acl.STORE_PATH.read_bytes()
    time.sleep(0.01)

    caplog.set_level(logging.WARNING, logger="services.shared_doc_store")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "mongo")  # roll forward
    assert _principals() == ["alice@x.io"], "a grant revoked in the shared store came back from the node's file"
    assert _role("alice@x.io") == ["viewer"], "the shared state must stay authoritative"
    assert resource_acl.STORE_PATH.read_bytes() == file_bytes, "the node's file is left as it is"

    conflicts = shared_doc_store.store_status()["resource_acls"]["local_file_conflicts"]
    assert len(conflicts) == 1
    assert conflicts[0]["path"] == str(resource_acl.STORE_PATH) and conflicts[0]["sha256"]
    warned = [r for r in caplog.records if "conflicting local data" in r.getMessage()]
    assert len(warned) == 1

    # A restarted process meets the same file: no merge, no second record, no second warning.
    _restart(resource_acl._DOC)
    assert _principals() == ["alice@x.io"]
    assert len(shared_doc_store.store_status()["resource_acls"]["local_file_conflicts"]) == 1
    assert len([r for r in caplog.records if "conflicting local data" in r.getMessage()]) == 1


def test_first_import_still_happens_when_the_shared_store_is_empty(live_mongo, tmp_path, monkeypatch):
    _pod(monkeypatch, tmp_path)
    _restart(resource_acl._DOC)
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    resource_acl.upsert_grant(tenant_id="t", resource_type=RT, resource_id="r", principal="bob@x.io", role="viewer")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "mongo")
    assert _principals() == ["bob@x.io"]
    status = shared_doc_store.store_status()["resource_acls"]
    assert status["local_file_conflicts"] == [] and len(status["import_sources"]) == 1


def test_changed_log_file_is_not_merged_after_the_first_import(tmp_path, monkeypatch, caplog):
    """An acknowledged review must not reopen from a node's rewritten log file."""
    install_shared_store(monkeypatch)
    path = tmp_path / "reviews.jsonl"
    path.write_text('{"id": "1", "status": "open"}\n')
    log = SharedLog(f"test_log_{uuid.uuid4().hex[:8]}", file_path=lambda: path)
    coll = log.backend()
    assert log.update(coll, "1", lambda rec: {**rec, "status": "acknowledged"})["status"] == "acknowledged"

    path.write_text('{"id": "1", "status": "open"}\n{"id": "2", "status": "open"}\n')  # rollback wrote it
    _restart(log)
    caplog.set_level(logging.WARNING, logger="services.shared_doc_store")
    coll = log.backend()
    assert [(r["id"], r["status"]) for r in log.records(coll)] == [("1", "acknowledged")]
    status = shared_doc_store.store_status()[log.name]
    assert len(status["local_file_conflicts"]) == 1
    assert len([r for r in caplog.records if "conflicting local data" in r.getMessage()]) == 1
