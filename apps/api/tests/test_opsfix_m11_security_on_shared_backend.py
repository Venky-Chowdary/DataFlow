"""Ops review Medium 11: security enforcement on the shared backend, against live MongoDB.

The suite pins the file backend (conftest), and the shared backend was
covered mostly through an in-memory stand-in. These tests run the
enforcement paths that read the control-plane stores -- resource ACL deny,
require-SSO, BYOK -- on the real backend: MongoDB, the per-record layout, two
"pods" with separate data directories, and an unreachable database (they
must fail closed). Each test uses its own throwaway database.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi import HTTPException

from services import (
    byok_key_manager,
    control_plane_store,
    mongodb_service,
    resource_acl,
    secret_vault,
    shared_doc_store,
    sso_enforcement,
    team_store,
    tenant_store,
)
from services.api_errors import ApiError
from services.control_plane_store import ControlPlaneUnavailable


@pytest.fixture
def shared(monkeypatch, tmp_path):
    svc = mongodb_service.MongoDBService("mongodb://localhost:27017/")
    if not svc.connect():
        pytest.skip("MongoDB not reachable on this runner")
    svc.db_name = f"opsfix_m11_{uuid.uuid4().hex[:10]}"
    monkeypatch.setattr(mongodb_service, "_mongodb_service", svc)
    monkeypatch.setattr(control_plane_store, "_remember_seen", lambda: None)
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "mongo")
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_CACHE_SECONDS", "0")
    for prefix in ("DATAFLOW_", "DATAWRAP_"):
        monkeypatch.delenv(f"{prefix}RESOURCE_ACL_STORE", raising=False)
    shared_doc_store.reset_outage_state()
    try:
        yield svc
    finally:
        shared_doc_store.reset_outage_state()
        svc.client.drop_database(svc.db_name)
        svc.disconnect()


def _pod(monkeypatch, root) -> None:
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(tenant_store, "STORE_PATH", root / "tenants.json")
    monkeypatch.setattr(byok_key_manager, "STORE_PATH", root / "byok_keys.json")
    monkeypatch.setattr(resource_acl, "STORE_PATH", root / "resource_acls.jsonl")


def _database_goes_away(monkeypatch) -> None:
    from pymongo import MongoClient

    down = mongodb_service.MongoDBService("mongodb://127.0.0.1:1/")
    down.client = MongoClient("mongodb://127.0.0.1:1/", serverSelectionTimeoutMS=5000, connect=False)
    down.db_name = "opsfix_m11_unreachable"
    monkeypatch.setattr(mongodb_service, "_mongodb_service", down)
    shared_doc_store.reset_outage_state()


def test_resource_acl_denies_across_pods_and_fails_closed(shared, monkeypatch, tmp_path):
    _pod(monkeypatch, tmp_path / "pod-a")
    grant = resource_acl.upsert_grant(
        tenant_id="t1", resource_type="connector", resource_id="c1", principal="owner@x.io", role="owner"
    )
    check = {"tenant_id": "t1", "resource_type": "connector", "resource_id": "c1"}

    _pod(monkeypatch, tmp_path / "pod-b")
    with pytest.raises(PermissionError):
        resource_acl.assert_resource_acl(**check, principal="mallory@x.io", min_role="viewer")
    resource_acl.assert_resource_acl(**check, principal="owner@x.io", min_role="owner")
    assert resource_acl.revoke_grant(grant.id, tenant_id="t1") is True

    _pod(monkeypatch, tmp_path / "pod-a")
    assert resource_acl.list_grants(tenant_id="t1") == []
    stored = shared.get_database()["control_plane_items"].find_one({"_id": f"resource_acls:{grant.id}"})
    assert stored is None, "the revoke is in the shared store, not a pod file"

    resource_acl.upsert_grant(**check, principal="owner@x.io", role="owner")
    _database_goes_away(monkeypatch)
    with pytest.raises(RuntimeError, match="unreadable"):
        resource_acl.assert_resource_acl(**check, principal="mallory@x.io", min_role="viewer")


def test_require_sso_set_on_one_pod_refuses_a_password_sign_in_on_another(shared, monkeypatch, tmp_path):
    monkeypatch.setattr(team_store, "list_memberships_for_user", lambda _email: [{"workspace_id": "ws-sso"}])
    monkeypatch.setattr(sso_enforcement, "_is_platform_admin", lambda _email: False)
    _pod(monkeypatch, tmp_path / "pod-a")
    tenant_store.create_tenant("ws-sso", "SSO only", sso_enforced=True)

    _pod(monkeypatch, tmp_path / "pod-b")
    with pytest.raises(ApiError) as refused:
        sso_enforcement.refuse_password_sign_in("bob@x.io")
    assert refused.value.status_code == 403

    _database_goes_away(monkeypatch)
    with pytest.raises(HTTPException) as unavailable:
        sso_enforcement.refuse_password_sign_in("bob@x.io")
    assert unavailable.value.status_code == 503, "an unreadable policy must refuse, not wave through"


def test_byok_key_from_one_pod_wraps_and_unwraps_on_another_and_never_downgrades(shared, monkeypatch, tmp_path):
    _pod(monkeypatch, tmp_path / "pod-a")
    first = byok_key_manager.create_key("tenant-b", "primary")
    token = secret_vault.encrypt_secret("s3cret", tenant_id="tenant-b")
    assert token.startswith(f"byok:{first.id}")

    _pod(monkeypatch, tmp_path / "pod-b")
    assert secret_vault.decrypt_secret(token, tenant_id="tenant-b") == "s3cret"
    second = byok_key_manager.rotate_key("tenant-b")
    assert [k.id for k in byok_key_manager.list_keys("tenant-b") if k.status == "active"] == [second.id]

    _pod(monkeypatch, tmp_path / "pod-a")
    assert secret_vault.encrypt_secret("other", tenant_id="tenant-b").startswith(f"byok:{second.id}")
    assert secret_vault.decrypt_secret(token, tenant_id="tenant-b") == "s3cret", "a rotated key still unwraps"

    _database_goes_away(monkeypatch)
    with pytest.raises(ControlPlaneUnavailable):
        secret_vault.encrypt_secret("s3cret", tenant_id="tenant-b")
