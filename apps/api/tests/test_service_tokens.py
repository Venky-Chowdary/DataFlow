"""Scoped API-key storage, authorization, rotation, and IAM routes."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import Request

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def token_app(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from services import audit_log, integrations_store
    from src.middleware.auth_middleware import AuthMiddleware
    from src.routers.iam_router import router as iam_router
    from src.services import auth_service
    from src.services.rbac import RBACMiddleware

    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-service-token-secret")
    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)

    app = FastAPI()
    app.add_middleware(RBACMiddleware)
    app.add_middleware(AuthMiddleware)

    @app.get("/api/v1/connectors/")
    def list_connectors():
        return {"connectors": []}

    @app.post("/api/v1/connectors/")
    def create_connector():
        return {"created": True}

    @app.get("/api/v1/connectors/principal")
    def principal(request: Request):
        return request.state.user

    app.include_router(iam_router, prefix="/api/v1")
    return TestClient(app), integrations_store, audit_log


def _auth(key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


def _mint(store, *, role="editor", scopes=None, kind="api_key", expires_in="90d"):
    return store.create_api_key(
        "test key",
        "operator@example.com",
        role=role,
        expires_in=expires_in,
        scopes=scopes,
        kind=kind,
    )


def test_scoped_editor_key_allows_reads_and_denies_connector_writes(token_app):
    client, store, _audit = token_app
    key = _mint(store, scopes=["connector.read"], kind="service_account")
    headers = _auth(key["key"])

    read_response = client.get("/api/v1/connectors/", headers=headers)
    write_response = client.post("/api/v1/connectors/", headers=headers)
    principal_response = client.get("/api/v1/connectors/principal", headers=headers)

    assert read_response.status_code == 200
    assert write_response.status_code == 403
    assert write_response.json()["required_permission"] == "connector.write"
    assert principal_response.json()["scopes"] == ["connector.read"]
    assert principal_response.json()["kind"] == "service_account"
    assert principal_response.json()["api_key_id"] == key["id"]


def test_service_account_scope_validation_returns_400(token_app):
    client, store, _audit = token_app
    admin = _mint(store, role="admin")
    headers = _auth(admin["key"])
    endpoint = "/api/v1/iam/service-accounts"

    outside_role = client.post(
        endpoint,
        headers=headers,
        json={"name": "viewer-write", "role": "viewer", "scopes": ["connector.write"]},
    )
    unknown_scope = client.post(
        endpoint,
        headers=headers,
        json={"name": "unknown", "role": "editor", "scopes": ["connector.unknown"]},
    )
    empty_scope = client.post(
        endpoint,
        headers=headers,
        json={"name": "empty", "role": "editor", "scopes": []},
    )

    assert outside_role.status_code == 400
    assert unknown_scope.status_code == 400
    assert empty_scope.status_code == 400


def test_legacy_key_without_scope_fields_keeps_full_role_permissions(token_app):
    client, store, _audit = token_app
    raw = "dfk_legacy-editor-token"
    store._save_file_keys(
        [
            {
                "id": "legacy-editor",
                "name": "Legacy editor",
                "prefix": raw[:12],
                "role": "editor",
                "key_hash": store._hash_api_key(raw),
                "created_at": "2024-01-01T00:00:00+00:00",
                "created_by": "operator@example.com",
                "last_used_at": None,
            }
        ]
    )

    verified = store.verify_workspace_api_key(raw)
    response = client.post("/api/v1/connectors/", headers=_auth(raw))

    from services.rbac import Permission, principal_permissions, role_permissions

    assert verified is not None
    assert verified["scopes"] is None
    assert verified["kind"] == "api_key"
    assert principal_permissions(verified, "editor") == role_permissions("editor")
    assert Permission.CONNECTOR_WRITE in principal_permissions(verified, "editor")
    assert response.status_code == 200


def test_service_account_scopes_are_sorted_deduplicated_and_returned(token_app):
    _client, store, _audit = token_app
    key = _mint(
        store,
        scopes=["connector.write", "connector.read", "connector.read"],
        kind="service_account",
    )

    assert key["scopes"] == ["connector.read", "connector.write"]
    assert key["kind"] == "service_account"
    verified = store.verify_workspace_api_key(key["key"])
    assert verified is not None
    assert verified["scopes"] == ["connector.read", "connector.write"]
    assert verified["kind"] == "service_account"


def test_rotation_preserves_scopes_and_old_key_expires_after_overlap(
    token_app, monkeypatch
):
    _client, store, _audit = token_app

    class FrozenDateTime(datetime):
        current = datetime(2026, 1, 1, tzinfo=timezone.utc)

        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return cls.current.replace(tzinfo=None)
            return cls.current.astimezone(tz)

    monkeypatch.setattr(store, "datetime", FrozenDateTime)
    old = _mint(
        store,
        role="editor",
        scopes=["connector.read"],
        kind="service_account",
        expires_in="30d",
    )
    source_record = next(
        item for item in store.load_api_key_records() if item["id"] == old["id"]
    )
    source_record["expires_at"] = (
        FrozenDateTime.current + timedelta(days=5)
    ).isoformat()
    store._upsert_api_key_record(source_record)

    new = store.rotate_api_key(old["id"], actor="admin@example.com", overlap_seconds=60)
    assert store._parse_expires_at(new["expires_at"]) == (
        FrozenDateTime.current + timedelta(days=30)
    )
    old_during_overlap = store.verify_workspace_api_key(old["key"])
    new_verified = store.verify_workspace_api_key(new["key"])
    FrozenDateTime.current += timedelta(seconds=61)
    old_after_overlap = store.verify_workspace_api_key(old["key"])

    assert old_during_overlap is not None
    assert new_verified is not None
    assert new["scopes"] == ["connector.read"]
    assert new["kind"] == "service_account"
    assert new["rotated_from"] == old["id"]
    assert old_after_overlap is None


def test_zero_rotation_overlap_expires_source_immediately(token_app, monkeypatch):
    _client, store, _audit = token_app

    class FrozenDateTime(datetime):
        current = datetime(2026, 1, 1, tzinfo=timezone.utc)

        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return cls.current.replace(tzinfo=None)
            return cls.current.astimezone(tz)

    monkeypatch.setattr(store, "datetime", FrozenDateTime)
    old = _mint(store)
    store.rotate_api_key(old["id"], actor="admin@example.com", overlap_seconds=0)

    assert store.verify_workspace_api_key(old["key"]) is None


def test_rotation_rejects_invalid_sources_and_unknown_ids(token_app):
    _client, store, _audit = token_app
    key = _mint(store)

    with pytest.raises(ValueError, match="overlap_seconds"):
        store.rotate_api_key(key["id"], actor="admin@example.com", overlap_seconds=-1)
    with pytest.raises(ValueError, match="overlap_seconds"):
        store.rotate_api_key(key["id"], actor="admin@example.com", overlap_seconds=604801)

    rotated = store.rotate_api_key(key["id"], actor="admin@example.com")
    with pytest.raises(ValueError, match="already been rotated"):
        store.rotate_api_key(key["id"], actor="admin@example.com")
    assert rotated["rotated_from"] == key["id"]

    revoked = _mint(store)
    assert store.revoke_api_key(revoked["id"])
    with pytest.raises(ValueError, match="revoked"):
        store.rotate_api_key(revoked["id"], actor="admin@example.com")
    with pytest.raises(store.ApiKeyNotFound):
        store.rotate_api_key("not-present", actor="admin@example.com")


def test_expired_api_key_cannot_be_rotated(token_app):
    _client, store, _audit = token_app
    key = _mint(store)
    record = next(
        item for item in store.load_api_key_records() if item["id"] == key["id"]
    )
    record["expires_at"] = (
        datetime.now(timezone.utc) - timedelta(seconds=1)
    ).isoformat()
    store._upsert_api_key_record(record)

    with pytest.raises(ValueError, match="expired"):
        store.rotate_api_key(key["id"], actor="admin@example.com")


def test_iam_is_admin_only_and_route_errors_are_mapped(token_app):
    client, store, _audit = token_app
    editor = _mint(store, role="editor")
    admin = _mint(store, role="admin")

    editor_response = client.get(
        "/api/v1/iam/service-accounts",
        headers=_auth(editor["key"]),
    )
    admin_response = client.get(
        "/api/v1/iam/service-accounts",
        headers=_auth(admin["key"]),
    )
    missing_rotation = client.post(
        "/api/v1/iam/api-keys/missing/rotate",
        headers=_auth(admin["key"]),
        json={"overlap_seconds": 86400},
    )
    key_to_rotate = _mint(store)
    first_rotation = client.post(
        f"/api/v1/iam/api-keys/{key_to_rotate['id']}/rotate",
        headers=_auth(admin["key"]),
        json={},
    )
    repeated_rotation = client.post(
        f"/api/v1/iam/api-keys/{key_to_rotate['id']}/rotate",
        headers=_auth(admin["key"]),
        json={},
    )
    key_to_revoke = _mint(store)
    assert store.revoke_api_key(key_to_revoke["id"])
    revoked_rotation = client.post(
        f"/api/v1/iam/api-keys/{key_to_revoke['id']}/rotate",
        headers=_auth(admin["key"]),
        json={},
    )

    assert editor_response.status_code == 403
    assert editor_response.json()["required_permission"] == "iam.manage"
    assert admin_response.status_code == 200
    assert missing_rotation.status_code == 404
    assert first_rotation.status_code == 200
    assert repeated_rotation.status_code == 400
    assert revoked_rotation.status_code == 400


def test_scope_aware_permission_helpers_and_admin_only_iam_permissions():
    from services import effective_role, rbac

    user = {
        "email": "service@example.com",
        "role": "editor",
        "auth_kind": "api_key",
        "scopes": ["connector.read"],
    }

    assert effective_role.effective_permissions(user) == {"connector.read"}
    summary = effective_role.permission_summary(user, "workspace-1")
    assert summary["permissions"] == ["connector.read"]
    assert summary["can_write_connectors"] is False
    assert not rbac.has_permission(user, rbac.Permission.CONNECTOR_WRITE)
    assert rbac.Permission.IAM_MANAGE in rbac.role_permissions("admin")
    assert rbac.Permission.SCIM_PROVISION in rbac.role_permissions("admin")
    for role in ("viewer", "operator", "editor"):
        assert rbac.Permission.IAM_MANAGE not in rbac.role_permissions(role)
        assert rbac.Permission.SCIM_PROVISION not in rbac.role_permissions(role)
    assert rbac._required_permission("GET", "/api/v1/iam/service-accounts") == "iam.manage"
    assert rbac._required_permission("POST", "/api/v1/scim/v2/Users") == "scim.provision"


def test_audit_events_never_contain_service_account_secrets_and_listing_is_public(
    token_app,
):
    client, store, audit_log = token_app
    admin = _mint(store, role="admin")
    headers = _auth(admin["key"])
    created = client.post(
        "/api/v1/iam/service-accounts",
        headers=headers,
        json={
            "name": "Deployment worker",
            "role": "editor",
            "scopes": ["connector.read"],
            "expires_in": "30d",
        },
    )
    assert created.status_code == 200
    created_key = created.json()
    rotated = client.post(
        f"/api/v1/iam/api-keys/{created_key['id']}/rotate",
        headers=headers,
        json={"overlap_seconds": 30},
    )
    assert rotated.status_code == 200
    revoked = client.delete(
        f"/api/v1/iam/service-accounts/{created_key['id']}",
        headers=headers,
    )
    assert revoked.status_code == 200

    listing = client.get("/api/v1/iam/service-accounts", headers=headers)
    assert listing.status_code == 200
    listing_json = json.dumps(listing.json())
    assert created_key["key"] not in listing_json
    assert rotated.json()["key"] not in listing_json
    assert "key_hash" not in listing_json

    audit_json = audit_log.STORE_PATH.read_text(encoding="utf-8")
    assert created_key["key"] not in audit_json
    assert rotated.json()["key"] not in audit_json
    events = [json.loads(line) for line in audit_json.splitlines()]
    actions = {event["action"] for event in events}
    assert {
        "iam.service_account.create",
        "iam.api_key.rotate",
        "iam.service_account.revoke",
    } <= actions
    for event in events:
        if event["action"].startswith("iam."):
            assert {
                "id",
                "prefix",
                "kind",
                "role",
                "scopes",
                "expires_at",
                "rotated_from",
                "rotated_to",
            } <= set(event["details"])
