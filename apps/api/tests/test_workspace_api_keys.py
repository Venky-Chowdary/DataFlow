"""Workspace API keys persist across a new read and expire like a PAT."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services import integrations_store


class _Result:
    def __init__(self, deleted: int) -> None:
        self.deleted_count = deleted


class _Keys:
    def __init__(self) -> None:
        self.docs: dict[str, dict] = {}

    def find(self, _query=None):
        return [dict(doc) for doc in self.docs.values()]

    def update_one(self, filt, update, upsert=False):
        key_id = filt["id"]
        if upsert or key_id in self.docs:
            self.docs[key_id] = dict(update["$set"])
        return _Result(1)

    def delete_one(self, filt):
        removed = self.docs.pop(filt["id"], None)
        return _Result(1 if removed else 0)


@pytest.fixture
def key_store(tmp_path, monkeypatch):
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    coll = _Keys()
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: coll)
    return coll


def test_minted_key_is_still_there_on_the_next_read(key_store):
    created = integrations_store.create_api_key(
        "Production ETL", "admin@example.com", role="editor", expires_in="90d"
    )
    listed = integrations_store.list_api_keys()
    assert listed[0]["id"] == created["id"]
    assert listed[0]["expired"] is False
    assert listed[0]["lifetime"] == "90d"
    assert listed[0]["expires_at"]
    assert "key_hash" not in listed[0]
    assert integrations_store.verify_workspace_api_key(created["key"])["id"] == created["id"]
    assert key_store.docs[created["id"]]["key_hash"]


def test_key_survives_when_the_local_file_is_empty(key_store):
    created = integrations_store.create_api_key(
        "MCP", "admin@example.com", expires_in="30d"
    )
    integrations_store._save_file_keys([])
    listed = integrations_store.list_api_keys()
    assert [row["id"] for row in listed] == [created["id"]]
    assert integrations_store.verify_workspace_api_key(created["key"])["role"] == "editor"


def test_file_key_is_healed_into_mongo(key_store, monkeypatch):
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    created = integrations_store.create_api_key(
        "Legacy file", "admin@example.com", expires_in="never"
    )
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: key_store)
    listed = integrations_store.list_api_keys()
    assert listed[0]["id"] == created["id"]
    assert listed[0]["expires_at"] is None
    assert created["id"] in key_store.docs
    assert integrations_store.verify_workspace_api_key(created["key"]) is not None


def test_expired_key_no_longer_authenticates_and_stays_listed(key_store):
    created = integrations_store.create_api_key(
        "Short", "admin@example.com", expires_in="7d"
    )
    record = key_store.docs[created["id"]]
    record["expires_at"] = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    integrations_store._upsert_api_key_record(record)
    assert integrations_store.verify_workspace_api_key(created["key"]) is None
    listed = integrations_store.list_api_keys()
    assert listed[0]["expired"] is True


def test_legacy_key_without_an_expiry_still_authenticates(key_store, monkeypatch):
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    raw = "dfk_legacy-token-value"
    integrations_store._save_file_keys(
        [
            {
                "id": "legacy-1",
                "name": "Old key",
                "prefix": raw[:12],
                "role": "editor",
                "key_hash": integrations_store._hash_api_key(raw),
                "created_at": "2024-01-01T00:00:00+00:00",
                "created_by": "admin@example.com",
                "last_used_at": None,
            }
        ]
    )
    verified = integrations_store.verify_workspace_api_key(raw)
    assert verified is not None
    assert verified["id"] == "legacy-1"


def test_api_key_keeps_its_own_role_when_the_creator_is_an_admin(monkeypatch):
    from services import effective_role

    monkeypatch.setattr(effective_role, "_membership_role", lambda **_k: "admin")
    assert effective_role.resolve_effective_role(
        {"email": "admin@example.com", "role": "editor", "auth_kind": "api_key"}
    ) == "editor"
    assert effective_role.resolve_effective_role(
        {"email": "admin@example.com", "role": "viewer", "auth_kind": "api_key"}
    ) == "viewer"
    assert effective_role.resolve_effective_role(
        {"email": "admin@example.com", "role": "editor"}
    ) == "admin"


def test_unknown_lifetime_is_refused():
    with pytest.raises(ValueError, match="expires_in"):
        integrations_store.parse_api_key_lifetime("2h")


def test_revoke_removes_the_key_from_both_stores(key_store):
    created = integrations_store.create_api_key("Drop me", "admin@example.com", expires_in="90d")
    assert integrations_store.revoke_api_key(created["id"]) is True
    assert integrations_store.list_api_keys() == []
    assert key_store.docs[created["id"]]["revoked_at"]
    assert integrations_store.verify_workspace_api_key(created["key"]) is None
    assert integrations_store.revoke_api_key(created["id"]) is False
    assert integrations_store.revoke_api_key("missing") is False


def test_stale_file_cannot_restore_a_revoked_key(key_store):
    created = integrations_store.create_api_key("Tunnel", "admin@example.com", expires_in="30d")
    secret = created["key"]
    assert integrations_store.revoke_api_key(created["id"]) is True
    stale = dict(key_store.docs[created["id"]])
    stale.pop("revoked_at", None)
    integrations_store._save_file_keys([stale])
    assert integrations_store.verify_workspace_api_key(secret) is None
    assert integrations_store.list_api_keys() == []
    assert integrations_store._file_key_records()[0]["revoked_at"]


def test_file_revocation_reaches_mongo_when_the_control_plane_missed_it(key_store):
    created = integrations_store.create_api_key("Late revoke", "admin@example.com", expires_in="30d")
    record = dict(key_store.docs[created["id"]])
    record["revoked_at"] = "2026-10-07T00:00:00+00:00"
    integrations_store._save_file_keys([record])
    key_store.docs[created["id"]].pop("revoked_at", None)
    assert integrations_store.verify_workspace_api_key(created["key"]) is None
    assert key_store.docs[created["id"]]["revoked_at"] == "2026-10-07T00:00:00+00:00"


@pytest.mark.parametrize(
    ("lifetime", "days"),
    [("7d", 7), ("30d", 30), ("60d", 60), ("90d", 90), ("365d", 365)],
)
def test_each_lifetime_expires_that_many_days_out(key_store, lifetime, days):
    created = integrations_store.create_api_key(
        lifetime, "admin@example.com", expires_in=lifetime
    )
    start = datetime.fromisoformat(created["created_at"])
    end = datetime.fromisoformat(created["expires_at"])
    assert abs((end - start).total_seconds() - days * 86400) < 5
    assert created["lifetime"] == lifetime
    assert created["expired"] is False


def test_blank_lifetime_defaults_to_90_days_and_never_has_no_date(key_store):
    assert integrations_store.parse_api_key_lifetime("") == "90d"
    assert integrations_store.parse_api_key_lifetime(" 60D ") == "60d"
    created = integrations_store.create_api_key("Open", "admin@example.com", expires_in="never")
    assert created["expires_at"] is None
    assert created["lifetime"] == "never"
    assert integrations_store.verify_workspace_api_key(created["key"])["id"] == created["id"]


def test_blank_name_and_unknown_role_do_not_mint_a_broken_key(key_store):
    created = integrations_store.create_api_key("   ", "admin@example.com", role="admin", expires_in="7d")
    assert created["name"] == "API key"
    assert created["role"] == "admin"
    with pytest.raises(ValueError, match="role"):
        integrations_store.create_api_key("nope", "admin@example.com", role="owner")
    assert [row["id"] for row in integrations_store.list_api_keys()] == [created["id"]]


def test_corrupt_expiry_fails_closed_and_use_stamps_last_used(key_store):
    created = integrations_store.create_api_key("Clock", "admin@example.com", expires_in="90d")
    assert integrations_store.list_api_keys()[0]["last_used_at"] is None
    integrations_store.verify_workspace_api_key(created["key"])
    assert integrations_store.list_api_keys()[0]["last_used_at"]
    record = key_store.docs[created["id"]]
    record["expires_at"] = "not-a-date"
    integrations_store._upsert_api_key_record(record)
    assert integrations_store.verify_workspace_api_key(created["key"]) is None
    assert integrations_store.list_api_keys()[0]["expired"] is True


def test_newest_key_is_listed_first_and_a_revoked_sibling_stays_hidden(key_store):
    older = integrations_store.create_api_key("Older", "admin@example.com", expires_in="90d")
    newer = integrations_store.create_api_key("Newer", "admin@example.com", expires_in="7d")
    assert [row["id"] for row in integrations_store.list_api_keys()] == [newer["id"], older["id"]]
    integrations_store.revoke_api_key(older["id"])
    assert [row["id"] for row in integrations_store.list_api_keys()] == [newer["id"]]
    assert integrations_store.verify_workspace_api_key(newer["key"])["id"] == newer["id"]
    assert integrations_store.verify_workspace_api_key("") is None
    assert integrations_store.verify_workspace_api_key("sk_live_not_ours") is None


def test_a_revoked_file_key_is_healed_as_a_tombstone(key_store):
    created = integrations_store.create_api_key("Gone", "admin@example.com", expires_in="30d")
    secret = created["key"]
    assert integrations_store.revoke_api_key(created["id"]) is True
    key_store.docs.pop(created["id"])
    assert integrations_store.verify_workspace_api_key(secret) is None
    assert integrations_store.list_api_keys() == []
    assert key_store.docs[created["id"]]["revoked_at"]


def test_api_key_role_is_enforced_when_auth_is_required(key_store, monkeypatch):
    """An editor key stays an editor even when the person who minted it is an admin."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from services import effective_role
    from src.middleware.auth_middleware import AuthMiddleware
    from src.services import auth_service
    from src.services.rbac import RBACMiddleware

    monkeypatch.setattr(auth_service, "auth_required", lambda: True)
    monkeypatch.setattr(effective_role, "_membership_role", lambda **_k: "admin")
    editor = integrations_store.create_api_key(
        "Editor", "admin@example.com", role="editor", expires_in="30d"
    )
    viewer = integrations_store.create_api_key(
        "Viewer", "admin@example.com", role="viewer", expires_in="7d"
    )
    operator = integrations_store.create_api_key(
        "Operator", "admin@example.com", role="operator", expires_in="90d"
    )
    admin = integrations_store.create_api_key(
        "Admin", "admin@example.com", role="admin", expires_in="never"
    )

    app = FastAPI()
    app.add_middleware(RBACMiddleware)
    app.add_middleware(AuthMiddleware)

    @app.get("/api/v1/workspace/settings")
    def settings():
        return {"ok": True}

    @app.get("/api/v1/workspace/api-keys")
    def listed():
        return {"ok": True}

    @app.post("/api/v1/workspace/api-keys")
    def minted():
        return {"ok": True}

    @app.delete("/api/v1/workspace/api-keys/{key_id}")
    def revoked(key_id: str):
        return {"ok": True, "id": key_id}

    client = TestClient(app)

    def bearer(secret: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {secret}"}

    assert client.get("/api/v1/workspace/api-keys").status_code == 401
    assert client.get("/api/v1/workspace/settings", headers=bearer(editor["key"])).status_code == 200
    assert client.get("/api/v1/workspace/api-keys", headers=bearer(editor["key"])).status_code == 403
    assert client.post("/api/v1/workspace/api-keys", headers=bearer(editor["key"])).status_code == 403
    assert client.get("/api/v1/workspace/settings", headers=bearer(operator["key"])).status_code == 200
    assert client.get("/api/v1/workspace/api-keys", headers=bearer(operator["key"])).status_code == 403
    assert (
        client.delete(
            f"/api/v1/workspace/api-keys/{editor['id']}",
            headers=bearer(viewer["key"]),
        ).status_code
        == 403
    )
    assert client.get("/api/v1/workspace/api-keys", headers=bearer(admin["key"])).status_code == 200
    assert (
        client.delete(
            f"/api/v1/workspace/api-keys/{viewer['id']}",
            headers=bearer(admin["key"]),
        ).status_code
        == 200
    )

    record = key_store.docs[operator["id"]]
    record["expires_at"] = "2020-01-01T00:00:00+00:00"
    integrations_store._upsert_api_key_record(record)
    assert client.get("/api/v1/workspace/settings", headers=bearer(operator["key"])).status_code == 401

    assert integrations_store.revoke_api_key(editor["id"]) is True
    assert client.get("/api/v1/workspace/settings", headers=bearer(editor["key"])).status_code == 401


def test_creating_a_key_keeps_the_rest_of_the_integration_file(key_store):
    data = integrations_store._load_raw()
    data["sso"]["saml"]["entity_id"] = "https://idp.example/saml"
    integrations_store._save(data)
    created = integrations_store.create_api_key("Beside SSO", "admin@example.com", expires_in="30d")
    stored = integrations_store._load_raw()
    assert stored["sso"]["saml"]["entity_id"] == "https://idp.example/saml"
    assert any(item["id"] == created["id"] for item in stored["api_keys"])
    assert "key" not in stored["api_keys"][0]
    assert stored["api_keys"][0]["key_hash"]
