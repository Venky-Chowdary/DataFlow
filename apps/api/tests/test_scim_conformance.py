"""SCIM 2.0 subset conformance and lifecycle tests."""

from __future__ import annotations

import importlib
import os
import sys
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def scim_client(monkeypatch, tmp_path):
    monkeypatch.setenv("ENV", "test")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAFLOW_ENV", "test")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-scim-auth-secret-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "y" * 32)

    from services import (
        audit_log,
        auth_sessions,
        integrations_store,
        scim_store,
        team_store,
        user_store,
    )
    from services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(scim_store, "STORE_PATH", tmp_path / "scim.json")
    monkeypatch.setattr(scim_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    monkeypatch.setattr(team_store, "_database", lambda: None)
    monkeypatch.setattr(auth_sessions, "_path", lambda: tmp_path / "sessions.json")

    from src.main import app

    admin = integrations_store.create_api_key(
        "SCIM test administrator",
        "operator@example.test",
        role="admin",
    )
    editor = integrations_store.create_api_key(
        "SCIM test editor",
        "editor@example.test",
        role="editor",
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/iam/scim-token",
            headers={"Authorization": f"Bearer {admin['key']}"},
            json={"name": "conformance SCIM token"},
        )
        assert response.status_code == 200, response.text
        scim_key = response.json()["key"]
        yield {
            "client": client,
            "admin": admin["key"],
            "editor": editor["key"],
            "scim": scim_key,
            "modules": {
                "audit_log": audit_log,
                "auth_sessions": auth_sessions,
                "integrations_store": integrations_store,
                "scim_store": scim_store,
                "team_store": team_store,
                "user_store": user_store,
            },
        }


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _scim_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/scim+json",
    }


def _create_user(scim_client, user_name: str, **fields):
    response = scim_client["client"].post(
        "/api/v1/scim/v2/Users",
        headers=_scim_headers(scim_client["scim"]),
        json={"userName": user_name, **fields},
    )
    assert response.status_code == 201, response.text
    return response


def _create_group(scim_client, display_name: str, **fields):
    response = scim_client["client"].post(
        "/api/v1/scim/v2/Groups",
        headers=_scim_headers(scim_client["scim"]),
        json={"displayName": display_name, **fields},
    )
    assert response.status_code == 201, response.text
    return response.json()


def _patch_group(scim_client, group_id: str, operation: dict):
    return scim_client["client"].patch(
        f"/api/v1/scim/v2/Groups/{group_id}",
        headers=_scim_headers(scim_client["scim"]),
        json={"Operations": [operation]},
    )


def test_discovery_authentication_and_scope_isolation(scim_client):
    client = scim_client["client"]
    for endpoint in ("ServiceProviderConfig", "ResourceTypes", "Schemas"):
        response = client.get(
            f"/api/v1/scim/v2/{endpoint}",
            headers=_scim_headers(scim_client["scim"]),
        )
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/scim+json")
    assert client.post("/api/v1/scim/v2/Users", json={"userName": "x@example.test"}).status_code == 401
    assert client.get(
        "/api/v1/scim/v2/Users",
        headers=_auth(scim_client["editor"]),
    ).status_code == 403
    assert client.get(
        "/api/v1/connectors/",
        headers=_auth(scim_client["scim"]),
    ).status_code == 403
    for tool in ("get_transfer_capabilities", "start_transfer"):
        response = client.post(
            "/api/v1/mcp/tools/call",
            headers=_auth(scim_client["scim"]),
            json={"name": tool, "arguments": {}},
        )
        assert response.status_code == 403, response.text


def test_existing_account_is_linked_without_exposing_password(scim_client):
    user_store = scim_client["modules"]["user_store"]
    user_store.create_user(
        email="precreated@example.test",
        name="Precreated",
        role="member",
        created_by="test",
    )
    linked = _create_user(scim_client, "PRECREATED@example.test")
    assert linked.json()["userName"] == "precreated@example.test"
    assert "password" not in linked.json()


def test_user_crud_filters_pagination_and_patch(scim_client):
    client = scim_client["client"]
    first = _create_user(scim_client, "one@example.test", displayName="One")
    second = _create_user(scim_client, "two@example.test", displayName="Two")
    third = _create_user(scim_client, "three@example.test", displayName="Three")
    inactive = _create_user(scim_client, "inactive@example.test", active=False)
    assert inactive.json()["active"] is False
    assert scim_client["modules"]["user_store"].get_user(
        "inactive@example.test"
    )["status"] == "disabled"
    first_id = first.json()["id"]

    assert first.headers["content-type"].startswith("application/scim+json")
    assert first.headers["location"].endswith(f"/Users/{first_id}")
    assert first.json()["meta"]["created"]
    duplicate = client.post(
        "/api/v1/scim/v2/Users",
        headers=_scim_headers(scim_client["scim"]),
        json={"userName": "ONE@example.test"},
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["scimType"] == "uniqueness"

    matched = client.get(
        "/api/v1/scim/v2/Users",
        headers=_scim_headers(scim_client["scim"]),
        params={"filter": 'userName eq "ONE@EXAMPLE.TEST"'},
    )
    assert matched.json()["totalResults"] == 1
    assert matched.json()["Resources"][0]["id"] == first_id
    no_match = client.get(
        "/api/v1/scim/v2/Users",
        headers=_scim_headers(scim_client["scim"]),
        params={"filter": 'userName eq "absent@example.test"'},
    )
    assert no_match.json()["totalResults"] == 0
    page = client.get(
        "/api/v1/scim/v2/Users",
        headers=_scim_headers(scim_client["scim"]),
        params={"startIndex": 2, "count": 1},
    ).json()
    assert page["totalResults"] == 4
    assert page["startIndex"] == 2
    assert page["itemsPerPage"] == 1
    assert page["Resources"][0]["id"] == second.json()["id"]
    inactive_filter = client.get(
        "/api/v1/scim/v2/Users",
        headers=_scim_headers(scim_client["scim"]),
        params={"filter": "active eq false"},
    ).json()
    assert inactive_filter["totalResults"] == 1
    email_filter = client.get(
        "/api/v1/scim/v2/Users",
        headers=_scim_headers(scim_client["scim"]),
        params={"filter": 'emails.value eq "one@example.test"'},
    )
    assert email_filter.status_code == 200
    assert email_filter.json()["totalResults"] == 1
    assert email_filter.json()["Resources"][0]["id"] == first_id
    invalid_page = client.get(
        "/api/v1/scim/v2/Users",
        headers=_scim_headers(scim_client["scim"]),
        params={"startIndex": 0},
    )
    assert invalid_page.status_code == 400
    assert invalid_page.json()["scimType"] == "invalidValue"
    assert invalid_page.json()["status"] == "400"
    assert invalid_page.headers["content-type"].startswith("application/scim+json")

    missing = client.get(
        "/api/v1/scim/v2/Users/not-a-user",
        headers=_scim_headers(scim_client["scim"]),
    )
    assert missing.status_code == 404
    assert missing.json()["schemas"] == ["urn:ietf:params:scim:api:messages:2.0:Error"]
    assert missing.headers["content-type"].startswith("application/scim+json")

    replacement = client.put(
        f"/api/v1/scim/v2/Users/{first_id}",
        headers=_scim_headers(scim_client["scim"]),
        json={
            "userName": "one@example.test",
            "displayName": "Replaced",
            "externalId": "external-one",
        },
    )
    assert replacement.status_code == 200
    assert replacement.json()["displayName"] == "Replaced"
    assert replacement.json()["externalId"] == "external-one"
    patched = client.patch(
        f"/api/v1/scim/v2/Users/{first_id}",
        headers=_scim_headers(scim_client["scim"]),
        json={"Operations": [{"op": "replace", "value": {"displayName": "Okta"}}]},
    )
    assert patched.status_code == 200
    assert patched.json()["displayName"] == "Okta"
    immutable = client.patch(
        f"/api/v1/scim/v2/Users/{first_id}",
        headers=_scim_headers(scim_client["scim"]),
        json={"Operations": [{"op": "replace", "path": "userName", "value": "new@example.test"}]},
    )
    assert immutable.status_code == 400
    assert immutable.json()["scimType"] == "mutability"
    invalid_filter = client.get(
        "/api/v1/scim/v2/Users",
        headers=_scim_headers(scim_client["scim"]),
        params={"filter": 'password eq "not-supported"'},
    )
    assert invalid_filter.status_code == 400
    assert invalid_filter.json()["scimType"] == "invalidFilter"
    assert third.status_code == 201


def test_deprovision_reactivation_and_delete(scim_client, monkeypatch):
    client = scim_client["client"]
    modules = scim_client["modules"]
    email = "deprovision@example.test"
    response = _create_user(scim_client, email)
    user_id = response.json()["id"]
    integrations_store = modules["integrations_store"]
    personal = integrations_store.create_api_key(
        "personal",
        email,
        role="editor",
        scopes=["connector.read"],
    )
    service_account = integrations_store.create_api_key(
        "persistent service account",
        email,
        role="editor",
        scopes=["connector.read"],
        kind="service_account",
    )
    from services.auth_service import create_token

    session_token, _ = create_token(email)
    connector_path = "/api/v1/connectors/"
    assert client.get(connector_path, headers=_auth(session_token)).status_code != 401

    deactivated = client.patch(
        f"/api/v1/scim/v2/Users/{user_id}",
        headers=_scim_headers(scim_client["scim"]),
        json={"Operations": [{"op": "Replace", "path": "active", "value": "False"}]},
    )
    assert deactivated.status_code == 200
    assert deactivated.json()["active"] is False
    assert client.get(connector_path, headers=_auth(session_token)).status_code == 401
    assert client.get(connector_path, headers=_auth(personal["key"])).status_code == 401
    assert client.get(connector_path, headers=_auth(service_account["key"])).status_code != 401

    from src.routers import auth_router
    from fastapi import HTTPException

    monkeypatch.setenv("DATAFLOW_SSO_AUTO_PROVISION", "1")
    monkeypatch.setenv("DATAFLOW_SSO_ALLOWED_DOMAINS", "example.test")
    with pytest.raises(HTTPException) as error:
        auth_router._require_sso_authorization(email)
    assert error.value.status_code == 403
    assert error.value.detail == "account_disabled"

    audit_events = modules["audit_log"].list_audit_events(limit=100)
    deprovision = [
        event
        for event in audit_events
        if event.get("action") == "scim.user.deprovision"
        and event.get("details", {}).get("scim_id") == user_id
    ]
    assert len(deprovision) == 1
    assert deprovision[0]["details"]["sessions_revoked"] == 1
    assert deprovision[0]["details"]["api_keys_revoked"] == [personal["id"]]
    assert deprovision[0]["details"]["kept_service_accounts"] == 1

    reactivated = client.patch(
        f"/api/v1/scim/v2/Users/{user_id}",
        headers=_scim_headers(scim_client["scim"]),
        json={"Operations": [{"op": "Replace", "path": "active", "value": "True"}]},
    )
    assert reactivated.status_code == 200
    new_session, _ = create_token(email)
    assert client.get(connector_path, headers=_auth(new_session)).status_code != 401
    assert client.get(connector_path, headers=_auth(personal["key"])).status_code == 401

    deleted_email = "deleted@example.test"
    deleted = _create_user(scim_client, deleted_email)
    deleted_id = deleted.json()["id"]
    delete_response = client.delete(
        f"/api/v1/scim/v2/Users/{deleted_id}",
        headers=_scim_headers(scim_client["scim"]),
    )
    assert delete_response.status_code == 204
    assert modules["user_store"].get_user(deleted_email) is None
    assert client.get(
        f"/api/v1/scim/v2/Users/{deleted_id}",
        headers=_scim_headers(scim_client["scim"]),
    ).status_code == 404


def test_deprovision_retry_completes_after_a_step_fails(scim_client, monkeypatch):
    client = scim_client["client"]
    modules = scim_client["modules"]
    user = _create_user(scim_client, "retry-deprovision@example.test").json()
    personal = modules["integrations_store"].create_api_key(
        "retry personal",
        user["userName"],
        role="editor",
        scopes=["connector.read"],
    )
    from services.auth_service import create_token

    session_token, _ = create_token(user["userName"])
    auth_sessions = modules["auth_sessions"]
    revoke_all = auth_sessions.revoke_all_for_email
    failed = False

    def fail_once(email):
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("simulated session store failure")
        return revoke_all(email)

    monkeypatch.setattr(auth_sessions, "revoke_all_for_email", fail_once)
    first = client.patch(
        f"/api/v1/scim/v2/Users/{user['id']}",
        headers=_scim_headers(scim_client["scim"]),
        json={"Operations": [{"op": "Replace", "path": "active", "value": "False"}]},
    )
    assert first.status_code == 500
    assert first.headers["content-type"].startswith("application/scim+json")
    assert modules["scim_store"].get_user(user["id"])["active"] is True

    retried = client.patch(
        f"/api/v1/scim/v2/Users/{user['id']}",
        headers=_scim_headers(scim_client["scim"]),
        json={"Operations": [{"op": "Replace", "path": "active", "value": "False"}]},
    )
    assert retried.status_code == 200, retried.text
    assert retried.json()["active"] is False
    assert client.get("/api/v1/connectors/", headers=_auth(session_token)).status_code == 401
    assert client.get("/api/v1/connectors/", headers=_auth(personal["key"])).status_code == 401

    events = modules["audit_log"].list_audit_events(limit=100)
    failures = [
        event for event in events
        if event.get("action") == "scim.user.deprovision.failed"
        and event.get("details", {}).get("scim_id") == user["id"]
    ]
    successes = [
        event for event in events
        if event.get("action") == "scim.user.deprovision"
        and event.get("details", {}).get("scim_id") == user["id"]
    ]
    assert len(failures) == len(successes) == 1
    assert failures[0]["details"]["step"] == "revoke_sessions"


def test_group_role_sync_manual_membership_last_admin_and_restart(scim_client):
    client = scim_client["client"]
    team_store = scim_client["modules"]["team_store"]
    from services import scim_store

    owner = "workspace-owner@example.test"
    workspace = team_store.create_workspace(name="SCIM roles", created_by=owner)
    last_owner = "last-admin-owner@example.test"
    last_workspace = team_store.create_workspace(name="SCIM last admin", created_by=last_owner)
    mappings = {
        "viewer group": {"workspace_id": workspace.id, "role": "viewer"},
        "editor group": {"workspace_id": workspace.id, "role": "editor"},
        "admin group": {"workspace_id": workspace.id, "role": "admin"},
        "manual group": {"workspace_id": workspace.id, "role": "viewer"},
        "put group": {"workspace_id": workspace.id, "role": "viewer"},
        "rename me": {"workspace_id": workspace.id, "role": "viewer"},
        "renamed group": {"workspace_id": workspace.id, "role": "editor"},
        "last group": {"workspace_id": last_workspace.id, "role": "admin"},
    }
    update = client.put(
        "/api/v1/iam/scim/group-mappings",
        headers=_auth(scim_client["admin"]),
        json={"mappings": mappings},
    )
    assert update.status_code == 200, update.text
    invalid_mapping = client.put(
        "/api/v1/iam/scim/group-mappings",
        headers=_auth(scim_client["admin"]),
        json={
            "mappings": {
                "invalid role": {
                    "workspace_id": workspace.id,
                    "role": "owner",
                }
            }
        },
    )
    assert invalid_mapping.status_code == 400

    user = _create_user(scim_client, "group-member@example.test").json()
    viewer = _create_group(scim_client, "Viewer Group")
    editor = _create_group(scim_client, "Editor Group")
    admin = _create_group(scim_client, "Admin Group")
    for group in (viewer, editor, admin):
        added = _patch_group(
            scim_client,
            group["id"],
            {"op": "add", "path": "members", "value": [{"value": user["id"]}]},
        )
        assert added.status_code == 200, added.text
    assert team_store.get_workspace_role(
        workspace_id=workspace.id, email=user["userName"]
    ) == "admin"
    removed_admin = _patch_group(
        scim_client,
        admin["id"],
        {
            "op": "Remove",
            "path": f'members[value eq "{user["id"]}"]',
        },
    )
    assert removed_admin.status_code == 200
    assert team_store.get_workspace_role(
        workspace_id=workspace.id, email=user["userName"]
    ) == "editor"
    _patch_group(scim_client, editor["id"], {"op": "remove", "path": "members", "value": [user["id"]]})
    assert team_store.get_workspace_role(
        workspace_id=workspace.id, email=user["userName"]
    ) == "viewer"
    _patch_group(scim_client, viewer["id"], {"op": "remove", "path": "members", "value": [user["id"]]})
    assert team_store.get_workspace_role(
        workspace_id=workspace.id, email=user["userName"]
    ) in (None, "")
    put_user = _create_user(scim_client, "put-member@example.test").json()
    put_group = _create_group(
        scim_client,
        "Put Group",
        members=[{"value": put_user["id"]}],
    )
    assert team_store.get_workspace_role(
        workspace_id=workspace.id, email=put_user["userName"]
    ) == "viewer"
    put_response = client.put(
        f"/api/v1/scim/v2/Groups/{put_group['id']}",
        headers=_scim_headers(scim_client["scim"]),
        json={"displayName": "Put Group"},
    )
    assert put_response.status_code == 200
    assert put_response.json()["members"] == []
    assert team_store.get_workspace_role(
        workspace_id=workspace.id, email=put_user["userName"]
    ) in (None, "")
    rename_user = _create_user(scim_client, "rename-member@example.test").json()
    rename_group = _create_group(
        scim_client,
        "Rename Me",
        members=[{"value": rename_user["id"]}],
    )
    assert team_store.get_workspace_role(
        workspace_id=workspace.id, email=rename_user["userName"]
    ) == "viewer"
    renamed = client.put(
        f"/api/v1/scim/v2/Groups/{rename_group['id']}",
        headers=_scim_headers(scim_client["scim"]),
        json={
            "displayName": "Renamed Group",
            "members": [{"value": rename_user["id"]}],
        },
    )
    assert renamed.status_code == 200, renamed.text
    assert team_store.get_workspace_role(
        workspace_id=workspace.id, email=rename_user["userName"]
    ) == "editor"

    manual_user = _create_user(scim_client, "manual-member@example.test").json()
    team_store.add_workspace_member(
        workspace_id=workspace.id,
        email=manual_user["userName"],
        role="editor",
        added_by=owner,
        actor_is_platform_admin=True,
    )
    manual_group = _create_group(scim_client, "Manual Group")
    _patch_group(
        scim_client,
        manual_group["id"],
        {"op": "add", "path": "members", "value": [{"value": manual_user["id"]}]},
    )
    _patch_group(
        scim_client,
        manual_group["id"],
        {"op": "remove", "path": "members", "value": [manual_user["id"]]},
    )
    assert team_store.get_workspace_role(
        workspace_id=workspace.id, email=manual_user["userName"]
    ) == "editor"

    last_user = _create_user(scim_client, "last-admin@example.test").json()
    last_group = _create_group(scim_client, "Last Group")
    _patch_group(
        scim_client,
        last_group["id"],
        {"op": "add", "path": "members", "value": [{"value": last_user["id"]}]},
    )
    second_last_user = _create_user(scim_client, "second-last-user@example.test").json()
    assert _patch_group(
        scim_client,
        last_group["id"],
        {
            "op": "add",
            "path": "members",
            "value": [{"value": second_last_user["id"]}],
        },
    ).status_code == 200
    team_store.remove_workspace_member(
        workspace_id=last_workspace.id,
        email=last_owner,
        removed_by=last_owner,
    )
    before = scim_store.get_group(last_group["id"])
    rejected = _patch_group(
        scim_client,
        last_group["id"],
        {"op": "replace", "path": "members", "value": []},
    )
    assert rejected.status_code == 409
    assert scim_store.get_group(last_group["id"]) == before
    assert team_store.get_workspace_role(
        workspace_id=last_workspace.id, email=last_user["userName"]
    ) == "admin"
    assert team_store.get_workspace_role(
        workspace_id=last_workspace.id, email=second_last_user["userName"]
    ) == "admin"

    listed = client.get(
        "/api/v1/scim/v2/Groups",
        headers=_scim_headers(scim_client["scim"]),
        params={"filter": 'displayName eq "VIEWER GROUP"'},
    )
    assert listed.status_code == 200
    assert listed.json()["totalResults"] == 1
    assert listed.json()["Resources"][0]["id"] == viewer["id"]
    duplicate = client.post(
        "/api/v1/scim/v2/Groups",
        headers=_scim_headers(scim_client["scim"]),
        json={"displayName": "vIeWeR gRoUp"},
    )
    assert duplicate.status_code == 409
    assert duplicate.json()["scimType"] == "uniqueness"

    deleted_group = _create_group(scim_client, "Delete Group")
    deletion = client.delete(
        f"/api/v1/scim/v2/Groups/{deleted_group['id']}",
        headers=_scim_headers(scim_client["scim"]),
    )
    assert deletion.status_code == 204
    assert client.get(
        f"/api/v1/scim/v2/Groups/{deleted_group['id']}",
        headers=_scim_headers(scim_client["scim"]),
    ).status_code == 404

    audit_actions = {
        event.get("action")
        for event in scim_client["modules"]["audit_log"].list_audit_events(limit=100)
    }
    assert {
        "scim.group.member_add",
        "scim.group.member_remove",
        "scim.group.role_change",
    } <= audit_actions

    persisted_id = viewer["id"]
    store_path = scim_store.STORE_PATH
    mappings_before_restart = scim_store.get_group_mappings()
    managed_before_restart = scim_store.get_managed_memberships()
    assert managed_before_restart
    importlib.reload(scim_store)
    scim_store.STORE_PATH = store_path
    scim_store.mongo_database = lambda: None
    assert scim_store.get_group_mappings() == mappings_before_restart
    assert scim_store.get_managed_memberships() == managed_before_restart
    assert client.get(
        f"/api/v1/scim/v2/Groups/{persisted_id}",
        headers=_scim_headers(scim_client["scim"]),
    ).status_code == 200


LIVE_MONGO_URI = os.getenv("DATAFLOW_LIVE_MONGO_URI", "").strip()


@pytest.mark.skipif(not LIVE_MONGO_URI, reason="DATAFLOW_LIVE_MONGO_URI is not configured")
def test_live_mongo_scim_user_deprovision_and_group_sync(monkeypatch, tmp_path):
    from pymongo import MongoClient

    from services import (
        audit_log,
        auth_sessions,
        integrations_store,
        scim_service,
        scim_store,
        team_store,
        user_store,
    )

    client = MongoClient(LIVE_MONGO_URI, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    db = client[f"dataflow_scim_it_{uuid.uuid4().hex}"]
    monkeypatch.setattr(scim_store, "mongo_database", lambda: db)
    scim_store._PREPARED_DATABASES = set()
    monkeypatch.setattr(user_store, "mongo_database", lambda: db)
    monkeypatch.setattr(team_store, "_database", lambda: db)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(auth_sessions, "_path", lambda: tmp_path / "sessions.json")
    email = f"live-scim-{uuid.uuid4().hex}@example.test"
    try:
        user = scim_service.create_user(
            {"userName": email, "displayName": "Live SCIM"},
            actor="live-test",
        )
        workspace = team_store.create_workspace(name="Live SCIM", created_by="live-owner@example.test")
        scim_service.set_group_mappings(
            {"live group": {"workspace_id": workspace.id, "role": "editor"}}
        )
        group = scim_service.create_group({"displayName": "Live Group"}, actor="live-test")
        updated = scim_service.patch_group(
            group["id"],
            {
                "Operations": [
                    {"op": "add", "path": "members", "value": [{"value": user["id"]}]}
                ]
            },
            actor="live-test",
        )
        assert user_store.get_user(email)["status"] == "active"
        assert updated["members"][0]["value"] == user["id"]
        assert team_store.get_workspace_role(workspace_id=workspace.id, email=email) == "editor"
        scim_service.deprovision_user(email, actor="live-test")
        assert user_store.get_user(email)["status"] == "disabled"
    finally:
        client.drop_database(db.name)
        client.close()
