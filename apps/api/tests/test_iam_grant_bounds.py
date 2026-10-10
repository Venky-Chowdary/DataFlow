"""Real-app checks that IAM grants never exceed the caller's permissions."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def grant_app(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-iam-grants-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))

    from services import audit_log, integrations_store, scim_store, team_store
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(scim_store, "mongo_database", lambda: None)
    monkeypatch.setattr(scim_store, "STORE_PATH", tmp_path / "scim.json")
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    from services import team_store as store
    from services import rbac
    from src.main import app

    full_admin = integrations_store.create_api_key(
        "Full admin", "admin@example.test", role="admin"
    )
    scoped_iam = integrations_store.create_api_key(
        "Scoped IAM", "scoped@example.test", role="admin", scopes=["iam.manage"]
    )
    scoped_iam_and_read = integrations_store.create_api_key(
        "Scoped IAM and read",
        "read@example.test",
        role="admin",
        scopes=["iam.manage", "job.read"],
    )
    scoped_workspace = integrations_store.create_api_key(
        "Scoped workspace manager",
        "workspace@example.test",
        role="admin",
        scopes=["iam.manage", "workspace.manage"],
    )
    workspace = store.create_workspace(name="Grant bounds", created_by="admin@example.test")

    with TestClient(app) as client:
        yield {
            "client": client,
            "admin": full_admin["key"],
            "scoped_iam": scoped_iam["key"],
            "scoped_iam_and_read": scoped_iam_and_read["key"],
            "scoped_workspace": scoped_workspace["key"],
            "workspace_id": workspace.id,
            "integrations_store": integrations_store,
            "rbac": rbac,
            "audit_log": audit_log,
        }


def _auth(key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {key}"}


def _assert_denied(response, context, *, route: str, missing: set[str]):
    assert response.status_code == 403, response.text
    assert response.json()["detail"] == (
        "Cannot grant permissions you do not hold: " + ", ".join(sorted(missing))
    )
    events = [
        json.loads(line)
        for line in context["audit_log"].STORE_PATH.read_text(encoding="utf-8").splitlines()
    ]
    event = next(
        item
        for item in reversed(events)
        if item["action"] == "iam.privilege_escalation.denied"
        and item["details"]["route"] == route
    )
    assert event["level"] == "warn"
    assert event["details"]["missing"] == sorted(missing)
    assert event["details"]["caller"]
    assert "secret" not in json.dumps(event).lower()


def test_scoped_iam_key_cannot_rotate_an_unscoped_admin_key(grant_app):
    client = grant_app["client"]
    store = grant_app["integrations_store"]
    target = store.create_api_key("Target admin", "target@example.test", role="admin")
    response = client.post(
        f"/api/v1/iam/api-keys/{target['id']}/rotate",
        headers=_auth(grant_app["scoped_iam"]),
        json={},
    )
    _assert_denied(
        response,
        grant_app,
        route=f"POST /api/v1/iam/api-keys/{target['id']}/rotate",
        missing=set(grant_app["rbac"].role_permissions("admin")) - {"iam.manage"},
    )
    source = next(row for row in store.load_api_key_records() if row["id"] == target["id"])
    assert source.get("rotated_to") is None
    assert not any(row.get("rotated_from") == target["id"] for row in store.load_api_key_records())


def test_scoped_iam_key_cannot_grant_job_run(grant_app):
    response = grant_app["client"].post(
        "/api/v1/iam/service-accounts",
        headers=_auth(grant_app["scoped_iam"]),
        json={"name": "Runner", "role": "admin", "scopes": ["job.run"]},
    )
    _assert_denied(
        response,
        grant_app,
        route="POST /api/v1/iam/service-accounts",
        missing={"job.run"},
    )


def test_scoped_iam_key_cannot_mint_scim_token(grant_app):
    response = grant_app["client"].post(
        "/api/v1/iam/scim-token",
        headers=_auth(grant_app["scoped_iam"]),
        json={},
    )
    _assert_denied(
        response,
        grant_app,
        route="POST /api/v1/iam/scim-token",
        missing={"scim.provision"},
    )


def test_scoped_iam_key_cannot_map_group_to_workspace_admin(grant_app):
    response = grant_app["client"].put(
        "/api/v1/iam/scim/group-mappings",
        headers=_auth(grant_app["scoped_iam"]),
        json={
            "mappings": {
                "operators": {
                    "workspace_id": grant_app["workspace_id"],
                    "role": "admin",
                }
            }
        },
    )
    _assert_denied(
        response,
        grant_app,
        route="PUT /api/v1/iam/scim/group-mappings",
        missing=set(grant_app["rbac"].role_permissions("admin"))
        - {"iam.manage"},
    )


def test_workspace_manager_cannot_mint_an_unscoped_admin_key(grant_app):
    response = grant_app["client"].post(
        "/api/v1/workspace/api-keys",
        headers=_auth(grant_app["scoped_workspace"]),
        json={"name": "Escalated", "role": "admin"},
    )
    _assert_denied(
        response,
        grant_app,
        route="POST /api/v1/workspace/api-keys",
        missing=set(grant_app["rbac"].role_permissions("admin"))
        - {"iam.manage", "workspace.manage"},
    )


def test_scoped_caller_can_grant_a_subset_of_its_permissions(grant_app):
    response = grant_app["client"].post(
        "/api/v1/iam/service-accounts",
        headers=_auth(grant_app["scoped_iam_and_read"]),
        json={"name": "Reader", "role": "editor", "scopes": ["job.read"]},
    )
    assert response.status_code == 200, response.text
    assert response.json()["scopes"] == ["job.read"]


def test_full_admin_retains_all_grant_routes(grant_app):
    client = grant_app["client"]
    admin = _auth(grant_app["admin"])
    store = grant_app["integrations_store"]
    target = store.create_api_key("Target admin", "target@example.test", role="admin")

    rotated = client.post(
        f"/api/v1/iam/api-keys/{target['id']}/rotate", headers=admin, json={}
    )
    service_account = client.post(
        "/api/v1/iam/service-accounts",
        headers=admin,
        json={"name": "Runner", "role": "admin", "scopes": ["job.run"]},
    )
    scim_token = client.post("/api/v1/iam/scim-token", headers=admin, json={})
    mapping = client.put(
        "/api/v1/iam/scim/group-mappings",
        headers=admin,
        json={
            "mappings": {
                "operators": {
                    "workspace_id": grant_app["workspace_id"],
                    "role": "admin",
                }
            }
        },
    )
    workspace_key = client.post(
        "/api/v1/workspace/api-keys",
        headers=admin,
        json={"name": "Workspace admin key", "role": "admin"},
    )

    assert rotated.status_code == 200, rotated.text
    assert service_account.status_code == 200, service_account.text
    assert scim_token.status_code == 200, scim_token.text
    assert mapping.status_code == 200, mapping.text
    assert workspace_key.status_code == 200, workspace_key.text


def test_grant_bound_helpers_report_sorted_missing_permissions():
    from services.rbac import PrivilegeEscalation, assert_grant_within

    with pytest.raises(PrivilegeEscalation) as exc_info:
        assert_grant_within({"job.read"}, {"job.run", "connector.write"})
    assert exc_info.value.missing == ("connector.write", "job.run")


def test_auth_disabled_allows_admin_key_creation(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))

    from services import audit_log, integrations_store
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "auth_required", lambda: False)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)

    from src.main import app

    with TestClient(app) as client:
        workspace_key = client.post(
            "/api/v1/workspace/api-keys",
            json={"name": "Operator admin", "role": "admin"},
        )
        service_account = client.post(
            "/api/v1/iam/service-accounts",
            json={"name": "Service admin", "role": "admin", "scopes": ["job.run"]},
        )

    assert [workspace_key.status_code, service_account.status_code] == [200, 200], (
        workspace_key.text,
        service_account.text,
    )
