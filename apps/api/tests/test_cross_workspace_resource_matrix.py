"""Cross-workspace checks that the route registry does not express.

Every id-addressed route is scoped in ONE registry,
``tests/fixtures/route_tenancy_scoping.json``, exercised by
``tests/test_tenancy_route_scoping.py`` (two workspaces, real seeds, an owner
control per route, an API-key caller and a viewer-elsewhere caller). Security
review M4 removed this file's own id-route loop with its skip/allow lists and
sentinel ids, whose stale reasons contradicted that registry; ids named in a
request body moved to the registry suite too. What stays here is the
preflight-run read by an editor of the other workspace.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.auth_rate_limit import reset_auth_rate_limits
from services import (
    audit_log,
    connector_store,
    contract_store,
    resource_acl,
    schedule_store,
    team_store,
    user_store,
)
from src.main import app

@pytest.fixture
def resource_matrix(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONTRACTS_PATH", str(tmp_path / "contracts.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root-matrix@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "matrix-test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(resource_acl, "STORE_PATH", tmp_path / "resource-acls.jsonl")
    contract_store.reset_contract_store()
    reset_auth_rate_limits()
    yield tmp_path
    contract_store.reset_contract_store()


def _admin() -> TestClient:
    client = TestClient(app)
    response = client.post(
        "/api/v1/auth/login",
        json={
            "email": "root-matrix@example.com",
            "password": "Bootstrap-Admin-2026",
        },
    )
    assert response.status_code == 200, response.text
    client.headers["Authorization"] = f"Bearer {response.json()['token']}"
    return client


def _workspace(admin: TestClient, name: str) -> str:
    response = admin.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


def _member(admin: TestClient, workspace_id: str) -> TestClient:
    created = admin.post(
        "/api/v1/team/users",
        json={
            "email": "matrix-editor@example.com",
            "platform_role": "member",
            "workspace_id": workspace_id,
            "workspace_role": "editor",
        },
    )
    assert created.status_code == 200, created.text
    client = TestClient(app)
    login = client.post(
        "/api/v1/auth/login",
        json={
            "email": "matrix-editor@example.com",
            "password": created.json()["temporary_password"],
        },
    )
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    client.headers["X-Workspace-Id"] = workspace_id
    return client


def _request(member, method: str, path: str, *, body=None):
    correlation_id = f"matrix-{uuid4().hex}"
    headers = {
        "X-Workspace-Id": member.headers["X-Workspace-Id"],
        "X-Correlation-ID": correlation_id,
    }
    response = member.request(
        method,
        path,
        headers=headers,
        json=body if body is not None else None,
    )
    print(
        f"{method} {path}: status={response.status_code} "
        f"body={json.dumps(response.json(), sort_keys=True) if response.content else ''}"
    )
    return response


def test_preflight_run_id_is_not_visible_cross_workspace(resource_matrix, monkeypatch):
    admin = _admin()
    workspace_a = _workspace(admin, "Preflight workspace A")
    workspace_b = _workspace(admin, "Preflight workspace B")
    editor = _member(admin, workspace_a)
    from services import preflight_run_store

    monkeypatch.setattr(preflight_run_store, "STORE_PATH", resource_matrix / "preflight-runs.jsonl")
    run = preflight_run_store.save_preflight_run(
        {"passed": True, "blockers": []},
        source_label="B_PREFLIGHT_SENTINEL",
        dest_label="destination",
        route={"workspace_id": workspace_b},
    )
    response = _request(
        editor,
        "GET",
        f"/api/v1/preflight/runs/{run['run_id']}",
    )
    assert response.status_code in {403, 404}
    assert "B_PREFLIGHT_SENTINEL" not in response.text


def test_exit_listings_and_pack_verify_disclose_nothing_cross_workspace(resource_matrix, monkeypatch):
    """Reversibility Proof checks the id registry does not express (its id routes are
    declared in ``fixtures/route_tenancy_scoping.json``): the id-less exit reads are
    scoped, and verify -- which takes a pack, not an id -- discloses nothing of the
    workspace a forged pack names."""
    from services import exit_inventory, exit_store

    admin = _admin()
    workspace_a = _workspace(admin, "Exit workspace A")
    workspace_b = _workspace(admin, "Exit workspace B")
    editor = _member(admin, workspace_a)
    monkeypatch.setattr(exit_store, "STORE_PATH", resource_matrix / "exit_inventory.json")
    monkeypatch.setattr(exit_store, "_db", lambda: None)
    connector = connector_store.create_connector(
        {"name": "B_SAVED_CONNECTOR_SENTINEL", "type": "postgresql", "host": "b.example.com", "port": 5432,
         "database": "b", "workspace_id": workspace_b}
    )
    exit_system = exit_inventory.declare(
        {
            "name": "B_EXIT_SYSTEM_SENTINEL",
            "source": {"connector_id": connector.id},
            "tables": [{"name": "B_EXIT_TABLE_SENTINEL", "key_columns": ["id"]}],
            "exit_window_hours": 24,
        },
        workspace_id=workspace_b,
        actor="matrix",
    )
    # Owner control: B's own caller does see the system in the same listings.
    for listing in ("/api/v1/exit/readiness", "/api/v1/exit/systems"):
        owned = admin.get(listing, headers={"X-Workspace-Id": workspace_b})
        assert owned.status_code == 200 and "B_EXIT_SYSTEM_SENTINEL" in owned.text, owned.text
        listed = _request(editor, "GET", listing)
        assert listed.status_code == 200, listed.text
        assert "B_EXIT_SYSTEM_SENTINEL" not in listed.text and exit_system["id"] not in listed.text
    forged = _request(
        editor,
        "POST",
        "/api/v1/exit/evidence/verify",
        body={
            "kind": "exit_evidence",
            "system_id": exit_system["id"],
            "rehearsal_id": "exr_bmatrix00001",
            "content_sha256": "0" * 64,
            "signature": {"alg": "HMAC-SHA256", "key_id": "k", "value": "0" * 64},
        },
    )
    assert forged.status_code == 200, forged.text
    assert forged.json()["ok"] is False and forged.json()["signature_ok"] is False
    for sentinel in ("B_EXIT_SYSTEM_SENTINEL", "B_EXIT_TABLE_SENTINEL", "B_SAVED_CONNECTOR_SENTINEL"):
        assert sentinel not in forged.text
