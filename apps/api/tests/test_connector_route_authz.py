"""Saved-connector ids are authorized against the connector's own workspace (#333 on TenantScope).

Saved connectors never resolve across workspaces, with or without a header.

An empty or missing ``X-Workspace-Id`` used to mean "every workspace" at the
connector store, so any signed-in user could query, introspect, preflight or
Pilot-sample another tenant's connector with its stored credentials.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import audit_log, connector_store, team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits
from src.main import app

_ADMIN_EMAIL = "root-scope@example.com"
_ADMIN_PASSWORD = "Bootstrap-Admin-2026"


@pytest.fixture
def scoped_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.delenv("DATAFLOW_REQUIRE_WORKSPACE", raising=False)
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", _ADMIN_EMAIL)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", _ADMIN_PASSWORD)
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "scope-test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    reset_auth_rate_limits()
    return tmp_path


def _admin() -> TestClient:
    client = TestClient(app)
    response = client.post(
        "/api/v1/auth/login", json={"email": _ADMIN_EMAIL, "password": _ADMIN_PASSWORD}
    )
    assert response.status_code == 200, response.text
    client.headers["Authorization"] = f"Bearer {response.json()['token']}"
    return client


def _workspace(admin: TestClient, name: str) -> str:
    response = admin.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


def _member(admin: TestClient, workspace_id: str, email: str, role: str = "editor") -> TestClient:
    created = admin.post(
        "/api/v1/team/users",
        json={
            "email": email,
            "platform_role": "member",
            "workspace_id": workspace_id,
            "workspace_role": role,
        },
    )
    assert created.status_code == 200, created.text
    client = TestClient(app)
    login = client.post(
        "/api/v1/auth/login",
        json={"email": email, "password": created.json()["temporary_password"]},
    )
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return client


def _sqlite(tmp_path: Path, name: str) -> str:
    # connectors may not open files in the service data dir, only its sqlite/ volume
    (tmp_path / "sqlite").mkdir(exist_ok=True)
    db_path = tmp_path / "sqlite" / f"{name}.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE secrets (id INTEGER PRIMARY KEY, value TEXT)")
    conn.execute("INSERT INTO secrets (id, value) VALUES (1, 'TENANT_B_ROW_SENTINEL')")
    conn.commit()
    conn.close()
    return f"sqlite:///{db_path}"


@pytest.fixture
def tenants(scoped_env):
    admin = _admin()
    ws_a = _workspace(admin, "Scope workspace A")
    ws_b = _workspace(admin, "Scope workspace B")
    mallory = _member(admin, ws_a, "mallory-scope@example.com")
    foreign = connector_store.create_connector(
        {
            "name": "acme-prod-db",
            "type": "sqlite",
            "role": "both",
            "connection_string": _sqlite(scoped_env, "tenant_b"),
            "workspace_id": ws_b,
        }
    )
    own = connector_store.create_connector(
        {
            "name": "mallory-db",
            "type": "sqlite",
            "role": "both",
            "connection_string": _sqlite(scoped_env, "tenant_a"),
            "workspace_id": ws_a,
        }
    )
    return {"admin": admin, "mallory": mallory, "ws_a": ws_a, "ws_b": ws_b, "foreign": foreign, "own": own}


# ── HTTP surfaces ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("header", ["", "A", "B"])
def test_query_execute_refuses_foreign_connector(tenants, header):
    mallory = tenants["mallory"]
    headers = {"X-Workspace-Id": {"": "", "A": tenants["ws_a"], "B": tenants["ws_b"]}[header]}
    response = mallory.post(
        "/api/v1/query/execute",
        json={"connector_id": tenants["foreign"].id, "query": "SELECT * FROM secrets", "limit": 10},
        headers={k: v for k, v in headers.items() if v},
    )
    assert response.status_code in (403, 404), response.text
    assert "TENANT_B_ROW_SENTINEL" not in response.text


def test_query_execute_still_serves_own_connector_without_header(tenants):
    response = tenants["mallory"].post(
        "/api/v1/query/execute",
        json={"connector_id": tenants["own"].id, "query": "SELECT * FROM secrets", "limit": 10},
    )
    assert response.status_code == 200, response.text


def test_query_export_refuses_foreign_destination(tenants):
    response = tenants["mallory"].post(
        "/api/v1/query/export",
        json={
            "connector_id": tenants["own"].id,
            "query": "SELECT * FROM secrets",
            "destination_connector_id": tenants["foreign"].id,
            "destination": "stolen",
        },
    )
    assert response.status_code == 404, response.text


def test_query_schema_refuses_foreign_connector(tenants):
    response = tenants["mallory"].post(
        "/api/v1/query/schema", json={"connector_id": tenants["foreign"].id}
    )
    assert response.status_code == 404, response.text


def test_transfer_introspect_refuses_foreign_connector(tenants):
    endpoint = {"kind": "database", "format": "sqlite", "connector_id": tenants["foreign"].id}
    for path in ("/api/v1/transfer/introspect", "/api/v1/transfer/route", "/api/v1/transfer/analyze"):
        response = tenants["mallory"].post(
            path, json={"source": endpoint, "destination": {"kind": "file_export", "format": "json"}}
        )
        assert response.status_code == 404, (path, response.text)
        assert "TENANT_B_ROW_SENTINEL" not in response.text


def test_transfer_execute_refuses_foreign_destination(tenants):
    response = tenants["mallory"].post(
        "/api/v1/transfer/execute",
        json={
            "source": {"kind": "database", "format": "sqlite", "connector_id": tenants["own"].id, "table": "secrets"},
            "destination": {"kind": "database", "format": "sqlite", "connector_id": tenants["foreign"].id, "table": "x"},
        },
    )
    assert response.status_code == 404, response.text


def test_preflight_refuses_foreign_connector(tenants):
    response = tenants["mallory"].post(
        "/api/v1/preflight/run",
        json={
            "connector_id": tenants["foreign"].id,
            "dest_type": "sqlite",
            "dest_table": "secrets",
            "columns": ["id"],
            "mappings": [{"source": "id", "target": "id"}],
        },
    )
    assert response.status_code == 404, response.text


def test_viewer_of_b_can_read_but_not_write_into_bs_connector(tenants):
    from services import team_store

    team_store.add_workspace_member(
        workspace_id=tenants["ws_b"], email="mallory-scope@example.com", role="viewer",
        added_by=_ADMIN_EMAIL, actor_is_platform_admin=True,
    )
    mallory, foreign, own = tenants["mallory"], tenants["foreign"], tenants["own"]
    read = mallory.post(
        "/api/v1/query/execute",
        json={"connector_id": foreign.id, "query": "SELECT * FROM secrets", "limit": 5},
    )
    assert read.status_code == 200, read.text  # a member may read the workspace's data
    export = mallory.post(
        "/api/v1/query/export",
        json={
            "connector_id": own.id,
            "query": "SELECT * FROM secrets",
            "destination_connector_id": foreign.id,
            "destination": "stolen",
        },
    )
    assert export.status_code == 403, export.text
    db_path = str(foreign.connection_string).replace("sqlite:///", "")
    tables = {r[0] for r in sqlite3.connect(db_path).execute("select name from sqlite_master")}
    assert "stolen" not in tables
