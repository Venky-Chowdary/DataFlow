"""Cross-workspace contract lifecycle must 404, not sign or export the other tenant.

Same class as D36 / schedule isolation: list already filtered by X-Workspace-Id,
but id-addressed sign / deprecate / export / breaker / import used to take a
UUID as enough. A workspace's schema agreement is not a shared object.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits
from src.main import app
from src.services.contract_store import get_contract_store, reset_contract_store
from src.services.data_contract import ColumnRule, ContractStatus, DataContract


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONTRACTS_PATH", str(tmp_path / "contracts.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    reset_auth_rate_limits()
    reset_contract_store()
    yield tmp_path
    reset_contract_store()


def _admin(isolated) -> TestClient:
    c = TestClient(app)
    login = c.post(
        "/api/v1/auth/login",
        json={"email": "root@example.com", "password": "Bootstrap-Admin-2026"},
    )
    assert login.status_code == 200, login.text
    c.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return c


def _workspace(client: TestClient, name: str) -> str:
    response = client.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


def _member(admin: TestClient, *, email: str, workspace_id: str) -> TestClient:
    created = admin.post(
        "/api/v1/team/users",
        json={
            "email": email,
            "platform_role": "member",
            "workspace_id": workspace_id,
            "workspace_role": "editor",
        },
    )
    assert created.status_code == 200, created.text
    issued = created.json()["temporary_password"]
    peer = TestClient(app)
    login = peer.post("/api/v1/auth/login", json={"email": email, "password": issued})
    assert login.status_code == 200, login.text
    peer.headers["Authorization"] = f"Bearer {login.json()['token']}"
    peer.headers["X-Workspace-Id"] = workspace_id
    return peer


def _agreement(workspace_id: str, name: str = "orders-v1") -> DataContract:
    contract = DataContract(
        name=name,
        columns=[
            ColumnRule(source_name="id", target_name="id", source_type="INTEGER", target_type="INTEGER"),
        ],
        metadata={"workspace_id": workspace_id},
    )
    get_contract_store().save_contract(contract)
    return contract


_LIFECYCLE = (
    ("GET", "/api/v1/contracts/{id}", None),
    ("GET", "/api/v1/contracts/{id}/history", None),
    ("GET", "/api/v1/contracts/{id}/breaker", None),
    ("GET", "/api/v1/contracts/{id}/export", None),
    ("POST", "/api/v1/contracts/{id}/sign", {"strict": True}),
    ("POST", "/api/v1/contracts/{id}/deprecate", None),
    ("POST", "/api/v1/contracts/{id}/breaker/reset", None),
    (
        "POST",
        "/api/v1/contracts/test",
        {
            "contract_id": "{id}",
            "source": {"kind": "file", "format": "csv"},
            "destination": {"kind": "database", "format": "postgresql"},
            "column_types": {"id": "INTEGER"},
        },
    ),
)


def test_editor_cannot_sign_or_export_another_workspace_contract(isolated):
    admin = _admin(isolated)
    mine = _workspace(admin, "Mine")
    theirs = _workspace(admin, "Theirs")
    foreign = _agreement(theirs, "Theirs agreement")
    editor = _member(admin, email="ed@example.com", workspace_id=mine)

    listed = editor.get("/api/v1/contracts")
    assert listed.status_code == 200, listed.text
    assert all(row["id"] != foreign.id for row in listed.json()["contracts"])

    for method, path, body in _LIFECYCLE:
        url = path.replace("{id}", foreign.id)
        json_body = None
        if isinstance(body, dict):
            json_body = {k: (foreign.id if v == "{id}" else v) for k, v in body.items()}
        resp = editor.request(method, url, json=json_body)
        assert resp.status_code == 404, (method, path, resp.status_code, resp.text)

    still = get_contract_store().get_contract(foreign.id)
    assert still is not None
    assert still.status == ContractStatus.DRAFT
    assert still.name == "Theirs agreement"


def test_sibling_header_cannot_address_other_workspace_contract(isolated):
    """Same actor, two workspaces: X-Workspace-Id is the tenant boundary."""
    admin = _admin(isolated)
    mine = _workspace(admin, "Mine-sib")
    sibling = _workspace(admin, "Sibling")
    mine_contract = _agreement(mine, "Mine agreement")
    admin.headers["X-Workspace-Id"] = sibling

    listed = admin.get("/api/v1/contracts")
    assert listed.status_code == 200, listed.text
    assert all(row["id"] != mine_contract.id for row in listed.json()["contracts"])

    sign = admin.post(f"/api/v1/contracts/{mine_contract.id}/sign", json={"strict": True})
    assert sign.status_code == 404, sign.text
    export = admin.get(f"/api/v1/contracts/{mine_contract.id}/export")
    assert export.status_code == 404, export.text

    still = get_contract_store().get_contract(mine_contract.id)
    assert still is not None
    assert still.status == ContractStatus.DRAFT

    admin.headers["X-Workspace-Id"] = mine
    ok = admin.post(f"/api/v1/contracts/{mine_contract.id}/sign", json={"strict": True})
    assert ok.status_code == 200, ok.text
    assert ok.json()["status"] == "signed"


def test_unscoped_contract_hidden_when_isolation_required(isolated):
    admin = _admin(isolated)
    ws = _workspace(admin, "Acme")
    legacy = DataContract(
        name="Legacy unscoped",
        columns=[ColumnRule(source_name="id", target_name="id", source_type="INTEGER", target_type="INTEGER")],
    )
    get_contract_store().save_contract(legacy)
    editor = _member(admin, email="ed2@example.com", workspace_id=ws)

    listed = editor.get("/api/v1/contracts")
    assert listed.status_code == 200, listed.text
    assert all(row["id"] != legacy.id for row in listed.json()["contracts"])

    get = editor.get(f"/api/v1/contracts/{legacy.id}")
    assert get.status_code == 404, get.text


def test_import_stamps_write_workspace_and_refuses_foreign_id(isolated):
    admin = _admin(isolated)
    mine = _workspace(admin, "Mine-imp")
    theirs = _workspace(admin, "Theirs-imp")
    foreign = _agreement(theirs, "Keep me")
    editor = _member(admin, email="imp@example.com", workspace_id=mine)

    created = editor.post(
        "/api/v1/contracts/import",
        json={
            "kind": "DataContract",
            "spec": {
                "name": "imported-ours",
                "columns": [
                    {"source_name": "id", "target_name": "id", "source_type": "INTEGER", "target_type": "INTEGER"}
                ],
            },
        },
    )
    assert created.status_code == 200, created.text
    assert created.json()["status"] == "draft"
    assert created.json()["metadata"]["workspace_id"] == mine

    hijack = editor.post(
        "/api/v1/contracts/import",
        json={
            "kind": "DataContract",
            "spec": {
                "id": foreign.id,
                "name": "hijack",
                "columns": [
                    {"source_name": "id", "target_name": "id", "source_type": "INTEGER", "target_type": "INTEGER"}
                ],
            },
        },
    )
    assert hijack.status_code == 404, hijack.text
    still = get_contract_store().get_contract(foreign.id)
    assert still is not None
    assert still.name == "Keep me"
    assert (still.metadata or {}).get("workspace_id") == theirs
