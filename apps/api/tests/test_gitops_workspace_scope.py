from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.auth_rate_limit import reset_auth_rate_limits
from services import audit_log, contract_store, schedule_store, team_store, user_store
from services.data_contract import DataContract
from src.main import app


@pytest.fixture
def gitops_isolation(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONTRACTS_PATH", str(tmp_path / "contracts.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root-gitops@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "gitops-test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    contract_store.reset_contract_store()
    reset_auth_rate_limits()
    yield tmp_path
    contract_store.reset_contract_store()


def _admin() -> TestClient:
    client = TestClient(app)
    response = client.post(
        "/api/v1/auth/login",
        json={
            "email": "root-gitops@example.com",
            "password": "Bootstrap-Admin-2026",
        },
    )
    assert response.status_code == 200, response.text
    client.headers["Authorization"] = f"Bearer {response.json()['token']}"
    return client


def _workspace(client: TestClient, name: str) -> str:
    response = client.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


def _member(admin: TestClient, workspace_id: str) -> TestClient:
    created = admin.post(
        "/api/v1/team/users",
        json={
            "email": "gitops-editor@example.com",
            "platform_role": "member",
            "workspace_id": workspace_id,
            "workspace_role": "editor",
        },
    )
    assert created.status_code == 200, created.text
    peer = TestClient(app)
    login = peer.post(
        "/api/v1/auth/login",
        json={
            "email": "gitops-editor@example.com",
            "password": created.json()["temporary_password"],
        },
    )
    assert login.status_code == 200, login.text
    peer.headers["Authorization"] = f"Bearer {login.json()['token']}"
    peer.headers["X-Workspace-Id"] = workspace_id
    return peer


def test_gitops_plan_is_workspace_scoped(gitops_isolation):
    admin = _admin()
    workspace_a = _workspace(admin, "GitOps workspace A")
    workspace_b = _workspace(admin, "GitOps workspace B")
    editor = _member(admin, workspace_a)
    foreign = schedule_store.create_schedule(
        {
            "name": "nightly-b",
            "source_connector_id": "source-b",
            "source_table": "B_SCHEDULE_SENTINEL",
            "dest_connector_id": "dest-b",
            "dest_table": "B_SCHEDULE_SENTINEL",
            "interval": "daily",
            "workspace_id": workspace_b,
        }
    )
    contract = DataContract(
        name="B_CONTRACT_SENTINEL",
        columns=[],
        metadata={"workspace_id": workspace_b},
    )
    contract_store.get_contract_store().save_contract(contract)

    base_schedule = {
        "source_connector_id": "source-a",
        "source_table": "orders",
        "dest_connector_id": "dest-a",
        "dest_table": "orders-copy",
        "interval": "daily",
    }

    def plan(label: str, spec: dict):
        response = editor.post(
            "/api/v1/schedules/gitops/plan",
            headers={"X-Workspace-Id": workspace_a},
            json={"kind": "PipelineSchedule", "spec": spec},
        )
        print(
            f"{label}: status={response.status_code} "
            f"body={json.dumps(response.json(), sort_keys=True)}"
        )
        return response

    by_name = plan("foreign name", {**base_schedule, "name": "nightly-b"})
    by_schedule_id = plan(
        "foreign schedule id",
        {**base_schedule, "id": foreign.id, "name": "caller-supplied-name"},
    )
    by_apply = editor.post(
        "/api/v1/schedules/gitops/apply",
        headers={"X-Workspace-Id": workspace_a},
        json={
            "kind": "PipelineSchedule",
            "spec": {**base_schedule, "id": foreign.id, "name": "caller-supplied-name"},
        },
    )
    by_contract_id = editor.post(
        "/api/v1/schedules/gitops/plan",
        headers={"X-Workspace-Id": workspace_a},
        json={"kind": "DataContract", "spec": {"id": contract.id}},
    )
    print(
        "foreign contract id: "
        f"status={by_contract_id.status_code} "
        f"body={json.dumps(by_contract_id.json(), sort_keys=True)}"
    )
    assert by_name.status_code == 200, by_name.text
    assert by_name.json()["actions"][0]["action"] == "create"
    assert by_schedule_id.status_code == 403, by_schedule_id.text
    assert by_apply.status_code == 403, by_apply.text
    assert by_contract_id.status_code == 403, by_contract_id.text

    denials = [
        event
        for event in audit_log.list_audit_events(limit=500)
        if event.get("action") == "authz.denied"
        and event.get("reason_code") == "cross_workspace"
    ]
    assert denials
