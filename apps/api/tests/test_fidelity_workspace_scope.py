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
from services import audit_log, connector_store, schedule_store, team_store, user_store
from src.main import app
from src.routers import fidelity_router


@pytest.fixture
def fidelity_isolation(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root-fidelity@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "fidelity-test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    reset_auth_rate_limits()
    return tmp_path


def _admin() -> TestClient:
    client = TestClient(app)
    response = client.post(
        "/api/v1/auth/login",
        json={
            "email": "root-fidelity@example.com",
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
            "email": "fidelity-editor@example.com",
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
            "email": "fidelity-editor@example.com",
            "password": created.json()["temporary_password"],
        },
    )
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    client.headers["X-Workspace-Id"] = workspace_id
    return client


class _Report:
    def to_dict(self):
        return {
            "passed": True,
            "source": {"table": "B_CONNECTOR_SENTINEL"},
        }


def test_fidelity_cannot_resolve_foreign_schedules_or_connectors(
    fidelity_isolation, monkeypatch
):
    admin = _admin()
    workspace_a = _workspace(admin, "Fidelity workspace A")
    workspace_b = _workspace(admin, "Fidelity workspace B")
    editor = _member(admin, workspace_a)
    source = connector_store.create_connector(
        {
            "name": "B_SOURCE_SENTINEL",
            "type": "postgresql",
            "host": "B_CONNECTOR_HOST_SENTINEL",
            "port": 5432,
            "database": "B_CONNECTOR_DB_SENTINEL",
            "workspace_id": workspace_b,
        }
    )
    destination = connector_store.create_connector(
        {
            "name": "B_DEST_SENTINEL",
            "type": "postgresql",
            "host": "B_CONNECTOR_HOST_SENTINEL",
            "port": 5432,
            "database": "B_CONNECTOR_DB_SENTINEL",
            "workspace_id": workspace_b,
        }
    )
    legacy_schedule = schedule_store.create_schedule(
        {
            "name": "blank-legacy-schedule",
            "source_connector_id": source.id,
            "source_table": "orders",
            "dest_connector_id": destination.id,
            "dest_table": "orders_copy",
            "interval": "daily",
            "workspace_id": "",
            "mappings": [{"source": "id", "target": "id"}],
        }
    )
    monkeypatch.setattr(
        fidelity_router,
        "run_and_record_campaign",
        lambda *args, **kwargs: (_Report(), {"run_count": 1}),
    )
    monkeypatch.setattr(fidelity_router, "run_fidelity_check", lambda **kwargs: _Report())

    scheduled = editor.post(
        "/api/v1/fidelity/check",
        headers={"X-Workspace-Id": workspace_a, "X-Correlation-ID": "fid-schedule-test"},
        json={"schedule_id": legacy_schedule.id},
    )
    direct = editor.post(
        "/api/v1/fidelity/check",
        headers={"X-Workspace-Id": workspace_a, "X-Correlation-ID": "fid-connectors-test"},
        json={
            "source": {"kind": "database", "format": "postgresql", "connector_id": source.id},
            "destination": {
                "kind": "database",
                "format": "postgresql",
                "connector_id": destination.id,
            },
        },
    )
    print(
        "legacy schedule: "
        f"status={scheduled.status_code} body={json.dumps(scheduled.json(), sort_keys=True)}"
    )
    print(
        "direct connector ids: "
        f"status={direct.status_code} body={json.dumps(direct.json(), sort_keys=True)}"
    )
    assert scheduled.status_code == 403, scheduled.text
    assert direct.status_code == 403, direct.text
    assert "B_CONNECTOR_SENTINEL" not in scheduled.text
    assert "B_CONNECTOR_SENTINEL" not in direct.text
    denials = [
        event
        for event in audit_log.list_audit_events(limit=500)
        if event.get("action") == "authz.denied"
        and event.get("reason_code") == "cross_workspace"
    ]
    assert denials


def test_fidelity_without_workspace_header_cannot_reach_foreign_schedule(
    fidelity_isolation, monkeypatch
):
    admin = _admin()
    workspace_a = _workspace(admin, "Fidelity outsider A")
    workspace_b = _workspace(admin, "Fidelity victim B")
    outsider = _member(admin, workspace_a)
    outsider.headers.pop("X-Workspace-Id", None)

    def _connector(name: str, workspace_id: str):
        return connector_store.create_connector(
            {
                "name": name,
                "type": "postgresql",
                "host": "B_CONNECTOR_HOST_SENTINEL",
                "port": 5432,
                "database": "B_CONNECTOR_DB_SENTINEL",
                "workspace_id": workspace_id,
            }
        )

    def _schedule(name: str, workspace_id: str, src, dst):
        return schedule_store.create_schedule(
            {
                "name": name,
                "source_connector_id": src.id,
                "source_table": "orders",
                "dest_connector_id": dst.id,
                "dest_table": "orders_copy",
                "interval": "daily",
                "workspace_id": workspace_id,
                "mappings": [{"source": "id", "target": "id"}],
            }
        )

    b_src, b_dst = _connector("B_SRC", workspace_b), _connector("B_DST", workspace_b)
    a_src, a_dst = _connector("A_SRC", workspace_a), _connector("A_DST", workspace_a)
    victim = _schedule("victim-schedule", workspace_b, b_src, b_dst)
    own = _schedule("own-schedule", workspace_a, a_src, a_dst)

    monkeypatch.setattr(
        fidelity_router,
        "run_and_record_campaign",
        lambda *args, **kwargs: (_Report(), {"run_count": 1}),
    )
    monkeypatch.setattr(fidelity_router, "run_fidelity_check", lambda **kwargs: _Report())
    # The documented production default: isolation off, header optional.
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "0")

    scheduled = outsider.post("/api/v1/fidelity/check", json={"schedule_id": victim.id})
    direct = outsider.post(
        "/api/v1/fidelity/check",
        json={
            "source": {"kind": "database", "format": "postgresql", "connector_id": b_src.id},
            "destination": {"kind": "database", "format": "postgresql", "connector_id": a_dst.id},
        },
    )
    allowed = outsider.post("/api/v1/fidelity/check", json={"schedule_id": own.id})

    assert scheduled.status_code == 403, scheduled.text
    assert direct.status_code == 403, direct.text
    assert "B_CONNECTOR_SENTINEL" not in scheduled.text + direct.text
    assert not (schedule_store.get_schedule(victim.id).fidelity_campaign or {})
    assert allowed.status_code == 200, allowed.text
