"""Overview first-load undercount — the API half of the race.

The browser used to fetch jobs before Permissions named ``X-Workspace-Id``.
That unscoped read counts only legacy (no workspace) jobs. A hard refresh
already has the id in localStorage, so the same endpoint returns the real
scoped total. This fixture is that pair of requests.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest
from fastapi.testclient import TestClient

from services import mongodb_service as mongodb_service_mod
from services.mongodb_service import get_mongodb_service
from services.team_store import create_workspace
from src.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setattr(mongodb_service_mod, "_mongodb_service", None)
    with TestClient(app) as c:
        yield c


def _seed(n: int, status: str, workspace_id: str = "") -> None:
    mongo = get_mongodb_service()
    for i in range(n):
        job_id = mongo.create_transfer_job({
            "source_type": "postgresql",
            "destination_type": "mysql",
            "destination_database": "warehouse",
            "destination_collection": f"{status}_{i}",
            "workspace_id": workspace_id,
        })
        mongo.update_job_status(job_id, status)


def test_first_load_without_workspace_header_undercounts_scoped_history(client):
    ws = create_workspace(name="Ops", created_by="anonymous")
    _seed(5, "completed")
    _seed(80, "completed", workspace_id=ws.id)
    _seed(10, "failed", workspace_id=ws.id)

    first_paint = client.get("/api/v1/connectors/jobs").json()
    after_refresh = client.get(
        "/api/v1/connectors/jobs",
        headers={"X-Workspace-Id": ws.id},
    ).json()

    assert first_paint["total"] == 5
    assert first_paint["count"] <= 50
    assert after_refresh["total"] == 95
    assert after_refresh["status_counts"]["completed"] == 85
    assert after_refresh["status_counts"]["failed"] == 10
    assert after_refresh["count"] == 50
    assert after_refresh["count"] < after_refresh["total"]
    assert first_paint["total"] < after_refresh["total"]
