"""Workspace scoping for transfer jobs."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest
from fastapi.testclient import TestClient

from services.team_store import create_workspace
from src.main import app


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    with TestClient(app) as c:
        yield c


def test_transfer_job_created_with_workspace_id(client, tmp_path):
    ws = create_workspace(name="ETL Team", created_by="anonymous")
    files = {
        "file": ("orders.csv", "id,name\n1,Alice\n2,Bob\n", "text/csv"),
    }
    data = {
        "destination_database": str(tmp_path / "orders.db"),
        "destination_collection": "orders",
        "dest_type": "sqlite",
    }
    response = client.post(
        "/api/v1/connectors/transfer",
        files=files,
        data=data,
        headers={"X-Workspace-Id": ws.id},
    )
    assert response.status_code == 200, response.text
    result = response.json()
    job_id = result.get("job_id")
    assert job_id

    # Job list scoped to workspace includes the job.
    response = client.get("/api/v1/connectors/jobs", headers={"X-Workspace-Id": ws.id})
    assert response.status_code == 200
    assert job_id in {j["_id"] for j in response.json()["jobs"]}

    # Job list without workspace does not include the job.
    response = client.get("/api/v1/connectors/jobs")
    assert response.status_code == 200
    assert job_id not in {j["_id"] for j in response.json()["jobs"]}

    # Get job with workspace succeeds.
    response = client.get(f"/api/v1/connectors/jobs/{job_id}", headers={"X-Workspace-Id": ws.id})
    assert response.status_code == 200

    # Members can still get the job without a header because they belong to the workspace.
    response = client.get(f"/api/v1/connectors/jobs/{job_id}")
    assert response.status_code == 200
