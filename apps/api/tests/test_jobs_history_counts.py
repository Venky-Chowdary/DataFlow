"""The Jobs endpoint must count the whole history, not the page it returns.

The header read "All (50) · Failed (10)" for a 90-job history because the page
counted its own rows, so it disagreed with Pilot (which counts in the store) and
the operator could not tell which number was true.
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
    # The job store is a process singleton; each test counts its own history.
    monkeypatch.setattr(mongodb_service_mod, "_mongodb_service", None)
    with TestClient(app) as c:
        yield c


def _seed(statuses: list[str], workspace_id: str = "") -> None:
    mongo = get_mongodb_service()
    for i, status in enumerate(statuses):
        job_id = mongo.create_transfer_job({
            "source_type": "postgresql",
            "destination_type": "mysql",
            "destination_database": "warehouse",
            "destination_collection": f"orders_{i}",
            "workspace_id": workspace_id,
        })
        mongo.update_job_status(job_id, status)


def test_totals_count_the_history_not_the_returned_page(client):
    statuses = ["completed"] * 42 + ["running"] * 17 + ["pending"] * 2 + ["failed"] * 29
    _seed(statuses)

    body = client.get("/api/v1/connectors/jobs").json()

    assert body["total"] == 90
    assert body["count"] == len(body["jobs"]) <= 50
    assert body["count"] < body["total"], "the page must be smaller than the history here"
    assert body["status_counts"] == {
        "completed": 42,
        "running": 17,
        "pending": 2,
        "failed": 29,
    }


def test_counts_are_scoped_to_the_workspace_that_asked(client):
    ws = create_workspace(name="ETL Team", created_by="anonymous")
    _seed(["failed", "failed"], workspace_id=ws.id)
    _seed(["completed"])

    scoped = client.get("/api/v1/connectors/jobs", headers={"X-Workspace-Id": ws.id}).json()
    # Workspace scope also sees unscoped legacy jobs, as the list does.
    assert scoped["total"] == 3
    assert scoped["status_counts"]["failed"] == 2

    global_only = client.get("/api/v1/connectors/jobs").json()
    assert global_only["total"] == 1
    assert global_only["status_counts"] == {"completed": 1}


def test_empty_history_reports_zero(client):
    body = client.get("/api/v1/connectors/jobs").json()
    assert body["total"] == 0
    assert body["status_counts"] == {}
    assert body["jobs"] == []
