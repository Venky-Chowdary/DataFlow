"""The Jobs endpoint must count the whole history, not the page it returns.

The header read "All (50) · Failed (10)" for a 90-job history because the page
counted its own rows, so it disagreed with Pilot (which counts in the store) and
the operator could not tell which number was true.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
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

    # D154: no header is the caller's whole scope — for the single operator of
    # an auth-off deployment that is every job, not "legacy only".
    everything = client.get("/api/v1/connectors/jobs").json()
    assert everything["total"] == 3
    assert everything["status_counts"] == {"failed": 2, "completed": 1}

    other = create_workspace(name="Other", created_by="anonymous")
    narrowed = client.get("/api/v1/connectors/jobs", headers={"X-Workspace-Id": other.id}).json()
    assert narrowed["total"] == 1  # the legacy job only; isolation is off


def test_empty_history_reports_zero(client):
    body = client.get("/api/v1/connectors/jobs").json()
    assert body["total"] == 0
    assert body["status_counts"] == {}
    assert body["jobs"] == []


def test_job_list_pages_searches_filters_and_counts_the_matching_slice(client):
    mongo = get_mongodb_service()
    records = [
        ("needle-orders-0", "completed", 1),
        ("needle-orders-1", "failed", 2),
        ("other-orders-2", "failed", 3),
        ("needle-orders-3", "failed", 4),
        ("needle-orders-4", "running", 5),
        ("needle-orders-5", "completed", 6),
    ]
    for name, status, day in records:
        job_id = mongo.create_transfer_job({
            "name": name,
            "source_name": name,
            "source_type": "postgresql",
            "destination_type": "mysql",
            "destination_database": "warehouse",
            "destination_collection": name,
        })
        mongo._jobs[job_id]["created_at"] = datetime(2026, 11, day, tzinfo=timezone.utc)
        mongo.update_job_status(job_id, status)

    page = client.get("/api/v1/connectors/jobs?limit=2&offset=2").json()
    assert [job["name"] for job in page["jobs"]] == ["needle-orders-3", "other-orders-2"]
    assert page["total"] == 6
    assert page["filtered_total"] == 6
    assert page["limit"] == 2
    assert page["offset"] == 2
    assert page["status_counts"]["failed"] == 3

    filtered = client.get(
        "/api/v1/connectors/jobs?limit=1&offset=1&status=failed&search=needle-orders"
        "&date_from=2026-11-02&date_to=2026-11-04"
    ).json()
    assert filtered["filtered_total"] == 2
    assert [job["name"] for job in filtered["jobs"]] == ["needle-orders-1"]
    assert filtered["total"] == 6


def test_jobs_alias_forwards_paging_and_filter_parameters(client):
    mongo = get_mongodb_service()
    for name, status in (("needle-orders-a", "failed"), ("needle-orders-b", "failed"), ("other-orders", "failed")):
        mongo.create_transfer_job({
            "name": name,
            "source_type": "postgresql",
            "destination_type": "mysql",
            "destination_database": "warehouse",
            "destination_collection": name,
            "status": status,
            "created_at": datetime(2026, 11, 8, tzinfo=timezone.utc),
        })

    query = "?limit=1&offset=1&status=failed&search=needle-orders&date_from=2026-11-08&date_to=2026-11-08"
    connectors = client.get(f"/api/v1/connectors/jobs{query}")
    alias = client.get(f"/api/v1/jobs{query}")

    assert connectors.status_code == alias.status_code == 200
    assert alias.json() == connectors.json()
    assert alias.json()["filtered_total"] == 2
    assert len(alias.json()["jobs"]) == 1
