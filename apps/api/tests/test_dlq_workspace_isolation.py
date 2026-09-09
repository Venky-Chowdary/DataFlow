"""DLQ list/count must not disclose another workspace's quarantine events.

Same class as D36: events are stamped with workspace_id; the read path used
to ignore X-Workspace-Id, so Overview "Needs attention" showed a platform
total after a workspace switch.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import quarantine_dlq as dlq
from src.main import app


@pytest.fixture
def isolated_dlq(tmp_path, monkeypatch):
    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "quarantine_dlq.jsonl")
    monkeypatch.setattr(dlq, "_dlq_coll", lambda: None)
    return tmp_path


@pytest.fixture
def client(isolated_dlq):
    return TestClient(app)


def test_count_and_list_are_workspace_scoped(isolated_dlq):
    dlq.append_dlq_event(
        job_id="job-a",
        action="quarantine",
        rows=3,
        workspace_id="ws-a",
        details={"name": "alpha"},
    )
    dlq.append_dlq_event(
        job_id="job-b",
        action="quarantine",
        rows=7,
        workspace_id="ws-b",
        details={"name": "beta"},
    )
    dlq.append_dlq_event(job_id="job-legacy", action="quarantine", rows=1)

    assert dlq.count_dlq_events() == 3
    assert dlq.count_dlq_events(workspace_id="ws-a") == 1
    assert dlq.count_dlq_events(workspace_id="ws-b") == 1
    listed_a = dlq.list_dlq_events(workspace_id="ws-a")
    assert [e["job_id"] for e in listed_a] == ["job-a"]
    assert all(e.get("workspace_id") == "ws-a" for e in listed_a)


def test_http_dlq_does_not_name_sibling_workspace_events(client):
    dlq.append_dlq_event(
        job_id="job-a",
        action="quarantine.held",
        rows=3,
        workspace_id="ws-a",
        details={"name": "alpha"},
    )
    dlq.append_dlq_event(
        job_id="job-b",
        action="quarantine.held",
        rows=7,
        workspace_id="ws-b",
        details={"name": "beta"},
    )

    listed_a = client.get("/api/v1/ops/dlq?limit=50", headers={"X-Workspace-Id": "ws-a"})
    assert listed_a.status_code == 200, listed_a.text
    body = listed_a.json()
    assert body["count"] == 1
    assert body["total"] == 1
    assert body["workspace_id"] == "ws-a"
    assert [e["job_id"] for e in body["events"]] == ["job-a"]
    assert "job-b" not in listed_a.text
    assert "beta" not in listed_a.text

    listed_b = client.get("/api/v1/ops/dlq?limit=50", headers={"X-Workspace-Id": "ws-b"})
    assert listed_b.json()["count"] == 1
    assert [e["job_id"] for e in listed_b.json()["events"]] == ["job-b"]
    assert "job-a" not in listed_b.text


def test_unscoped_dlq_stays_out_of_workspace_count(isolated_dlq):
    dlq.append_dlq_event(job_id="job-scoped", action="quarantine", workspace_id="ws-only")
    dlq.append_dlq_event(job_id="job-global", action="quarantine")
    assert dlq.count_dlq_events(workspace_id="ws-only") == 1
    assert [e["job_id"] for e in dlq.list_dlq_events(workspace_id="ws-only")] == ["job-scoped"]
