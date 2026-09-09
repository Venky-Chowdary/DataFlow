"""CDC freshness must not disclose another workspace's lag.

Same class as D36 / D46: polls can be stamped with workspace_id; Overview
and Pipelines used GET /ops/freshness which ignored X-Workspace-Id, so
Needs attention showed a sibling tenant's CDC SLO.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import ops_metrics
from src.main import app


@pytest.fixture
def isolated_freshness():
    ops_metrics._labeled_gauges["dataflow_pipeline_lag_seconds"] = {}
    ops_metrics._labeled_gauges["dataflow_pipeline_lag_bytes"] = {}
    ops_metrics._labeled_counters["dataflow_pipeline_cdc_polls_total"] = {}
    ops_metrics._pipeline_heartbeat.clear()
    ops_metrics._pipeline_workspace.clear()
    yield
    ops_metrics._labeled_gauges["dataflow_pipeline_lag_seconds"] = {}
    ops_metrics._labeled_gauges["dataflow_pipeline_lag_bytes"] = {}
    ops_metrics._labeled_counters["dataflow_pipeline_cdc_polls_total"] = {}
    ops_metrics._pipeline_heartbeat.clear()
    ops_metrics._pipeline_workspace.clear()


@pytest.fixture
def client(isolated_freshness):
    return TestClient(app)


def test_freshness_summary_is_workspace_scoped(isolated_freshness):
    ops_metrics.record_cdc_poll(
        lag_seconds=400.0,
        job_id="job-a",
        stream="orders",
        schedule_id="sched-a",
        workspace_id="ws-a",
    )
    ops_metrics.record_cdc_poll(
        lag_seconds=12.0,
        job_id="job-b",
        stream="users",
        schedule_id="sched-b",
        workspace_id="ws-b",
    )
    ops_metrics.record_cdc_poll(
        lag_seconds=90.0,
        job_id="job-legacy",
        stream="legacy",
        schedule_id="sched-legacy",
    )

    unscoped = ops_metrics.freshness_summary(max_lag_warn_seconds=60.0)
    assert unscoped["stale_count"] >= 2
    assert {p["job_id"] for p in unscoped["pipelines"]} >= {"job-a", "job-b", "job-legacy"}
    assert unscoped["workspace_id"] is None

    only_a = ops_metrics.freshness_summary(max_lag_warn_seconds=60.0, workspace_id="ws-a")
    assert only_a["workspace_id"] == "ws-a"
    assert [p["job_id"] for p in only_a["pipelines"]] == ["job-a"]
    assert only_a["stale_count"] == 1
    assert only_a["critical_count"] == 1
    assert only_a["slo_status"] == "critical"
    assert only_a["worst_lag_seconds"] == 400.0
    assert all(a.get("schedule_id") == "sched-a" for a in only_a["alerts"])
    assert "sched-b" not in str(only_a["alerts"])
    assert "job-b" not in str(only_a["pipelines"])

    only_b = ops_metrics.freshness_summary(max_lag_warn_seconds=60.0, workspace_id="ws-b")
    assert [p["job_id"] for p in only_b["pipelines"]] == ["job-b"]
    assert only_b["slo_status"] == "ok"
    assert only_b["worst_lag_seconds"] == 12.0
    assert only_b["stale_count"] == 0


def test_scoped_freshness_does_not_inherit_sibling_global_lag(isolated_freshness):
    ops_metrics.record_cdc_poll(
        lag_seconds=400.0,
        job_id="job-foreign",
        stream="orders",
        workspace_id="ws-foreign",
    )
    empty = ops_metrics.freshness_summary(max_lag_warn_seconds=60.0, workspace_id="ws-empty")
    assert empty["pipelines"] == []
    assert empty["alerts"] == []
    assert empty["stale_count"] == 0
    assert empty["worst_lag_seconds"] is None
    assert empty["slo_status"] == "n_a"


def test_http_freshness_does_not_name_sibling_workspace(client):
    ops_metrics.record_cdc_poll(
        lag_seconds=400.0,
        job_id="job-a",
        stream="orders",
        schedule_id="sched-a",
        workspace_id="ws-a",
    )
    ops_metrics.record_cdc_poll(
        lag_seconds=250.0,
        job_id="job-b",
        stream="users",
        schedule_id="sched-b",
        workspace_id="ws-b",
    )

    listed_a = client.get("/api/v1/ops/freshness?warn_seconds=60", headers={"X-Workspace-Id": "ws-a"})
    assert listed_a.status_code == 200, listed_a.text
    body = listed_a.json()
    assert body["workspace_id"] == "ws-a"
    assert body["stale_count"] == 1
    assert [p["job_id"] for p in body["pipelines"]] == ["job-a"]
    assert "job-b" not in listed_a.text
    assert "sched-b" not in listed_a.text

    listed_b = client.get("/api/v1/ops/freshness?warn_seconds=60", headers={"X-Workspace-Id": "ws-b"})
    assert listed_b.json()["workspace_id"] == "ws-b"
    assert [p["job_id"] for p in listed_b.json()["pipelines"]] == ["job-b"]
    assert "job-a" not in listed_b.text


def test_isolation_on_without_header_is_400(client, monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    missing = client.get("/api/v1/ops/freshness?warn_seconds=60")
    assert missing.status_code == 400
    assert "workspace" in missing.json()["detail"].lower()


def test_unscoped_get_stays_platform_wide(client):
    ops_metrics.record_cdc_poll(
        lag_seconds=42.0,
        job_id="job-e2e",
        stream="orders",
        schedule_id="s1",
        workspace_id="ws-a",
    )
    fr = client.get("/api/v1/ops/freshness")
    assert fr.status_code == 200
    body = fr.json()
    assert body["workspace_id"] is None
    assert body["worst_lag_seconds"] is not None
    assert body["worst_lag_seconds"] >= 42.0
    assert any(p["job_id"] == "job-e2e" for p in body["pipelines"])
