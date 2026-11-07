"""Plan Validate must not occupy the HTTP worker for a 1M-row scan.

A sync POST that walks ``iter_stored_upload_rows`` on the FastAPI event loop
freezes ``/health``: nginx 504s, Studio shows "Plan preflight failed" / Validate
Not run, and Execute stays locked with no verdict. ``async_run`` returns 202 and
GET polls the same SSOT ``run_plan_preflight``. Sync callers still get 200.
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.plan_preflight_job import reset_plan_preflight_jobs
from src.routers import transfer_router


@pytest.fixture(autouse=True)
def _isolate_jobs():
    reset_plan_preflight_jobs()
    yield
    reset_plan_preflight_jobs()


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(transfer_router.router, prefix="/api/v1")
    return TestClient(app)


def _wait_job(client: TestClient, plan_id: str, run_id: str, *, timeout: float = 3.0) -> dict:
    deadline = time.time() + timeout
    last: dict = {}
    while time.time() < deadline:
        res = client.get(f"/api/v1/transfer/plans/{plan_id}/preflight", params={"run_id": run_id})
        if res.status_code == 200:
            last = res.json()
            if last.get("status") in {"complete", "failed"}:
                return last
        time.sleep(0.02)
    raise AssertionError(f"Validate job {run_id} did not finish: {last}")


def test_async_run_returns_202_then_poll_completes():
    seen: dict = {}

    def fake_run(plan_id, *, acknowledgments=None, shape_recipe=None):
        seen["plan_id"] = plan_id
        seen["ack"] = acknowledgments
        seen["shape_recipe"] = shape_recipe
        return {
            "passed": True,
            "run_id": "pf_ssot_abc",
            "passed_count": 18,
            "total_gates": 18,
        }

    client = _client()
    with patch(
        "services.transfer_plan_service.run_plan_preflight",
        side_effect=fake_run,
    ):
        res = client.post(
            "/api/v1/transfer/plans/plan-async/preflight",
            json={"async_run": True},
        )
        assert res.status_code == 202, res.text
        started = res.json()
        assert started["status"] == "running"
        assert started["plan_id"] == "plan-async"
        assert started["run_id"]
        assert started.get("passed") is None

        done = _wait_job(client, "plan-async", started["run_id"])

    assert seen["plan_id"] == "plan-async"
    assert done["status"] == "complete"
    assert done["passed"] is True
    # SSOT run_id is the citable Execute handle; job handle stays alongside.
    assert done["run_id"] == "pf_ssot_abc"
    assert done["job_run_id"] == started["run_id"]
    assert done["passed_count"] == 18


def test_get_shows_running_until_the_ssot_finishes():
    """Studio polls GET while the 1M walk is still on the worker."""
    release = threading.Event()

    def slow_run(plan_id, *, acknowledgments=None, shape_recipe=None):
        release.wait(timeout=2)
        return {"passed": True, "run_id": "pf_ssot_slow"}

    client = _client()
    with patch(
        "services.transfer_plan_service.run_plan_preflight",
        side_effect=slow_run,
    ):
        res = client.post(
            "/api/v1/transfer/plans/plan-slow/preflight",
            json={"async_run": True},
        )
        assert res.status_code == 202
        run_id = res.json()["run_id"]
        mid = client.get(
            "/api/v1/transfer/plans/plan-slow/preflight",
            params={"run_id": run_id},
        )
        assert mid.status_code == 200
        assert mid.json()["status"] == "running"
        release.set()
        done = _wait_job(client, "plan-slow", run_id)
    assert done["status"] == "complete"
    assert done["run_id"] == "pf_ssot_slow"


def test_get_publishes_rows_scanned_while_running():
    """The ticker must read a live count, not invent 1/9…9/9."""
    from services.plan_preflight_job import report_plan_preflight_progress

    release = threading.Event()

    def slow_run(plan_id, *, acknowledgments=None, shape_recipe=None):
        report_plan_preflight_progress(
            plan_id,
            rows_scanned=412_000,
            rows_estimate=1_000_000,
            phase="scanning_population_fit",
        )
        release.wait(timeout=2)
        return {"passed": True, "run_id": "pf_ssot_progress"}

    client = _client()
    with patch(
        "services.transfer_plan_service.run_plan_preflight",
        side_effect=slow_run,
    ):
        res = client.post(
            "/api/v1/transfer/plans/plan-progress/preflight",
            json={"async_run": True},
        )
        assert res.status_code == 202
        run_id = res.json()["run_id"]
        # Give the worker one tick to publish.
        time.sleep(0.05)
        mid = client.get(
            "/api/v1/transfer/plans/plan-progress/preflight",
            params={"run_id": run_id},
        )
        assert mid.status_code == 200
        body = mid.json()
        assert body["status"] == "running"
        assert body["rows_scanned"] == 412_000
        assert body["rows_estimate"] == 1_000_000
        assert body["phase"] == "scanning_population_fit"
        release.set()
        done = _wait_job(client, "plan-progress", run_id)
    assert done["status"] == "complete"


def test_failed_job_surfaces_error():
    def boom(plan_id, *, acknowledgments=None, shape_recipe=None):
        raise RuntimeError("warehouse probe hung")

    client = _client()
    with patch(
        "services.transfer_plan_service.run_plan_preflight",
        side_effect=boom,
    ):
        res = client.post(
            "/api/v1/transfer/plans/plan-fail/preflight",
            json={"async_run": True},
        )
        assert res.status_code == 202, res.text
        done = _wait_job(client, "plan-fail", res.json()["run_id"])

    assert done["status"] == "failed"
    assert "warehouse probe hung" in done["error"]
    assert done.get("passed") is None


def test_sync_post_without_async_run_still_200():
    """Schedules and API clients keep the blocking 200 contract."""
    client = _client()
    with patch(
        "services.transfer_plan_service.run_plan_preflight",
        return_value={"passed": True, "run_id": "pf_sync"},
    ):
        res = client.post("/api/v1/transfer/plans/p-sync/preflight")
    assert res.status_code == 200, res.text
    assert res.json()["passed"] is True
    assert res.json()["run_id"] == "pf_sync"


def test_get_preflight_404_when_no_job():
    client = _client()
    res = client.get("/api/v1/transfer/plans/missing/preflight")
    assert res.status_code == 404
