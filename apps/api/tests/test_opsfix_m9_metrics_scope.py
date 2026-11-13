"""Ops review Medium 9: deployment-wide metrics are cheap and for platform admins only.

Every scraped process (each API pod, every worker, the scheduler) counted the
whole ``transfer_jobs`` collection with a ``$group`` plus an attention
``$cond``, and ``/metrics`` needs only ``workspace.manage`` in any workspace,
so any tenant's workspace admin could read deployment-wide job counts, queue
depth and per-route traffic.

- The job gauges are computed by one process: the one that owns the
  pipeline cadence (the scheduler, or the API where no scheduler runs). The
  read is one status-only ``$group`` with a time limit.
- The job gauges and the per-route HTTP series are shown to platform admins
  only; a workspace admin's scrape keeps the process counters it had before.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from services import ops_metrics


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(ops_metrics, "_http", {})
    monkeypatch.setattr(ops_metrics, "_store_cache", {"at": 0.0, "value": None})
    for key in ("PROCESS_ROLE", "SCHEDULE_LOOP"):
        monkeypatch.delenv(f"DATAFLOW_{key}", raising=False)
        monkeypatch.delenv(f"DATAWRAP_{key}", raising=False)


@pytest.fixture
def job_store(monkeypatch):
    from services import mongodb_service

    store = mongodb_service.MemoryMongoDBService()
    store.connect()
    monkeypatch.setattr(mongodb_service, "_mongodb_service", store)
    store.create_transfer_job({"job_id": "r1", "status": "running"})
    return store


def _scrape(monkeypatch, *, platform_admin: bool) -> str:
    import asyncio

    from services import workspace_access
    from src import metrics_app

    monkeypatch.setattr(workspace_access, "tenant_scope", lambda _request, *_a: SimpleNamespace(unrestricted=platform_admin))
    response = asyncio.run(metrics_app.metrics_response(SimpleNamespace(state=SimpleNamespace())))
    return response.body.decode()


def test_a_workspace_admin_does_not_see_deployment_wide_series(monkeypatch, job_store):
    ops_metrics.record_http_request(method="GET", route="/api/v1/tenants/{tenant_id}", status_code=200, duration_seconds=0.01)
    ops_metrics.record_job_outcome(status="completed", records=10)
    text = _scrape(monkeypatch, platform_admin=False)
    assert "dataflow_http_requests_total" not in text
    assert "dataflow_http_request_duration_seconds" not in text
    assert 'dataflow_jobs{status="running"}' not in text
    assert "dataflow_job_queue_depth" not in text
    assert "dataflow_jobs_completed_total" in text, "the process counters it could read before stay"


def test_a_platform_admin_sees_everything(monkeypatch, job_store):
    ops_metrics.record_http_request(method="GET", route="/health", status_code=200, duration_seconds=0.01)
    text = _scrape(monkeypatch, platform_admin=True)
    assert "dataflow_http_requests_total" in text
    assert 'dataflow_jobs{status="running"} 1.0' in text


@pytest.mark.parametrize(
    ("env", "owner"),
    [
        ({}, True),  # one process (API, cadence on)
        ({"DATAFLOW_PROCESS_ROLE": "scheduler"}, True),
        ({"DATAFLOW_PROCESS_ROLE": "worker"}, False),
        ({"DATAFLOW_PROCESS_ROLE": "api", "DATAFLOW_SCHEDULE_LOOP": "0"}, False),
    ],
)
def test_only_the_cadence_owner_reads_the_job_store(monkeypatch, job_store, env, owner):
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    reads: list[int] = []
    real = ops_metrics._read_store_gauges
    monkeypatch.setattr(ops_metrics, "_read_store_gauges", lambda: reads.append(1) or real())
    text = ops_metrics.prometheus_text()
    assert bool(reads) is owner
    assert ('dataflow_jobs{status="running"}' in text) is owner


class _Jobs:
    def __init__(self) -> None:
        self.pipelines: list[tuple[list, dict]] = []

    def aggregate(self, pipeline, **kwargs):
        self.pipelines.append((pipeline, kwargs))
        return iter([{"_id": "running", "n": 2}, {"_id": "failed", "n": 1}, {"_id": "weird", "n": 4}])


def test_the_store_read_is_one_status_only_group_with_a_time_limit(monkeypatch):
    from services import control_plane_store, mongodb_service

    jobs = _Jobs()

    class _Mongo:
        def count_jobs(self, **_kwargs):
            raise AssertionError("count_jobs scans with an attention $cond on every scrape")

        def list_jobs(self, **_kwargs):
            return []

    monkeypatch.setattr(mongodb_service, "_mongodb_service", _Mongo())
    monkeypatch.setattr(control_plane_store, "mongo_collection", lambda name: jobs if name == "transfer_jobs" else None)
    gauges = ops_metrics._read_store_gauges()
    assert gauges["by_status"]["running"] == 2 and gauges["by_status"]["other"] == 4
    assert jobs.pipelines == [([{"$group": {"_id": "$status", "n": {"$sum": 1}}}], {"maxTimeMS": 5000})]
