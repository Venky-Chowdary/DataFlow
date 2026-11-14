"""Audit M-7: HTTP request metrics, store-derived job gauges, scrapeable workers."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from services import ops_metrics

# One exposition sample line: name{label="value",...} number
_SAMPLE = re.compile(
    r'^[a-zA-Z_:][a-zA-Z0-9_:]*'
    r'(\{[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\]|\\.)*"(,[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\]|\\.)*")*\})?'
    r' -?[0-9]+(\.[0-9]+)?(e[+-]?[0-9]+)?$'
)


@pytest.fixture(autouse=True)
def _clean_metrics(monkeypatch):
    monkeypatch.setattr(ops_metrics, "_http", {})
    monkeypatch.setattr(ops_metrics, "_store_cache", {"at": 0.0, "value": None})
    yield


def _samples(text: str, name: str) -> dict[str, float]:
    out = {}
    for line in text.splitlines():
        if line.startswith(name + "{") or line.startswith(name + " "):
            labels, _, value = line[len(name):].rpartition(" ")
            out[labels] = float(value)
    return out


# ---------------------------------------------------------------------------
# HTTP request metrics
# ---------------------------------------------------------------------------


def test_requests_are_counted_by_route_template_and_status_class():
    from fastapi.testclient import TestClient

    from src.main import app

    client = TestClient(app)  # no lifespan: only the request path is under test
    job_ids = [uuid.uuid4().hex for _ in range(5)]
    assert client.get("/health").status_code == 200
    for job_id in job_ids:
        client.get(f"/api/v1/connectors/jobs/{job_id}")
    client.get(f"/no/such/route/{uuid.uuid4().hex}")

    text = ops_metrics.prometheus_text()
    counts = _samples(text, "dataflow_http_requests_total")
    assert counts['{method="GET",route="/health",status_class="2xx"}'] == 1.0
    job_series = [labels for labels in counts if 'route="/api/v1/connectors/jobs/{job_id}"' in labels]
    assert len(job_series) == 1 and counts[job_series[0]] == 5.0, counts
    assert any('route="unmatched"' in labels for labels in counts)
    for job_id in job_ids:
        assert job_id not in text, "a raw path or id reached a metric label"
    assert "/no/such/route" not in text


def test_route_template_never_returns_the_raw_path():
    class _Route:
        path = "/jobs/{job_id}/files/{name:path}"
        path_format = "/jobs/{job_id}/files/{name}"

    scope = {"route": _Route(), "path": "/api/v1/jobs/abc/files/a/b.csv", "path_params": {"job_id": "abc", "name": "a/b.csv"}}
    assert ops_metrics.route_template(scope) == "/api/v1/jobs/{job_id}/files/{name:path}"
    # A route registered with its full path (no include prefix).
    scope = {"route": _Route(), "path": "/jobs/abc/files/x", "path_params": {"job_id": "abc", "name": "x"}}
    assert ops_metrics.route_template(scope) == "/jobs/{job_id}/files/{name:path}"
    # Refused before routing, or no route at all: one fixed label.
    assert ops_metrics.route_template({"path": "/api/v1/jobs/abc"}) == "unmatched"
    assert ops_metrics.route_template({"route": _Route(), "path": "/elsewhere", "path_params": {}}) == _Route.path


def test_latency_histogram_is_cumulative_and_consistent():
    for seconds in (0.003, 0.04, 0.04, 0.7, 45.0):
        ops_metrics.record_http_request(
            method="get", route="/api/v1/jobs", status_code=200, duration_seconds=seconds
        )
    ops_metrics.record_http_request(method="POST", route="/api/v1/jobs", status_code=503, duration_seconds=0.2)
    text = ops_metrics.prometheus_text()
    base = 'method="GET",route="/api/v1/jobs",status_class="2xx"'
    buckets = _samples(text, "dataflow_http_request_duration_seconds_bucket")
    assert buckets[f'{{{base},le="0.005"}}'] == 1
    assert buckets[f'{{{base},le="0.05"}}'] == 3
    assert buckets[f'{{{base},le="1.0"}}'] == 4
    assert buckets[f'{{{base},le="30.0"}}'] == 4
    assert buckets[f'{{{base},le="+Inf"}}'] == 5
    ordered = [buckets[f'{{{base},le="{b}"}}'] for b in ops_metrics.HTTP_BUCKETS]
    assert ordered == sorted(ordered)
    assert _samples(text, "dataflow_http_request_duration_seconds_count")[f"{{{base}}}"] == 5
    assert _samples(text, "dataflow_http_request_duration_seconds_sum")[f"{{{base}}}"] == pytest.approx(45.783)
    assert _samples(text, "dataflow_http_requests_total")['{method="POST",route="/api/v1/jobs",status_class="5xx"}'] == 1


def test_label_cardinality_is_bounded(monkeypatch):
    monkeypatch.setattr(ops_metrics, "MAX_HTTP_SERIES", 50)
    for n in range(400):
        ops_metrics.record_http_request(method="GET", route=f"/r/{n}", status_code=200, duration_seconds=0.01)
    ops_metrics.record_http_request(method="BREW", route="/r/0", status_code=999, duration_seconds=0.01)
    counts = _samples(ops_metrics.prometheus_text(), "dataflow_http_requests_total")
    assert len(counts) <= 50 + 2
    assert counts['{method="GET",route="other",status_class="2xx"}'] == 350
    assert '{method="OTHER",route="other",status_class="unknown"}' in counts


def test_every_exposition_line_is_valid_including_pipeline_series():
    ops_metrics.record_cdc_poll(lag_seconds=4.5, schedule_id='sch"1', stream="public.orders", job_id="j1")
    ops_metrics.record_http_request(method="GET", route="/health", status_code=200, duration_seconds=0.001)
    try:
        for line in ops_metrics.prometheus_text().splitlines():
            if line and not line.startswith("#"):
                assert _SAMPLE.match(line.replace('le="+Inf"', 'le="Inf"')), f"not valid exposition: {line!r}"
    finally:
        for series in (*ops_metrics._labeled_gauges.values(), *ops_metrics._labeled_counters.values()):
            for key in [k for k in series if "sch" in k]:
                series.pop(key, None)


# ---------------------------------------------------------------------------
# Gauges read from the job store
# ---------------------------------------------------------------------------


@pytest.fixture
def store_gauge_owner(monkeypatch):
    """This process emits the store-derived series (ops_metrics.store_gauges_owner).

    Ownership follows the scheduler role (schedule_loop_enabled), which the
    test session turns off by default (tests/conftest.py); the owner path is
    made explicit here instead of read from the environment.
    """
    monkeypatch.setattr(ops_metrics, "store_gauges_owner", lambda: True)


@pytest.fixture
def job_store(monkeypatch):
    from services import mongodb_service

    store = mongodb_service.MemoryMongoDBService()
    store.connect()
    monkeypatch.setattr(mongodb_service, "_mongodb_service", store)
    return store


def test_jobs_by_status_queue_depth_and_oldest_running_age_come_from_the_store(
        job_store, store_gauge_owner):
    now = datetime.now(timezone.utc)
    job_store.create_transfer_job({"job_id": "r1", "status": "running", "started_at": now - timedelta(seconds=900)})
    job_store.create_transfer_job({"job_id": "r2", "status": "running", "started_at": now - timedelta(seconds=30)})
    job_store.create_transfer_job({"job_id": "q1", "status": "queued"})
    job_store.create_transfer_job({"job_id": "p1", "status": "pending"})
    job_store.create_transfer_job({"job_id": "f1", "status": "failed"})
    job_store.create_transfer_job({"job_id": "x1", "status": "some_new_state"})

    text = ops_metrics.prometheus_text()
    jobs = _samples(text, "dataflow_jobs")
    assert jobs['{status="running"}'] == 2
    assert jobs['{status="failed"}'] == 1
    assert jobs['{status="completed"}'] == 0
    assert jobs['{status="other"}'] == 1, "an unknown status must not become its own label"
    assert _samples(text, "dataflow_job_queue_depth")[""] == 2
    assert 895 <= _samples(text, "dataflow_oldest_running_job_age_seconds")[""] <= 960
    assert _samples(text, "dataflow_metrics_store_up")[""] == 1


def test_store_read_is_cached_between_scrapes(job_store, monkeypatch, store_gauge_owner):
    calls = []
    real = ops_metrics._read_store_gauges
    monkeypatch.setattr(ops_metrics, "_read_store_gauges", lambda: calls.append(1) or real())
    for _ in range(5):
        ops_metrics.prometheus_text()
    assert len(calls) == 1


def test_scrape_answers_and_says_so_when_the_store_is_down(monkeypatch, store_gauge_owner):
    def down():
        raise RuntimeError("store unreachable")

    monkeypatch.setattr(ops_metrics, "_read_store_gauges", down)
    text = ops_metrics.prometheus_text()
    assert _samples(text, "dataflow_metrics_store_up")[""] == 0
    assert not _samples(text, "dataflow_jobs")
    assert "dataflow_http_requests_total" in text


def test_a_process_that_does_not_own_the_store_gauges_emits_none(job_store, monkeypatch):
    calls = []
    monkeypatch.setattr(ops_metrics, "store_gauges_owner", lambda: False)
    monkeypatch.setattr(ops_metrics, "_read_store_gauges", lambda: calls.append(1) or {})
    job_store.create_transfer_job({"job_id": "n1", "status": "running"})
    text = ops_metrics.prometheus_text()
    assert calls == []
    for name in ("dataflow_jobs", "dataflow_job_queue_depth", "dataflow_metrics_store_up"):
        assert not _samples(text, name), name
    assert "dataflow_http_requests_total" in text


# ---------------------------------------------------------------------------
# Workers: the same /metrics, behind the same authentication
# ---------------------------------------------------------------------------


@pytest.fixture
def worker_metrics_client(monkeypatch, job_store):
    from fastapi.testclient import TestClient

    from services import rbac
    from src.metrics_app import build_metrics_app

    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "password123")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-for-worker-metrics")
    monkeypatch.setattr(rbac._auth_service, "_REQUIRE_AUTH", True)
    return TestClient(build_metrics_app(role="worker", health=lambda: {"draining": False, "inflight": 2}))


def test_worker_health_is_public_and_carries_no_data(worker_metrics_client):
    response = worker_metrics_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "role": "worker", "draining": False, "inflight": 2}


def test_worker_metrics_refuse_an_unauthenticated_scrape(worker_metrics_client):
    assert worker_metrics_client.get("/metrics").status_code == 401
    assert worker_metrics_client.get("/metrics", headers={"Authorization": "Bearer not-a-token"}).status_code == 401


def test_worker_metrics_answer_an_authorised_scrape(worker_metrics_client, job_store,
                                                   store_gauge_owner):
    from src.services.auth_service import create_token

    job_store.create_transfer_job({"job_id": "w1", "status": "running", "started_at": datetime.now(timezone.utc)})
    ops_metrics.record_job_outcome(status="completed", records=10)
    token = create_token("admin@example.com")[0]
    response = worker_metrics_client.get("/metrics", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain; version=0.0.4")
    assert 'dataflow_jobs{status="running"} 1.0' in response.text
    assert "dataflow_jobs_completed_total" in response.text


def test_worker_metrics_app_serves_nothing_else(worker_metrics_client):
    from src.services.auth_service import create_token

    headers = {"Authorization": f"Bearer {create_token('admin@example.com')[0]}"}
    assert worker_metrics_client.get("/api/v1/connectors/jobs", headers=headers).status_code == 404
    assert worker_metrics_client.get("/docs", headers=headers).status_code == 404


def test_metrics_port_setting(monkeypatch):
    from src import metrics_app

    for key in ("DATAFLOW_METRICS_PORT", "DATAWRAP_METRICS_PORT"):
        monkeypatch.delenv(key, raising=False)
    assert metrics_app.metrics_port() is None
    assert metrics_app.start_metrics_server(role="worker") is False, "no port configured: nothing listens"
    monkeypatch.setenv("DATAFLOW_METRICS_PORT", "9464")
    assert metrics_app.metrics_port() == 9464
    monkeypatch.setenv("DATAFLOW_METRICS_PORT", "not-a-port")
    assert metrics_app.metrics_port() is None
