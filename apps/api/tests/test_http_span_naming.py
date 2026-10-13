from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import tracing  # noqa: E402


@pytest.fixture
def traced_api(monkeypatch):
    pytest.importorskip("opentelemetry.sdk")
    from fastapi.testclient import TestClient
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    monkeypatch.setenv("DATAFLOW_ENABLE_TRACING", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "0")
    monkeypatch.setenv("DATAFLOW_ENV", "development")

    from src.main import app

    class MissingJobMongo:
        fail = False

        def get_job(self, _job_id):
            if self.fail:
                raise RuntimeError("job store unavailable")
            return None

    mongo = MissingJobMongo()
    connectors_module = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(connectors_module, "get_mongodb_service", lambda: mongo)

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(tracing, "_PROVIDER_READY", True)
    monkeypatch.setattr(tracing, "_TRACER", provider.get_tracer("dataflow.http-test"))
    client = TestClient(app, raise_server_exceptions=False)
    yield client, exporter, mongo
    client.close()
    provider.shutdown()


def _http_span(exporter):
    spans = [
        span
        for span in exporter.get_finished_spans()
        if span.attributes.get("http.request.method") == "GET"
    ]
    assert len(spans) == 1
    return spans[0]


@pytest.mark.parametrize(
    ("raw_path", "template", "expected"),
    [
        (
            "/api/v1/connectors/jobs/otel-job-id",
            "/connectors/jobs/{job_id}",
            "/api/v1/connectors/jobs/{job_id}",
        ),
        ("/auth/login", "/auth/login", "/auth/login"),
        (
            "/api/v1/files/a/b",
            "/files/{file_path:path}",
            "/files/{file_path:path}",
        ),
        ("/api/v1", "/", "/api/v1"),
        ("/", "/", "/"),
        ("/api/v1/auth/login", None, None),
        ("/api/v1/auth/login", "", None),
    ],
)
def test_http_route_template(raw_path, template, expected):
    from src.main import _http_route_template

    assert _http_route_template(raw_path, template) == expected


def test_matched_route_uses_template_and_records_status(traced_api):
    client, exporter, _mongo = traced_api
    path = "/api/v1/connectors/jobs/otel-job-id"
    response = client.get(path)

    assert response.status_code == 404
    span = _http_span(exporter)
    assert span.name == "GET /api/v1/connectors/jobs/{job_id}"
    assert "otel-job-id" not in span.name
    assert span.attributes["http.route"] == "/api/v1/connectors/jobs/{job_id}"
    assert span.attributes["http.request.method"] == "GET"
    assert span.attributes["http.method"] == "GET"
    assert span.attributes["url.path"] == path
    assert span.attributes["url.scheme"] == "http"
    assert span.attributes["http.response.status_code"] == 404
    assert span.attributes["http.status_code"] == 404


def test_unmatched_path_keeps_method_only_name(traced_api):
    client, exporter, _mongo = traced_api
    path = "/unmatched/otel-private-path"
    response = client.get(path)

    assert response.status_code == 404
    span = _http_span(exporter)
    assert span.name == "GET"
    assert path not in span.name
    assert "http.route" not in span.attributes
    assert span.attributes["http.response.status_code"] == 404
    assert span.attributes["http.status_code"] == 404


def test_server_span_marks_5xx_response_as_error(traced_api):
    from opentelemetry.trace import StatusCode

    client, exporter, mongo = traced_api
    mongo.fail = True
    response = client.get("/api/v1/connectors/jobs/otel-job-error")

    assert response.status_code == 500
    span = _http_span(exporter)
    assert span.status.status_code == StatusCode.ERROR
    assert span.attributes["http.response.status_code"] == 500
    assert span.attributes["http.status_code"] == 500
