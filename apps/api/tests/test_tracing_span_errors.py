from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import tracing  # noqa: E402


@pytest.fixture
def memory_tracing(monkeypatch):
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    monkeypatch.setenv("DATAFLOW_ENABLE_TRACING", "1")
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(tracing, "_PROVIDER_READY", True)
    monkeypatch.setattr(tracing, "_TRACER", provider.get_tracer("dataflow.test"))
    yield exporter
    provider.shutdown()


def test_body_exception_propagates_same_instance(memory_tracing):
    error = ValueError("original")

    with pytest.raises(ValueError) as raised:
        with tracing.start_span("body-error"):
            raise error

    assert raised.value is error


def test_body_exception_is_recorded_on_exported_span(memory_tracing):
    from opentelemetry.trace import StatusCode

    with pytest.raises(ValueError, match="original"):
        with tracing.start_span("body-error"):
            raise ValueError("original")

    spans = memory_tracing.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].status.status_code == StatusCode.ERROR
    assert any(event.name == "exception" for event in spans[0].events)


def test_span_creation_failure_uses_noop_and_runs_body_once(memory_tracing, monkeypatch):
    class FailingTracer:
        def start_as_current_span(self, *_args, **_kwargs):
            raise RuntimeError("span creation failed")

    monkeypatch.setattr(tracing, "_TRACER", FailingTracer())
    seen = []

    def run():
        with tracing.start_span("creation-failure") as span:
            seen.append(span)
            return "body result"

    assert run() == "body result"
    assert len(seen) == 1
    assert isinstance(seen[0], tracing._NoopSpan)


def test_normal_span_exports_once_with_non_error_status(memory_tracing):
    from opentelemetry.trace import StatusCode

    with tracing.start_span("normal"):
        pass

    spans = memory_tracing.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].status.status_code in {StatusCode.OK, StatusCode.UNSET}


def test_submit_in_context_propagates_job_and_correlation_only_for_that_task():
    from services.logging_config import current_job_id, reset_job_id, set_job_id

    previous_correlation_id = tracing.get_correlation_id()
    job_token = set_job_id("job-1")
    tracing.set_correlation_id("corr-1")
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            propagated = tracing.submit_in_context(
                executor,
                lambda: (current_job_id(), tracing.get_correlation_id()),
            ).result()
            plain = executor.submit(
                lambda: (current_job_id(), tracing.get_correlation_id())
            ).result()
    finally:
        reset_job_id(job_token)
        tracing.set_correlation_id(previous_correlation_id)

    assert propagated == ("job-1", "corr-1")
    assert plain == ("", "")
