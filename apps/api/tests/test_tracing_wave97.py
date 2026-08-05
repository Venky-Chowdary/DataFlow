"""OpenTelemetry tracing — prove the opt-in path and the fail-closed path.

Tracing is off by default and must never break a transfer when the SDK is
missing or misconfigured. When it is on, the transfer root span must carry
``job_id`` and the inbound correlation id, and credentials must never appear
as span attributes.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import tracing  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate_tracing(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ENABLE_TRACING", raising=False)
    monkeypatch.delenv("DATAFLOW_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("DATAFLOW_TRACE_CONSOLE", raising=False)
    tracing.reset_for_tests()
    yield
    tracing.reset_for_tests()


class TestFailClosed:
    def test_disabled_by_default(self):
        assert tracing.tracing_enabled() is False

    def test_start_span_is_noop_when_disabled(self):
        with tracing.start_span("transfer.execute", attributes={"password": "secret"}) as span:
            assert isinstance(span, tracing._NoopSpan)
            span.set_attribute("x", 1)  # must not raise

    def test_current_trace_id_empty_when_disabled(self):
        assert tracing.current_trace_id() == ""

    def test_redact_strips_secrets(self):
        cleaned = tracing.redact_attributes(
            {
                "dataflow.job_id": "job_1",
                "password": "hunter2",
                "api_key": "sk-xxx",
                "nested_cfg": {"host": "h", "password": "p"},
                "ok_int": 7,
            }
        )
        assert cleaned["dataflow.job_id"] == "job_1"
        assert cleaned["password"] == "***"
        assert cleaned["api_key"] == "***"
        assert cleaned["nested_cfg"] == "<dict>"
        assert cleaned["ok_int"] == 7


class TestEnabledWithSdk:
    @pytest.fixture
    def memory_tracer(self, monkeypatch):
        pytest.importorskip("opentelemetry")
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        monkeypatch.setenv("DATAFLOW_ENABLE_TRACING", "1")
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        # Bypass ensure_provider's own provider install; point the module
        # straight at a tracer from the in-memory provider.
        tracing._PROVIDER_READY = True
        tracing._TRACER = provider.get_tracer("dataflow.test")
        yield exporter
        provider.shutdown()
        tracing.reset_for_tests()

    def test_root_span_carries_job_and_correlation(self, memory_tracer):
        tracing.set_correlation_id("corr-abc")
        with tracing.start_span(
            "transfer.execute",
            attributes={"dataflow.job_id": "job_42", "password": "nope"},
        ) as span:
            tracing.set_span_attribute(span, "dataflow.records_transferred", 100)
            tid = tracing.current_trace_id()
            assert len(tid) == 32

        spans = memory_tracer.get_finished_spans()
        assert len(spans) == 1
        finished = spans[0]
        assert finished.name == "transfer.execute"
        assert finished.attributes["dataflow.job_id"] == "job_42"
        assert finished.attributes["dataflow.correlation_id"] == "corr-abc"
        assert finished.attributes["dataflow.records_transferred"] == 100
        # Secrets must never land on a span.
        assert "password" not in finished.attributes or finished.attributes["password"] == "***"

    def test_child_phase_span_nests_under_root(self, memory_tracer):
        with tracing.start_span("transfer.execute", attributes={"dataflow.job_id": "j"}):
            with tracing.start_span(
                "transfer.phase.read", attributes={"dataflow.phase": "read"}
            ):
                pass
        spans = memory_tracer.get_finished_spans()
        assert len(spans) == 2
        by_name = {s.name: s for s in spans}
        root = by_name["transfer.execute"]
        child = by_name["transfer.phase.read"]
        assert child.parent is not None
        assert child.parent.span_id == root.context.span_id

    def test_lineage_event_becomes_span_event(self, memory_tracer):
        from services import lineage_telemetry as lineage

        lineage.clear_events()
        with tracing.start_span("transfer.execute"):
            lineage.emit_run_started(
                run_id="r1",
                job_id="j1",
                source={"type": "postgres"},
                destination={"type": "mysql"},
                validation_mode="strict",
                write_semantics="append",
            )
        spans = memory_tracer.get_finished_spans()
        assert spans
        events = spans[0].events
        assert any(e.name == "run_started" for e in events)

    def test_phase_profile_measure_opens_span(self, memory_tracer):
        from services.phase_profile import PHASE_READ, PhaseProfile

        profile = PhaseProfile()
        with tracing.start_span("transfer.execute"):
            with profile.measure(PHASE_READ, rows=10):
                pass
        spans = memory_tracer.get_finished_spans()
        names = {s.name for s in spans}
        assert "transfer.phase.read" in names
        snap = profile.snapshot()
        assert snap["phases"][0]["phase"] == PHASE_READ
        assert snap["phases"][0]["rows"] == 10
