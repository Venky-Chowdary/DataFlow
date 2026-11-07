from __future__ import annotations

import logging
import json

from services import otel_logging, tracing
from services.log_redaction import redact_log_text, redact_log_value
from services.logging_config import JsonFormatter


def test_signal_endpoint_appends_and_replaces_paths(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://alloy:4318")
    assert tracing.otlp_http_signal_endpoint("traces") == "http://alloy:4318/v1/traces"
    assert tracing.otlp_http_signal_endpoint("logs") == "http://alloy:4318/v1/logs"

    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "https://example.invalid/otlp/v1/traces",
    )
    assert (
        tracing.otlp_http_signal_endpoint("logs")
        == "https://example.invalid/otlp/v1/logs"
    )


def test_redact_log_text_masks_headers_assignments_and_uri_passwords():
    line = (
        "Authorization=Bearer abc.def token=glc_secret "
        "url=https://alice:hunter2@example.invalid/db"
    )
    redacted = redact_log_text(line)
    assert "abc.def" not in redacted
    assert "glc_secret" not in redacted
    assert "hunter2" not in redacted
    assert redacted.count("***") >= 3


def test_json_formatter_redacts_message_extras_and_raw_payloads():
    record = logging.LogRecord(
        name="dataflow.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Authorization=Bearer abc.def",
        args=(),
        exc_info=None,
    )
    record.api_key = "secret-key"
    record.payload = {"email": "person@example.invalid"}
    payload = json.loads(JsonFormatter().format(record))
    assert "abc.def" not in payload["msg"]
    assert "***" in payload["msg"]
    assert payload["api_key"] == "***"
    assert payload["payload"] == "***"


def test_nested_log_value_redaction():
    value = redact_log_value(
        "metadata",
        {"token": "secret", "url": "https://alice:hunter2@example.invalid/db"},
    )
    assert value["token"] == "***"
    assert "hunter2" not in value["url"]


def test_redacted_record_masks_secret_extras_without_mutating_original():
    record = logging.LogRecord(
        name="dataflow.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="token=%s",
        args=("sensitive",),
        exc_info=None,
    )
    record.api_key = "also-sensitive"
    cloned = otel_logging._redacted_record(record)
    assert "sensitive" not in cloned.getMessage()
    assert cloned.api_key == "***"
    assert record.api_key == "also-sensitive"
    assert record.getMessage() == "token=sensitive"


def test_otel_logs_are_opt_in(monkeypatch):
    monkeypatch.delenv("DATAFLOW_OTEL_LOGS", raising=False)
    monkeypatch.delenv("DATAWRAP_OTEL_LOGS", raising=False)
    assert otel_logging.otel_logs_enabled() is False


def test_telemetry_resource_emits_current_and_legacy_environment_keys(monkeypatch):
    from services import tracing

    monkeypatch.setattr(tracing.socket, "gethostname", lambda: "api-host-123")

    monkeypatch.setenv("DATAFLOW_SERVICE_NAME", "dataflow-api-production")
    monkeypatch.setenv("DATAFLOW_SERVICE_NAMESPACE", "dataflow")
    monkeypatch.setenv("DATAFLOW_ENVIRONMENT", "production")
    monkeypatch.setenv("DATAFLOW_SERVICE_INSTANCE_ID", "replica-123")
    monkeypatch.setenv("DATAFLOW_SERVICE_VERSION", "commit-abc")

    attrs = tracing.telemetry_resource().attributes

    assert attrs["service.name"] == "dataflow-api-production"
    assert attrs["service.namespace"] == "dataflow"
    assert attrs["service.instance.id"] == "replica-123"
    assert attrs["service.version"] == "commit-abc"
    assert attrs["grafana.host.id"] == "replica-123"
    assert attrs["host.name"] == "api-host-123"
    assert attrs["deployment.environment.name"] == "production"
    assert attrs["deployment.environment"] == "production"
