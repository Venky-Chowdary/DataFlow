"""Optional OpenTelemetry log export for the centralized logging pipeline."""

from __future__ import annotations

import copy
import logging
import threading
import traceback
from typing import Any

from services.brand_env import getenv_brand
from services.log_redaction import REDACTED, redact_log_text, redact_log_value

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()
_PROVIDER: Any = None
_HANDLER: logging.Handler | None = None
def otel_logs_enabled() -> bool:
    return getenv_brand("OTEL_LOGS", "0").lower() in ("1", "true", "yes")


def _redacted_record(record: logging.LogRecord) -> logging.LogRecord:
    cloned = copy.copy(record)
    cloned.msg = redact_log_text(record.getMessage())
    cloned.args = ()
    try:
        from services.logging_config import _RESERVED_RECORD_ATTRS
    except (ImportError, AttributeError):
        reserved = frozenset(logging.makeLogRecord({}).__dict__)
    else:
        reserved = _RESERVED_RECORD_ATTRS
    for key in tuple(cloned.__dict__):
        if key not in reserved:
            setattr(cloned, key, redact_log_value(key, cloned.__dict__[key]))

    if record.exc_info:
        try:
            exc_type, exc_value, _ = record.exc_info
            exception_type = getattr(exc_type, "__name__", "") or type(
                exc_value
            ).__name__
        except Exception:
            exc_value = None
            exception_type = "Exception"
        try:
            exception_message = redact_log_text(str(exc_value))
        except Exception:
            exception_message = REDACTED
        try:
            exception_stacktrace = redact_log_text(
                "".join(traceback.format_exception(*record.exc_info))
            )
        except Exception:
            exception_stacktrace = REDACTED
        cloned.exc_info = None
        cloned.exc_text = None
        setattr(cloned, "exception.type", exception_type)
        setattr(cloned, "exception.message", exception_message)
        setattr(cloned, "exception.stacktrace", exception_stacktrace)

    if record.stack_info:
        cloned.stack_info = redact_log_text(record.stack_info)
    return cloned


def install_otel_log_handler(root: logging.Logger) -> logging.Handler | None:
    global _PROVIDER, _HANDLER
    if not otel_logs_enabled():
        return None
    if _HANDLER is not None:
        if _HANDLER not in root.handlers:
            root.addHandler(_HANDLER)
        return _HANDLER

    with _LOCK:
        if _HANDLER is not None:
            if _HANDLER not in root.handlers:
                root.addHandler(_HANDLER)
            return _HANDLER
        try:
            from opentelemetry._logs import set_logger_provider
            from opentelemetry.exporter.otlp.proto.http._log_exporter import (
                OTLPLogExporter,
            )
            from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

            from services.tracing import (
                otlp_http_signal_endpoint,
                telemetry_resource,
            )

            endpoint = otlp_http_signal_endpoint("logs")
            if not endpoint:
                raise ValueError("OTLP endpoint is required when OTEL logs are enabled")

            provider = LoggerProvider(resource=telemetry_resource())
            provider.add_log_record_processor(
                BatchLogRecordProcessor(OTLPLogExporter(endpoint=endpoint))
            )
            set_logger_provider(provider)

            class RedactingLoggingHandler(LoggingHandler):
                def emit(self, record: logging.LogRecord) -> None:
                    super().emit(_redacted_record(record))

            handler = RedactingLoggingHandler(
                level=logging.NOTSET,
                logger_provider=provider,
            )
            from services.logging_config import ContextFilter

            handler.addFilter(ContextFilter())
            root.addHandler(handler)
            _PROVIDER = provider
            _HANDLER = handler
            return handler
        except Exception as exc:  # noqa: BLE001 - telemetry must not block the API
            logger.warning("OpenTelemetry log export disabled: %s", exc)
            return None


def shutdown_otel_logging() -> None:
    global _PROVIDER, _HANDLER
    provider = _PROVIDER
    _PROVIDER = None
    _HANDLER = None
    if provider is not None:
        try:
            provider.shutdown()
        except Exception as exc:  # noqa: BLE001 - shutdown is best-effort
            logger.debug("OpenTelemetry log shutdown failed: %s", exc)


def reset_for_tests() -> None:
    global _PROVIDER, _HANDLER
    _PROVIDER = None
    _HANDLER = None
