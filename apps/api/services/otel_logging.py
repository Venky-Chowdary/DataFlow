"""Optional OpenTelemetry log export for the centralized logging pipeline."""

from __future__ import annotations

import copy
import logging
import threading
from typing import Any

from services.brand_env import getenv_brand
from services.log_redaction import SECRET_KEY, redact_log_text

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
    for key in tuple(cloned.__dict__):
        if SECRET_KEY.match(key):
            setattr(cloned, key, "***")
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
