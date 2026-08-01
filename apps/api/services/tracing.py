"""OpenTelemetry tracing for DataFlow transfers — optional, fail-closed.

Tracing is off by default. When ``DATAFLOW_ENABLE_TRACING=1`` and the
OpenTelemetry SDK is installed, every transfer gets a root span keyed by
``job_id`` and every phase (read / transform_write / checksum) becomes a
child. When either condition is false the public helpers return no-op
context managers and the product behaves exactly as before.

Why this is a separate module rather than sprinkled ``start_as_current_span``
calls:

* The SDK is not a hard dependency (it ships with the optional RAG stack via
  chromadb). Importing it at module top would break a lean install.
* Transfer work crosses a ``ThreadPoolExecutor`` in the scheduler and again
  inside the chunk dispatcher. OTel context does not follow ``submit()``
  automatically — :func:`attach_context` / :func:`detach_context` exist so
  the caller can carry a token across the pool boundary.
* Span attributes must never contain credentials. Anything that looks like a
  password, token, or private key is redacted before it touches a span.

The module is also the bridge for the existing ``X-Correlation-ID`` header:
HTTP middleware records the correlation id into the current context, and the
transfer root span joins it with ``job_id`` so an operator can stitch an API
request to the job it started.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

logger = logging.getLogger(__name__)

_TRACER_NAME = "dataflow.transfer"
_PROVIDER_LOCK = threading.Lock()
_PROVIDER_READY = False
_TRACER: Any = None

#: Correlation id from the inbound HTTP request. Bridged into transfer spans
#: so an API call and the job it started share one operator-visible identity.
_CORRELATION_ID: ContextVar[str] = ContextVar("df_correlation_id", default="")
_TRACEPARENT: ContextVar[str] = ContextVar("df_traceparent", default="")

_SECRET_KEYS = re.compile(
    r"(password|passwd|secret|token|api[_-]?key|private[_-]?key|authorization|credential)",
    re.IGNORECASE,
)


def tracing_enabled() -> bool:
    return os.getenv("DATAFLOW_ENABLE_TRACING", "0").lower() in ("1", "true", "yes")


def otlp_endpoint() -> str:
    return (
        os.getenv("DATAFLOW_OTLP_ENDPOINT")
        or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        or ""
    ).strip()


def set_correlation_id(value: str | None) -> None:
    _CORRELATION_ID.set((value or "").strip())


def get_correlation_id() -> str:
    return _CORRELATION_ID.get()


def set_traceparent(value: str | None) -> None:
    _TRACEPARENT.set((value or "").strip())


def get_traceparent() -> str:
    return _TRACEPARENT.get()


def current_trace_id() -> str:
    """Hex trace id of the active span, or empty when tracing is off/unavailable."""
    tracer_api = _otel_trace()
    if tracer_api is None:
        return ""
    try:
        span = tracer_api.get_current_span()
        ctx = span.get_span_context() if span is not None else None
        if ctx is None or not getattr(ctx, "is_valid", False):
            return ""
        return format(int(ctx.trace_id), "032x")
    except Exception:
        return ""


def redact_attributes(attrs: dict[str, Any] | None) -> dict[str, Any]:
    """Drop or mask anything that looks like a secret before it hits a span."""
    if not attrs:
        return {}
    out: dict[str, Any] = {}
    for key, value in attrs.items():
        if _SECRET_KEYS.search(str(key)):
            out[str(key)] = "***"
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            out[str(key)] = value
        else:
            # Nested structures are almost always endpoint configs; never
            # serialize them raw. A short type tag is enough for debugging.
            out[str(key)] = f"<{type(value).__name__}>"
    return out


@contextmanager
def start_span(
    name: str,
    *,
    attributes: dict[str, Any] | None = None,
    kind: str = "internal",
) -> Iterator[Any]:
    """Open a span. Yields a no-op object when tracing is off or unavailable."""
    tracer = _get_tracer()
    if tracer is None:
        yield _NoopSpan()
        return

    attrs = redact_attributes(attributes)
    correlation = get_correlation_id()
    if correlation and "dataflow.correlation_id" not in attrs:
        attrs["dataflow.correlation_id"] = correlation

    span_kind = _span_kind(kind)
    try:
        with tracer.start_as_current_span(name, kind=span_kind, attributes=attrs) as span:
            yield span
    except Exception as exc:
        # Tracing must never break a transfer. Log once and yield a no-op so
        # the caller's ``with`` block still runs.
        logger.debug("tracing span %s failed open: %s", name, exc, exc_info=exc)
        yield _NoopSpan()


def set_span_error(span: Any, exc: BaseException) -> None:
    if span is None or isinstance(span, _NoopSpan):
        return
    try:
        span.record_exception(exc)
        status = _otel_status()
        if status is not None:
            span.set_status(status.Status(status.StatusCode.ERROR, str(exc)[:200]))
    except Exception as inner:
        logger.debug("set_span_error failed: %s", inner)


def set_span_attribute(span: Any, key: str, value: Any) -> None:
    if span is None or isinstance(span, _NoopSpan):
        return
    try:
        cleaned = redact_attributes({key: value}).get(key)
        if cleaned is not None:
            span.set_attribute(key, cleaned)
    except Exception as exc:
        logger.debug("set_span_attribute failed: %s", exc)


def add_span_event(name: str, attributes: dict[str, Any] | None = None) -> None:
    """Attach a named event to the current span. No-op when tracing is off."""
    if not tracing_enabled():
        return
    tracer_api = _otel_trace()
    if tracer_api is None:
        return
    try:
        span = tracer_api.get_current_span()
        if span is None or not getattr(span, "is_recording", lambda: False)():
            return
        span.add_event(name, attributes=redact_attributes(attributes))
    except Exception as exc:
        logger.debug("add_span_event failed: %s", exc)


def capture_context() -> Any:
    """Snapshot the current OTel context for hand-off to a worker thread."""
    ctx_api = _otel_context()
    if ctx_api is None:
        return None
    try:
        return ctx_api.get_current()
    except Exception:
        return None


def attach_context(token_or_ctx: Any) -> Any:
    """Attach a previously captured context. Returns a detach token."""
    ctx_api = _otel_context()
    if ctx_api is None or token_or_ctx is None:
        return None
    try:
        return ctx_api.attach(token_or_ctx)
    except Exception:
        return None


def detach_context(token: Any) -> None:
    ctx_api = _otel_context()
    if ctx_api is None or token is None:
        return
    try:
        ctx_api.detach(token)
    except Exception as exc:
        logger.debug("detach_context failed: %s", exc)


def ensure_provider() -> bool:
    """Initialize the global TracerProvider once. Safe to call repeatedly."""
    global _PROVIDER_READY, _TRACER
    if not tracing_enabled():
        return False
    if _PROVIDER_READY:
        return _TRACER is not None
    with _PROVIDER_LOCK:
        if _PROVIDER_READY:
            return _TRACER is not None
        _PROVIDER_READY = True
        try:
            from opentelemetry import trace
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
        except ImportError:
            logger.info(
                "DATAFLOW_ENABLE_TRACING=1 but OpenTelemetry SDK is not installed; "
                "tracing disabled. Install opentelemetry-api + opentelemetry-sdk "
                "(+ opentelemetry-exporter-otlp for remote export)."
            )
            return False

        service_name = os.getenv("DATAFLOW_SERVICE_NAME", "dataflow-api")
        resource = Resource.create(
            {
                "service.name": service_name,
                "service.namespace": "dataflow",
            }
        )
        provider = TracerProvider(resource=resource)

        endpoint = otlp_endpoint()
        exporter: Any = None
        if endpoint:
            try:
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                    OTLPSpanExporter,
                )

                exporter = OTLPSpanExporter(endpoint=endpoint, insecure=_otlp_insecure())
            except ImportError:
                try:
                    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                        OTLPSpanExporter as HttpExporter,
                    )

                    exporter = HttpExporter(endpoint=endpoint)
                except ImportError:
                    logger.warning(
                        "OTLP endpoint configured (%s) but no OTLP exporter "
                        "package is installed; falling back to console exporter.",
                        endpoint,
                    )
        if exporter is None and os.getenv("DATAFLOW_TRACE_CONSOLE", "0").lower() in (
            "1",
            "true",
            "yes",
        ):
            exporter = ConsoleSpanExporter()
        if exporter is not None:
            provider.add_span_processor(BatchSpanProcessor(exporter))

        # Only install as global if nothing else has claimed the slot — a host
        # that already wired OTel (e.g. an APM agent) must keep its provider.
        existing = trace.get_tracer_provider()
        if type(existing).__name__ in {"ProxyTracerProvider", "NoOpTracerProvider", "_DefaultTracerProvider"}:
            trace.set_tracer_provider(provider)
            active = provider
        else:
            active = existing
        _TRACER = active.get_tracer(_TRACER_NAME)
        logger.info(
            "OpenTelemetry tracing enabled (service=%s, exporter=%s)",
            service_name,
            type(exporter).__name__ if exporter else "none",
        )
        return True


def shutdown_tracing() -> None:
    """Flush and shut down the provider. Called from the FastAPI lifespan."""
    if not _PROVIDER_READY:
        return
    try:
        from opentelemetry import trace

        provider = trace.get_tracer_provider()
        shutdown = getattr(provider, "shutdown", None)
        if callable(shutdown):
            shutdown()
    except Exception as exc:
        logger.debug("tracing shutdown failed: %s", exc)


def reset_for_tests() -> None:
    """Drop provider state so tests can reconfigure cleanly."""
    global _PROVIDER_READY, _TRACER
    with _PROVIDER_LOCK:
        _PROVIDER_READY = False
        _TRACER = None
    set_correlation_id("")
    set_traceparent("")


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


class _NoopSpan:
    def set_attribute(self, *_a: Any, **_k: Any) -> None:
        return None

    def record_exception(self, *_a: Any, **_k: Any) -> None:
        return None

    def set_status(self, *_a: Any, **_k: Any) -> None:
        return None

    def is_recording(self) -> bool:
        return False


def _get_tracer() -> Any:
    if not tracing_enabled():
        return None
    if not _PROVIDER_READY:
        ensure_provider()
    return _TRACER


def _otel_trace() -> Any:
    try:
        from opentelemetry import trace

        return trace
    except ImportError:
        return None


def _otel_context() -> Any:
    try:
        from opentelemetry import context

        return context
    except ImportError:
        return None


def _otel_status() -> Any:
    try:
        from opentelemetry.trace import status

        return status
    except ImportError:
        return None


def _span_kind(kind: str) -> Any:
    trace = _otel_trace()
    if trace is None:
        return None
    mapping = {
        "internal": trace.SpanKind.INTERNAL,
        "server": trace.SpanKind.SERVER,
        "client": trace.SpanKind.CLIENT,
        "producer": trace.SpanKind.PRODUCER,
        "consumer": trace.SpanKind.CONSUMER,
    }
    return mapping.get(kind, trace.SpanKind.INTERNAL)


def _otlp_insecure() -> bool:
    return os.getenv("DATAFLOW_OTLP_INSECURE", "1").lower() in ("1", "true", "yes")
