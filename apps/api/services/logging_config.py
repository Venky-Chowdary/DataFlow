"""Structured logging for the Datawrap API and worker processes.

Before this module the API process configured logging not at all: only
``src/worker_main.py`` ever called ``basicConfig``, so under Uvicorn every
``logger.warning`` from the engine, the connectors, and reconciliation inherited
whatever handler Uvicorn installed, with no timestamp format we controlled, no
per-module level tuning, and no structured fields.

The bigger problem was correlation. A transfer touches the router, the
scheduler, the engine, several connectors, and the reconciler, and almost none
of those log lines carried the ``job_id`` — so the answer to "what happened to
job X?" could not be reached from logs at all. The ``correlation_id`` the HTTP
middleware already minted reached span attributes, the response header, and the
audit log, but never a log record.

This module fixes both:

* One :func:`configure_logging` entry point, called from the app lifespan and
  the worker, honouring ``DATAFLOW_LOG_LEVEL`` and ``DATAFLOW_LOG_FORMAT``
  (``json`` for shippable output, ``text`` for a readable local console).
* A :class:`ContextFilter` that stamps every record with the ambient
  ``correlation_id``, ``job_id``, and ``trace_id``, so a single grep or a single
  structured query returns the whole story of one job across every module.
* :func:`log_context`, a context manager that binds ``job_id``,
  ``workspace_id``, ``schedule_id``, ``run_id``, ``attempt`` and ``phase`` for
  a block (:func:`job_log_context` is the job-only form used by the engine).

Both the filter and the context are ``ContextVar``-backed, which means they
follow ``async`` tasks correctly and are captured explicitly for worker threads
rather than leaking between concurrent jobs.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any, Iterator

from services.brand_env import getenv_brand
from services.log_redaction import redact_log_text, redact_log_value

#: Job currently being executed on this task/thread. Bound by
#: :func:`job_log_context` so engine and connector logs self-identify.
_JOB_ID: ContextVar[str] = ContextVar("df_log_job_id", default="")

#: Ambient identity bound by :func:`log_context`, keyed by record field name.
_BOUND_FIELDS: dict[str, ContextVar[str]] = {
    "job_id": _JOB_ID,
    "workspace_id": ContextVar("df_log_workspace_id", default=""),
    "schedule_id": ContextVar("df_log_schedule_id", default=""),
    "run_id": ContextVar("df_log_run_id", default=""),
    "attempt": ContextVar("df_log_attempt", default=""),
    "phase": ContextVar("df_log_phase", default=""),
}

#: Fields a LogRecord always carries after :class:`ContextFilter` runs.
_CONTEXT_FIELDS = ("correlation_id", "trace_id", *_BOUND_FIELDS)

_MAX_CONTEXT_CHARS = 256

#: Attributes present on every LogRecord. Anything outside this set was passed
#: by the caller via ``extra=`` and is worth emitting as a structured field.
_RESERVED_RECORD_ATTRS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)

_CONFIGURED = False


def current_job_id() -> str:
    """Job id bound to the current context, or an empty string."""
    return _JOB_ID.get()


def set_job_id(job_id: str | None) -> Token:
    """Bind a job id to the current context, returning a reset token."""
    return _JOB_ID.set((job_id or "").strip())


def reset_job_id(token: Token) -> None:
    _JOB_ID.reset(token)


@contextmanager
def log_context(**ids: Any) -> Iterator[None]:
    """Stamp every record emitted in this block with the given identity fields.

    Accepts the keys of ``_BOUND_FIELDS``; ``None`` clears a field for the
    block. Unknown keys are ignored with a warning rather than failing the
    caller. Values are restored on exit, so nested scopes compose.
    """
    tokens: list[tuple[ContextVar[str], Token]] = []
    unknown = sorted(key for key in ids if key not in _BOUND_FIELDS)
    if unknown:
        logging.getLogger(__name__).warning(
            "log_context ignored unknown fields: %s", ",".join(unknown)
        )
    try:
        for key, value in ids.items():
            var = _BOUND_FIELDS.get(key)
            if var is None:
                continue
            text = "" if value is None else str(value).strip()
            tokens.append((var, var.set(text[:_MAX_CONTEXT_CHARS])))
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


@contextmanager
def job_log_context(job_id: str | None) -> Iterator[None]:
    """Stamp every log record emitted in this block with ``job_id``.

    Used by the transfer engine so that connector and reconciliation logs, which
    have no idea a job exists, still become attributable to one.
    """
    with log_context(job_id=job_id):
        yield


class ContextFilter(logging.Filter):
    """Attach correlation, job, and trace identity to every record.

    Implemented as a filter rather than a formatter so both the JSON and text
    formatters see the same fields, and so records forwarded to third-party
    handlers (Uvicorn's, an APM agent's) are enriched too.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        for field, var in _BOUND_FIELDS.items():
            if not hasattr(record, field):
                setattr(record, field, var.get())
        if not hasattr(record, "correlation_id"):
            record.correlation_id = _safe_correlation_id()
        if not hasattr(record, "trace_id"):
            record.trace_id = _safe_trace_id()
        return True


def _safe_correlation_id() -> str:
    """Correlation id, tolerating a partially initialised tracing module.

    Logging must never be the reason a request fails, so every lookup here is
    best-effort and degrades to an empty field.
    """
    try:
        from services.tracing import get_correlation_id

        return get_correlation_id() or ""
    except Exception:
        return ""


def _safe_trace_id() -> str:
    try:
        from services.tracing import current_trace_id

        return current_trace_id() or ""
    except Exception:
        return ""


class JsonFormatter(logging.Formatter):
    """Render records as one JSON object per line.

    Chosen over ``python-json-logger`` to avoid a dependency for ~30 lines, and
    so the exact field names stay under our control: log shippers key alerts off
    these names, which makes them an interface rather than an implementation
    detail.
    """

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        """ISO-8601 UTC with milliseconds — avoid ``%03d`` (invalid on Windows strftime)."""
        ct = self.converter(record.created)
        base = time.strftime("%Y-%m-%dT%H:%M:%S", ct)
        ms = int(record.msecs)
        return f"{base}.{ms:03d}Z"

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": redact_log_text(record.getMessage()),
        }
        for field in _CONTEXT_FIELDS:
            value = getattr(record, field, "")
            if value:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = redact_log_text(
                self.formatException(record.exc_info)
            )
        if record.stack_info:
            payload["stack"] = redact_log_text(self.formatStack(record.stack_info))
        # Anything the caller passed via extra= becomes a first-class field so
        # structured queries can filter on it (rows=, table=, dest_type=, ...).
        for key, value in record.__dict__.items():
            if key in _RESERVED_RECORD_ATTRS or key in payload:
                continue
            if key.startswith("_"):
                continue
            payload[key] = _jsonable(redact_log_value(key, value))
        return json.dumps(payload, default=str)


def _jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


class TextFormatter(logging.Formatter):
    """Human-readable console format that still shows the correlation fields."""

    default_fmt = "%(asctime)s %(levelname)-7s [%(name)s]%(context)s %(message)s"

    def __init__(self) -> None:
        super().__init__(self.default_fmt)

    def format(self, record: logging.LogRecord) -> str:
        bits = [
            f"{field}={getattr(record, field)}"
            for field in _CONTEXT_FIELDS
            if getattr(record, field, "")
        ]
        record.context = f" ({' '.join(bits)})" if bits else ""
        return redact_log_text(super().format(record))


def log_format() -> str:
    """``json`` or ``text``; JSON is the default outside a TTY.

    A developer running the API locally gets readable output, while a container
    (no TTY) emits shippable JSON without anyone remembering to set a variable.
    """
    explicit = (getenv_brand("LOG_FORMAT") or "").strip().lower()
    if explicit in ("json", "text"):
        return explicit
    return "text" if sys.stderr.isatty() else "json"


def configure_logging(*, force: bool = False) -> None:
    """Install Datawrap's root logging configuration. Idempotent.

    Safe to call from both the API lifespan and the worker entry point; the
    second call is a no-op unless ``force`` is set (used by tests).
    """
    global _CONFIGURED
    if _CONFIGURED and not force:
        return

    level_name = (getenv_brand("LOG_LEVEL") or "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)

    formatter: logging.Formatter = (
        JsonFormatter() if log_format() == "json" else TextFormatter()
    )
    context_filter = ContextFilter()

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(formatter)
    handler.addFilter(context_filter)
    handler.addFilter(_LibraryFloorFilter())

    root = logging.getLogger()
    # Replace rather than append: Uvicorn may already have installed a handler,
    # and keeping both would double every line.
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)
    try:
        from services.otel_logging import install_otel_log_handler

        otel_handler = install_otel_log_handler(root)
        if otel_handler is not None and not any(
            isinstance(f, _LibraryFloorFilter) for f in otel_handler.filters
        ):
            otel_handler.addFilter(_LibraryFloorFilter())
    except Exception as exc:  # noqa: BLE001 - local logging must remain available
        # Local logging must remain available even if remote export is broken.
        logging.getLogger(__name__).warning(
            "OpenTelemetry log handler bootstrap failed: %s",
            exc,
        )

    # Uvicorn's loggers propagate to root once their own handlers are dropped,
    # so access and error logs pick up the same format and context fields.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv = logging.getLogger(name)
        uv.handlers.clear()
        uv.propagate = True

    # Third-party libraries that are chatty at INFO and drown out our own lines.
    for name, noisy_level in _library_levels().items():
        logging.getLogger(name).setLevel(noisy_level)

    _CONFIGURED = True


def _library_levels() -> dict[str, int]:
    """Per-library floors, overridable via ``DATAFLOW_LOG_LEVELS``.

    Format: ``pymongo=DEBUG,botocore=INFO``. Anything unparseable is skipped
    rather than raising — a malformed log setting must not stop the process from
    booting.
    """
    defaults = dict(SENSITIVE_LIBRARY_FLOORS)
    defaults.update({"asyncio": logging.WARNING, "watchfiles": logging.WARNING})
    raw = (getenv_brand("LOG_LEVELS") or "").strip()
    for pair in raw.split(","):
        if "=" not in pair:
            continue
        name, _, level_name = pair.partition("=")
        level = getattr(logging, level_name.strip().upper(), None)
        if isinstance(level, int):
            name = name.strip()
            floor = _sensitive_floor(name)
            defaults[name] = level if floor is None else max(level, floor)
    return defaults


# Libraries that log credentials, auth headers, signed URLs, DSNs or bound row
# values below WARNING (kafka: PLAIN auth_bytes; botocore: Authorization header
# and signature; urllib3/httpx: full URL incl. query; sqlalchemy.engine and
# pymongo.command: SQL params / command documents). The floor is a hard minimum:
# DATAFLOW_LOG_LEVELS may raise it, never lower it, and _LibraryFloorFilter on
# our handlers enforces it even if a library lowers its own logger level
# (boto3.set_stream_logger does).
SENSITIVE_LIBRARY_FLOORS: dict[str, int] = {
    name: logging.WARNING
    for name in (
        "kafka",
        "botocore",
        "boto3",
        "urllib3",
        "httpx",
        "httpcore",
        "snowflake.connector",
        "google.auth",
        "azure",
        "paramiko",
        "pymongo",
        "sqlalchemy.engine",
    )
}


def _sensitive_floor(logger_name: str) -> int | None:
    name = logger_name
    while name:
        floor = SENSITIVE_LIBRARY_FLOORS.get(name)
        if floor is not None:
            return floor
        name = name.rpartition(".")[0]
    return None


class _LibraryFloorFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        floor = _sensitive_floor(record.name)
        return floor is None or record.levelno >= floor


def reset_for_tests() -> None:
    """Allow a test to reconfigure logging from scratch."""
    global _CONFIGURED
    _CONFIGURED = False
