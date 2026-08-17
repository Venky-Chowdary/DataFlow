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
* :func:`job_log_context`, a context manager that binds a ``job_id`` for the
  duration of a transfer, including across the thread-pool boundary.

Both the filter and the context are ``ContextVar``-backed, which means they
follow ``async`` tasks correctly and are captured explicitly for worker threads
rather than leaking between concurrent jobs.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any, Iterator

from services.brand_env import getenv_brand

#: Job currently being executed on this task/thread. Bound by
#: :func:`job_log_context` so engine and connector logs self-identify.
_JOB_ID: ContextVar[str] = ContextVar("df_log_job_id", default="")

#: Fields a LogRecord always carries after :class:`ContextFilter` runs.
_CONTEXT_FIELDS = ("correlation_id", "job_id", "trace_id")

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
def job_log_context(job_id: str | None) -> Iterator[None]:
    """Stamp every log record emitted in this block with ``job_id``.

    Used by the transfer engine so that connector and reconciliation logs, which
    have no idea a job exists, still become attributable to one.
    """
    token = set_job_id(job_id)
    try:
        yield
    finally:
        reset_job_id(token)


class ContextFilter(logging.Filter):
    """Attach correlation, job, and trace identity to every record.

    Implemented as a filter rather than a formatter so both the JSON and text
    formatters see the same fields, and so records forwarded to third-party
    handlers (Uvicorn's, an APM agent's) are enriched too.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "job_id"):
            record.job_id = _JOB_ID.get()
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
            "msg": record.getMessage(),
        }
        for field in _CONTEXT_FIELDS:
            value = getattr(record, field, "")
            if value:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        # Anything the caller passed via extra= becomes a first-class field so
        # structured queries can filter on it (rows=, table=, dest_type=, ...).
        for key, value in record.__dict__.items():
            if key in _RESERVED_RECORD_ATTRS or key in payload:
                continue
            if key.startswith("_"):
                continue
            payload[key] = _jsonable(value)
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
        return super().format(record)


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

    root = logging.getLogger()
    # Replace rather than append: Uvicorn may already have installed a handler,
    # and keeping both would double every line.
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)

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
    defaults = {
        "pymongo": logging.WARNING,
        "botocore": logging.WARNING,
        "boto3": logging.WARNING,
        "urllib3": logging.WARNING,
        "snowflake.connector": logging.WARNING,
        "sqlalchemy.engine": logging.WARNING,
        "asyncio": logging.WARNING,
        "watchfiles": logging.WARNING,
    }
    raw = (getenv_brand("LOG_LEVELS") or "").strip()
    for pair in raw.split(","):
        if "=" not in pair:
            continue
        name, _, level_name = pair.partition("=")
        level = getattr(logging, level_name.strip().upper(), None)
        if isinstance(level, int):
            defaults[name.strip()] = level
    return defaults


def reset_for_tests() -> None:
    """Allow a test to reconfigure logging from scratch."""
    global _CONFIGURED
    _CONFIGURED = False
