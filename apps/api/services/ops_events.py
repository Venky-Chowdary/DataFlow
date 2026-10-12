"""Canonical operational events: one stable name, one outcome, safe fields.

Free-text log lines answer "what did this module say" but not "how many
transfers failed in Gate-8 this hour" — a shipper cannot query a sentence.
:func:`emit_event` is the single way to log a lifecycle boundary (start, finish,
retry, hold, state change) as a structured record:

* ``event`` is a stable ``domain.object.verb`` name with no dynamic values, and
  is also the whole message text, so alerts key off the name, never prose.
* ``outcome`` comes from a closed set (:data:`OUTCOMES`) and picks the default
  level: failed/errored are ERROR, retrying/held are WARNING, the rest INFO.
* Fields are scalars only, redacted through the canonical
  :func:`services.log_redaction.redact_log_value`, and truncated. Containers are
  dropped and counted, so a caller cannot ship a row or a payload by accident.
* An error contributes only its class name and a coarse category — never its
  message (driver messages embed cell values and DSNs) and never a traceback.

Ambient identity (job, workspace, schedule, run, correlation, trace) is stamped
by :class:`services.logging_config.ContextFilter`; bind it with
:func:`services.logging_config.log_context` rather than repeating it here.

Emission never raises into business code: a contract violation is logged as a
WARNING and a failure inside the helper degrades to a minimal fallback line.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import contextmanager
from typing import Any, Iterator

from services.log_redaction import redact_log_value

_logger = logging.getLogger(__name__)

#: Closed outcome vocabulary shared by every event.
OUTCOMES = frozenset(
    {"started", "succeeded", "failed", "errored", "retrying", "held", "skipped", "cancelled"}
)

#: ``domain.object.verb`` — lowercase segments, at least two, no dynamic values.
EVENT_NAME = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")

#: Emitted in place of a malformed event name (the bad name may carry data).
INVALID_EVENT = "ops.event.invalid"
#: Emitted when the helper itself fails.
EMIT_FAILED_EVENT = "ops.event.emit_failed"

MAX_FIELD_CHARS = 256

_DEFAULT_LEVELS = {
    "failed": logging.ERROR,
    "errored": logging.ERROR,
    "retrying": logging.WARNING,
    "held": logging.WARNING,
}

_CORE_FIELDS = frozenset(
    {"event", "outcome", "duration_ms", "error_type", "error_category", "dropped_fields"}
)

#: ``extra=`` keys that collide with LogRecord attributes make ``logging`` raise.
_RECORD_ATTRS = frozenset(
    logging.LogRecord("", logging.INFO, "", 0, "", None, None).__dict__
) | {"message", "asctime"}

_SCALARS = (str, int, float, bool)

#: Interruptions, not defects: a cancelled task, Ctrl-C, or a closed generator.
CANCELLATION_ERRORS = (asyncio.CancelledError, KeyboardInterrupt, GeneratorExit)


def validate_event(event: str, outcome: str) -> None:
    """Raise ``ValueError`` when ``event``/``outcome`` break the contract.

    For tests and static checks; :func:`emit_event` never raises and degrades
    a violation to a WARNING instead.
    """
    if not isinstance(event, str) or not EVENT_NAME.match(event):
        raise ValueError(f"event name must match {EVENT_NAME.pattern}")
    if outcome not in OUTCOMES:
        raise ValueError(f"outcome must be one of {sorted(OUTCOMES)}")


def error_category(error: BaseException) -> str:
    """Coarse, value-free category for ``error``.

    Reuses the writer's connection-loss classifier rather than inventing a
    second taxonomy; anything it does not recognise is ``unknown``.
    """
    if isinstance(error, TimeoutError):
        return "timeout"
    try:
        from connectors.write_resilience import is_connection_lost

        if is_connection_lost(error):
            return "connection_lost"
    except Exception as exc:  # noqa: BLE001 - classification is best-effort
        _logger.debug("error category lookup failed: %s", type(exc).__name__)
    return "unknown"


def _safe_fields(fields: dict[str, Any]) -> tuple[dict[str, Any], int]:
    safe: dict[str, Any] = {}
    dropped = 0
    for key, value in fields.items():
        if (
            not isinstance(key, str)
            or key.startswith("_")
            or key in _RECORD_ATTRS
            or key in _CORE_FIELDS
            or (value is not None and not isinstance(value, _SCALARS))
        ):
            dropped += 1
            continue
        # Redact before truncating: a cut can split a secret past the pattern.
        value = redact_log_value(key, value)
        if isinstance(value, str) and len(value) > MAX_FIELD_CHARS:
            value = value[:MAX_FIELD_CHARS]
        safe[key] = value
    return safe, dropped


def emit_event(
    logger: logging.Logger,
    event: str,
    *,
    outcome: str,
    level: int | None = None,
    duration_ms: float | None = None,
    error: BaseException | None = None,
    **fields: Any,
) -> None:
    """Log one canonical operational event. Never raises."""
    try:
        violations: list[str] = []
        if not isinstance(event, str) or not EVENT_NAME.match(event):
            violations.append("event_name")
            event = INVALID_EVENT
        if outcome not in OUTCOMES:
            violations.append("outcome")
            outcome = "errored"
        if violations:
            resolved_level = logging.WARNING
        elif level is not None:
            resolved_level = int(level)
        else:
            resolved_level = _DEFAULT_LEVELS.get(outcome, logging.INFO)

        safe, dropped = _safe_fields(fields)
        extra: dict[str, Any] = {"event": event, "outcome": outcome}
        if duration_ms is not None:
            extra["duration_ms"] = round(float(duration_ms), 3)
        if error is not None:
            extra["error_type"] = type(error).__name__
            extra["error_category"] = error_category(error)
        if dropped:
            extra["dropped_fields"] = dropped
        if violations:
            extra["contract_violation"] = ",".join(violations)
        extra.update(safe)
        logger.log(resolved_level, event, extra=extra)
    except Exception as exc:  # noqa: BLE001 - logging must never fail the caller
        _logger.warning(
            EMIT_FAILED_EVENT,
            extra={
                "event": EMIT_FAILED_EVENT,
                "outcome": "errored",
                "error_type": type(exc).__name__,
            },
        )


@contextmanager
def operation(
    logger: logging.Logger,
    prefix: str,
    *,
    emit_start: bool = False,
    **fields: Any,
) -> Iterator[dict[str, Any]]:
    """Time a block and emit ``{prefix}.succeeded``, ``.failed`` or ``.cancelled``.

    Yields a dict the block fills with result fields (counts, verdicts). Task
    cancellation, KeyboardInterrupt and GeneratorExit emit ``.cancelled`` at
    INFO. The original exception instance is always re-raised untouched.
    """
    result: dict[str, Any] = {}
    if emit_start:
        emit_event(logger, f"{prefix}.started", outcome="started", **fields)
    started = time.monotonic()
    try:
        yield result
    except CANCELLATION_ERRORS as exc:
        emit_event(
            logger,
            f"{prefix}.cancelled",
            outcome="cancelled",
            duration_ms=(time.monotonic() - started) * 1000.0,
            error=exc,
            **{**fields, **result},
        )
        raise
    except BaseException as exc:
        emit_event(
            logger,
            f"{prefix}.failed",
            outcome="failed",
            duration_ms=(time.monotonic() - started) * 1000.0,
            error=exc,
            **{**fields, **result},
        )
        raise
    emit_event(
        logger,
        f"{prefix}.succeeded",
        outcome="succeeded",
        duration_ms=(time.monotonic() - started) * 1000.0,
        **{**fields, **result},
    )
