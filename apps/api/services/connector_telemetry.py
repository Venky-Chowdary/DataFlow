"""Safe operational telemetry for connector boundaries."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from services.error_handling import RetryBudget
from services.ops_events import emit_event

_LOG = logging.getLogger("dataflow.connector")
_ALLOWED_FIELDS = frozenset(
    {
        "driver_family",
        "role",
        "surface",
        "connector_id",
        "object_count",
        "column_count",
        "row_count",
        "http_status",
        "request_id",
    }
)
_SAFE_CATEGORIES = frozenset(
    {
        "auth_config",
        "secret_undecryptable",
        "not_found",
        "unsupported",
        "no_input",
        "invalid_request",
        "probe_failed",
        "not_connected",
    }
)
_LITERAL_DRIVER_FAMILIES = frozenset({"http", "sdk_declarative", "singer"})
_LEVELS = {
    "succeeded": logging.INFO,
    "failed": logging.WARNING,
    "errored": logging.ERROR,
    "skipped": logging.INFO,
}
_ACTIVE_OPERATIONS: ContextVar[dict[str, ConnectorOp]] = ContextVar(
    "dataflow_connector_operations",
    default={},
)


def _safe_category(category: str) -> str:
    return category if category in _SAFE_CATEGORIES else "unsupported"


@dataclass
class ConnectorOp:
    op: str
    fields: dict[str, Any] = field(default_factory=dict)
    started_at: float = field(default_factory=time.perf_counter)
    outcome: str = ""
    category: str | None = None
    error: BaseException | None = None

    def update(self, **fields: Any) -> None:
        self.fields.update(
            (key, value) for key, value in fields.items() if key in _ALLOWED_FIELDS
        )

    def _mark(
        self,
        outcome: str,
        *,
        category: str | None = None,
        error: BaseException | None = None,
        **fields: Any,
    ) -> None:
        self.update(**fields)
        self.outcome = outcome
        self.category = _safe_category(category) if category is not None else None
        self.error = error

    def _fill_empty(self, fields: dict[str, Any]) -> None:
        for key, value in fields.items():
            current = self.fields.get(key)
            if current in (None, "") or (
                isinstance(current, str) and current.lower() == "unknown"
            ):
                if key in _ALLOWED_FIELDS:
                    self.fields[key] = value

    def succeeded(self, **counts: Any) -> None:
        self._mark("succeeded", **counts)

    def failed(
        self,
        *,
        category: str,
        error: BaseException | None = None,
        **fields: Any,
    ) -> None:
        self._mark("failed", category=category, error=error, **fields)

    def errored(self, error: BaseException, **fields: Any) -> None:
        self._mark("errored", error=error, **fields)

    def skipped(self, *, category: str) -> None:
        self._mark("skipped", category=category)

    def _emit_terminal(self) -> None:
        outcome = self.outcome or "succeeded"
        fields = dict(self.fields)
        if self.category is not None:
            fields["category"] = self.category
        emit_event(
            _LOG,
            f"connector.{self.op}.{outcome}",
            outcome=outcome,
            level=_LEVELS[outcome],
            duration_ms=(time.perf_counter() - self.started_at) * 1000.0,
            error=self.error,
            **fields,
        )


@contextmanager
def connector_operation(
    op: str,
    *,
    driver_family: str,
    role: str = "",
    surface: str = "",
    connector_id: str | None = None,
) -> Iterator[ConnectorOp]:
    fields = {
        "driver_family": driver_family,
        "role": role,
        "surface": surface,
        "connector_id": connector_id,
    }
    active = _ACTIVE_OPERATIONS.get()
    existing = active.get(op)
    if existing is not None:
        existing._fill_empty(fields)
        yield existing
        return

    operation = ConnectorOp(op=op)
    operation.update(**fields)
    token = _ACTIVE_OPERATIONS.set({**active, op: operation})
    try:
        try:
            yield operation
        except BaseException as exc:
            if operation.outcome != "failed":
                operation.errored(exc)
            elif operation.error is None:
                operation.error = exc
            raise
        else:
            if not operation.outcome:
                operation.succeeded()
    finally:
        _ACTIVE_OPERATIONS.reset(token)
        operation._emit_terminal()


def log_retry(
    operation: str,
    *,
    driver_family: str,
    budget: RetryBudget,
    request_id: str | None = None,
) -> Callable[[Exception, float], None]:
    def on_transient(exc: Exception, delay: float) -> None:
        outcome = (
            "retrying"
            if budget.attempts_made < budget.max_attempts
            else "failed"
        )
        event = (
            "connector.retry.scheduled"
            if outcome == "retrying"
            else "connector.retry.exhausted"
        )
        level = logging.WARNING if outcome == "retrying" else logging.ERROR
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        fields: dict[str, Any] = {
            "attempt": budget.attempts_made,
            "max_attempts": budget.max_attempts,
            "delay_ms": int(round(delay * 1000)),
            "operation": operation,
            "driver_family": _resolve_retry_driver_family(driver_family),
        }
        if isinstance(status, int) and not isinstance(status, bool):
            fields["http_status"] = status
        if request_id:
            fields["request_id"] = request_id
        emit_event(
            _LOG,
            event,
            outcome=outcome,
            level=level,
            error=exc,
            **fields,
        )

    return on_transient


def driver_family(raw: str) -> str:
    cid = str(raw or "").lower().strip()
    try:
        from connectors.sdk import list_descriptors
        from src.transfer.connector_capabilities import (
            CATALOG_ID_ALIASES,
            file_source_types,
        )
        from src.transfer.connector_registry import registered_driver_types

        descriptors = list_descriptors()
        allowed = (
            set(registered_driver_types())
            | set(file_source_types())
            | {"generic_sql", "rest_api"}
            | {descriptor.id for descriptor in descriptors}
        )
        if cid in _LITERAL_DRIVER_FAMILIES or cid in allowed:
            return cid
        for descriptor in descriptors:
            if cid in {str(item).lower().strip() for item in descriptor.catalog_ids}:
                return descriptor.id
        from src.transfer.engine_family import sql_duplex_family

        alias = sql_duplex_family(cid) or CATALOG_ID_ALIASES.get(cid)
        return alias if alias in allowed else "unknown"
    except Exception as exc:
        _LOG.debug("driver family resolution failed: %s", type(exc).__name__)
        return "unknown"


def _resolve_retry_driver_family(raw: str) -> str:
    return raw if raw in _LITERAL_DRIVER_FAMILIES else driver_family(raw)
