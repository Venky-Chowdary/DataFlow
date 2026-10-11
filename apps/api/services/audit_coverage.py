"""Audit coverage for sensitive API routes and repeated authorization denials."""

from __future__ import annotations

import logging
import re
import threading
import time
from collections import OrderedDict
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from services import audit_log

logger = logging.getLogger(__name__)

# Entries use the registered route template as a prefix. Dynamic segments are
# written as FastAPI declares them, which keeps coverage testable against app.routes.
SENSITIVE_ROUTE_RULES: tuple[tuple[str, str, str], ...] = (
    ("POST", "/api/v1/connectors/test", "connector.test"),
    ("POST", "/api/v1/connectors/", "connector.create"),
    ("DELETE", "/api/v1/connectors/{connector_id}", "connector.delete"),
    ("POST", "/api/v1/connectors/saved/{connector_id}/rotate-secrets", "connector.rotate"),
    ("POST", "/api/v1/connectors/saved/{connector_id}/test", "connector.test"),
    ("POST", "/api/v1/connectors/saved", "connector.create"),
    ("PUT", "/api/v1/connectors/saved/{connector_id}", "connector.update"),
    ("DELETE", "/api/v1/connectors/saved/{connector_id}", "connector.delete"),
    ("GET", "/api/v1/workspace/sso", "secret.sso.read"),
    ("PATCH", "/api/v1/workspace/sso/{sso_type}", "secret.sso.update"),
    ("POST", "/api/v1/workspace/sso/{sso_type}/test", "secret.sso.test"),
    ("GET", "/api/v1/workspace/api-keys", "secret.api_key.read"),
    ("POST", "/api/v1/workspace/api-keys", "secret.api_key.create"),
    ("DELETE", "/api/v1/workspace/api-keys/{key_id}", "secret.api_key.delete"),
    ("POST", "/api/v1/iam/api-keys/{key_id}/rotate", "secret.api_key.rotate"),
    ("GET", "/api/v1/workspace/tenant/byok-keys", "secret.byok.read"),
    ("POST", "/api/v1/workspace/tenant/byok-keys", "secret.byok.create"),
    ("POST", "/api/v1/workspace/tenant/byok-keys/{key_id}/rotate", "secret.byok.rotate"),
    ("POST", "/api/v1/transfer/plans/{plan_id}/approve", "transfer_plan.approve.request"),
    (
        "POST",
        "/api/v1/schedules/{schedule_id}/approvals/{approval_id}/approve",
        "schedule.approval.approve",
    ),
    (
        "POST",
        "/api/v1/schedules/{schedule_id}/approvals/{approval_id}/reject",
        "schedule.approval.reject",
    ),
)

_DENIAL_WINDOW_SECONDS = 60.0
_DENIAL_MAX_KEYS = 10_000
_DENIAL_LOCK = threading.Lock()
_DENIALS: OrderedDict[tuple[str, str, str], tuple[float, int]] = OrderedDict()
_FAILURE_LOCK = threading.Lock()
_AUDIT_ACCESS_FAILURES = 0


def audit_access_failure_count() -> int:
    """Return the process-local count of sensitive-route audit write failures."""
    with _FAILURE_LOCK:
        return _AUDIT_ACCESS_FAILURES


def _record_audit_failure(exc: Exception) -> None:
    global _AUDIT_ACCESS_FAILURES
    with _FAILURE_LOCK:
        _AUDIT_ACCESS_FAILURES += 1
    logger.error(
        "Sensitive-route audit write failed (%s)",
        type(exc).__name__,
        exc_info=exc,
    )


def _rule_for_request(request: Request) -> str | None:
    path = request.url.path
    method = request.method.upper()
    for rule_method, prefix, action in SENSITIVE_ROUTE_RULES:
        if rule_method == method and _matches_route_template(path, prefix):
            return action
    return None


def _matches_route_template(path: str, template: str) -> bool:
    parts = re.split(r"(\{[^/{}]+\})", template)
    pattern = "".join(
        "[^/]+" if part.startswith("{") and part.endswith("}") else re.escape(part)
        for part in parts
    )
    return re.fullmatch(pattern, path) is not None


def _correlation_id(request: Request) -> str | None:
    value = request.headers.get("X-Correlation-ID") or getattr(
        request.state, "correlation_id", None
    )
    return str(value) if value else None


def _auth_metadata(request: Request) -> tuple[str, str | None]:
    user = getattr(request.state, "user", None)
    user = user if isinstance(user, dict) else {}
    api_key_id = getattr(request.state, "api_key_id", None) or user.get("api_key_id")
    auth_kind = (
        getattr(request.state, "auth_kind", None)
        or user.get("auth_kind")
        or ("session" if user else "anonymous")
    )
    return str(auth_kind), str(api_key_id) if api_key_id else None


class AuditAccessMiddleware(BaseHTTPMiddleware):
    """Record sensitive-route metadata after the handler response is available."""

    async def dispatch(self, request: Request, call_next) -> Response:
        try:
            response = await call_next(request)
        except Exception:
            self._record(request, status=500)
            raise
        self._record(request, status=response.status_code)
        return response

    @staticmethod
    def _record(request: Request, *, status: int) -> None:
        action = _rule_for_request(request)
        if action is None:
            return

        auth_kind, api_key_id = _auth_metadata(request)
        details: dict[str, Any] = {
            "auth_kind": auth_kind,
            "method": request.method.upper(),
            "path": request.url.path,
            "status": status,
        }
        if api_key_id:
            details["api_key_id"] = api_key_id

        try:
            audit_log.append_audit_event(
                action=action,
                resource=request.url.path,
                actor=audit_log.actor_from_request(request),
                level="error" if status >= 500 else "info",
                correlation_id=_correlation_id(request),
                workspace_id=audit_log.workspace_id_from_request(request),
                details=details,
            )
        except Exception as exc:
            _record_audit_failure(exc)


def _suppressed_since_last(actor: str, permission: str, path: str) -> int | None:
    """Deduplicate denials in process-local, 60-second windows with a 10k-key LRU.

    The state is intentionally process-local and bounded to 10,000 keys; it is a
    noise-control aid, not a cross-replica authorization or accounting primitive.
    """
    now = time.monotonic()
    key = (actor, permission, path)
    with _DENIAL_LOCK:
        prior = _DENIALS.get(key)
        if prior is None:
            _DENIALS[key] = (now, 0)
            suppressed = 0
        else:
            started, count = prior
            _DENIALS.move_to_end(key)
            if now - started < _DENIAL_WINDOW_SECONDS:
                _DENIALS[key] = (started, count + 1)
                return None
            _DENIALS[key] = (now, 0)
            suppressed = count
        while len(_DENIALS) > _DENIAL_MAX_KEYS:
            _DENIALS.popitem(last=False)
    return suppressed


def unruled_route_suppressed_since_last(request: Request) -> int | None:
    """Apply the denial-window keying to an allowed unruled-route warning."""
    return _suppressed_since_last(
        audit_log.actor_from_request(request),
        "no_rule",
        request.url.path,
    )


def record_authz_denial(
    request: Request,
    *,
    required_permission: str,
    effective_role: str,
    reason: str | None = None,
) -> None:
    actor = audit_log.actor_from_request(request)
    path = request.url.path
    suppressed = _suppressed_since_last(actor, required_permission, path)
    if suppressed is None:
        return

    user = getattr(request.state, "user", None)
    api_key_id = getattr(request.state, "api_key_id", None)
    if not api_key_id and isinstance(user, dict):
        api_key_id = user.get("api_key_id")
    details: dict[str, Any] = {
        "required_permission": required_permission,
        "effective_role": effective_role,
        "path": path,
        "method": request.method.upper(),
        "suppressed_since_last": suppressed,
    }
    if reason:
        details["reason"] = reason
    if api_key_id:
        details["api_key_id"] = str(api_key_id)

    try:
        audit_log.append_audit_event(
            action="authz.denied",
            resource=path,
            actor=actor,
            level="warn",
            correlation_id=_correlation_id(request),
            workspace_id=audit_log.workspace_id_from_request(request),
            details=details,
        )
    except Exception as exc:
        _record_audit_failure(exc)
