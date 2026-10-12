"""Audit coverage for sensitive API routes and repeated authorization denials."""

from __future__ import annotations

import logging
import re
import threading
import time
from hashlib import sha256
from collections import OrderedDict
from contextvars import ContextVar, Token
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from services import audit_log

logger = logging.getLogger(__name__)

SECURITY_EVENTS: dict[str, str] = {
    "auth.request.rejected": "An HTTP request was rejected during authentication.",
    "authz.tenant_bind.denied": "Authenticated access was denied by tenant binding.",
    "authz.denied": "An authenticated request lacked the required permission.",
    "auth.login": "A password authentication attempt completed.",
    "auth.logout": "An authenticated session was revoked.",
    "auth.sso.login": "An SSO authentication completed.",
    "system.security_settings.snapshot": "Effective security settings at API startup.",
}

_REQUEST_PRINCIPAL: ContextVar[dict[str, str] | None] = ContextVar(
    "audit_request_principal", default=None
)


def set_request_principal(principal: dict[str, str]) -> Token[dict[str, str] | None]:
    return _REQUEST_PRINCIPAL.set(dict(principal))


def current_request_principal() -> dict[str, str] | None:
    principal = _REQUEST_PRINCIPAL.get()
    return dict(principal) if principal is not None else None


def reset_request_principal(token: Token[dict[str, str] | None]) -> None:
    _REQUEST_PRINCIPAL.reset(token)


def session_label(jti: str) -> str:
    return "ses_" + sha256(("df-session:" + jti).encode()).hexdigest()[:24]


def request_principal(request: Request) -> dict[str, str]:
    state = request.state
    user = getattr(state, "user", None)
    user = user if isinstance(user, dict) else {}
    auth_kind = (
        getattr(state, "auth_kind", None)
        or user.get("auth_kind")
        or ("session" if user else "anonymous")
    )
    key_auth = bool(
        getattr(state, "api_key_auth", False)
        or getattr(state, "api_key_id", None)
        or user.get("api_key_id")
        or user.get("auth_kind") == "api_key"
    )
    if key_auth:
        kind = user.get("kind")
        actor_type = kind if kind in {"service_account", "scim", "api_key"} else "api_key"
        return {
            "actor_type": str(actor_type),
            "actor_id": str(
                getattr(state, "api_key_id", None) or user.get("api_key_id") or ""
            ),
            "auth_method": str(auth_kind or "api_key"),
            "created_by": str(getattr(state, "api_key_created_by", None) or ""),
        }
    if not user:
        return {
            "actor_type": "anonymous",
            "actor_id": "",
            "auth_method": str(auth_kind or "anonymous"),
        }
    actor_type = "mcp_oauth" if user.get("auth_kind") == "oauth" else "user"
    return {
        "actor_type": actor_type,
        "actor_id": str(user.get("email") or getattr(state, "user_email", None) or ""),
        "auth_method": str(auth_kind),
    }


def record_security_event(
    event: str,
    *,
    outcome: str,
    request: Request | None = None,
    resource: str = "",
    reason_code: str | None = None,
    status: int | None = None,
    duration_ms: float | None = None,
    session_id: str | None = None,
    actor: str | None = None,
    level: str | None = None,
    details: dict[str, Any] | None = None,
    principal: dict[str, str] | None = None,
) -> None:
    """Write one named, redacted audit event and its operational log line."""
    try:
        from services import ops_events, tracing

        if event not in SECURITY_EVENTS:
            logger.warning("Unknown security audit event %r", event)
        try:
            ops_events.validate_event(event, outcome)
        except Exception as exc:
            logger.warning(
                "Security audit event contract rejected (%s)",
                type(exc).__name__,
            )

        resolved_principal = principal
        if resolved_principal is None:
            resolved_principal = (
                request_principal(request)
                if request is not None
                else current_request_principal()
            )

        if actor is None:
            if request is not None:
                resolved_actor = audit_log.actor_from_request(request)
            elif resolved_principal:
                actor_type = str(resolved_principal.get("actor_type") or "anonymous")
                actor_id = str(resolved_principal.get("actor_id") or "")
                resolved_actor = (
                    f"{actor_type}:{actor_id}"
                    if actor_type in {"service_account", "scim", "api_key"}
                    else actor_id or "system"
                )
            else:
                resolved_actor = "system"
        else:
            resolved_actor = actor

        if request is not None:
            try:
                from services.workspace_access import authorized_workspace_id

                workspace_id = authorized_workspace_id(request)
            except Exception:
                workspace_id = ""
            correlation_id = getattr(request.state, "correlation_id", None)
            if correlation_id is None:
                correlation_id = tracing.get_correlation_id()
            route = request.scope.get("route")
            route_template = str(getattr(route, "path", "") or "")
        else:
            from services import logging_config

            workspace_var = getattr(logging_config, "_BOUND_FIELDS", {}).get(
                "workspace_id"
            )
            workspace_id = workspace_var.get() if workspace_var is not None else ""
            correlation_id = tracing.get_correlation_id()
            route_template = None
        trace_id = tracing.current_trace_id()

        event_details = dict(details or {})
        if status is not None:
            event_details["status"] = status
        if duration_ms is not None:
            event_details["duration_ms"] = duration_ms

        audit_level = level or {
            "succeeded": "success",
            "failed": "warn",
            "errored": "error",
        }.get(outcome, "info")
        try:
            audit_log.append_audit_event(
                action=event,
                resource=resource,
                actor=resolved_actor,
                level=audit_level,
                correlation_id=str(correlation_id) if correlation_id else None,
                workspace_id=workspace_id,
                details=event_details,
                principal=resolved_principal,
                outcome=outcome,
                reason_code=reason_code,
                session_id=session_id,
                trace_id=trace_id,
                route_template=route_template,
            )
        except Exception as exc:
            _record_audit_failure(exc)

        principal_fields = resolved_principal or {}
        try:
            ops_events.emit_event(
                logger,
                event,
                outcome=outcome,
                actor_type=str(principal_fields.get("actor_type") or ""),
                actor_id=str(principal_fields.get("actor_id") or ""),
                auth_method=str(principal_fields.get("auth_method") or ""),
                session_id=session_id,
                workspace_id=str(workspace_id or ""),
                route_template=str(route_template or ""),
                status=status,
                duration_ms=duration_ms,
                reason_code=reason_code,
            )
        except Exception as exc:
            _record_audit_failure(exc)
    except Exception as exc:
        try:
            _record_audit_failure(exc)
        except Exception:
            logger.error(
                "Security audit event failed (%s)",
                type(exc).__name__,
                exc_info=exc,
            )


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
    value = getattr(request.state, "correlation_id", None)
    if value is None:
        value = request.headers.get("X-Correlation-ID")
    return str(value) if value else None


def _auth_metadata(request: Request) -> tuple[str, str | None]:
    principal = request_principal(request)
    api_key_id = (
        principal["actor_id"]
        if principal["actor_type"] in {"service_account", "scim", "api_key"}
        else None
    )
    return principal["auth_method"], api_key_id


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

        principal = request_principal(request)
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
                principal=principal,
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

    record_security_event(
        "authz.denied",
        outcome="failed",
        request=request,
        resource=path,
        reason_code=reason or "missing_permission",
        status=403,
        actor=actor,
        details=details,
    )
