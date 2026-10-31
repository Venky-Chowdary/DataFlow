"""Named audit events for workspace administration (QA enterprise #35).

QA (production 557f556c) saw admin changes to Settings, SSO, notification
channels, AI providers, key rotation and revocation only as generic
``http.patch`` / ``http.delete`` rows at ``info`` — even when the request was
refused with 403. Two causes:

* the handlers' own events were written without a workspace, so the
  workspace-scoped audit view (``X-Workspace-Id``) never showed them;
* a refused request (RBAC 403, a handler's 400/403/404) never reaches the
  handler's audit call, so only the catch-all ``http.<method>`` row remained.

Every admin route is named here. Handlers record success through
:func:`record_admin_change` (actor, target, outcome, workspace from the
request). When a request ends without such an event — refused, invalid, or a
handler that writes none — the timing middleware records the same named
action with the response's outcome via :func:`record_unhandled_admin_change`.
"""

from __future__ import annotations

import re
from typing import Any

from starlette.requests import Request

#: (method, route template, action). First match wins.
ADMIN_ROUTES: tuple[tuple[str, str, str], ...] = (
    ("PATCH", "/api/v1/workspace/settings", "workspace.settings.update"),
    ("PATCH", "/api/v1/workspace/sso/{sso_type}", "workspace.sso.update"),
    ("POST", "/api/v1/workspace/notifications", "workspace.notification.create"),
    ("POST", "/api/v1/workspace/notifications/{channel_id}/test", "workspace.notification.test"),
    (
        "POST",
        "/api/v1/workspace/notifications/{channel_id}/signing-secret/rotate",
        "workspace.notification.signing_secret.rotate",
    ),
    ("PATCH", "/api/v1/workspace/notifications/{channel_id}", "workspace.notification.update"),
    ("DELETE", "/api/v1/workspace/notifications/{channel_id}", "workspace.notification.delete"),
    ("PATCH", "/api/v1/workspace/ai-providers/{provider}", "workspace.ai_provider.update"),
    ("DELETE", "/api/v1/workspace/ai-providers/{provider}/key", "workspace.ai_provider.key_removed"),
    ("PATCH", "/api/v1/workspace/pilot-engine", "workspace.pilot_engine.update"),
    ("POST", "/api/v1/workspace/api-keys", "workspace.api_key.create"),
    ("DELETE", "/api/v1/workspace/api-keys/{key_id}", "workspace.api_key.revoke"),
    ("POST", "/api/v1/iam/api-keys/{key_id}/rotate", "iam.api_key.rotate"),
    ("POST", "/api/v1/iam/service-accounts", "iam.service_account.create"),
    ("DELETE", "/api/v1/iam/service-accounts/{key_id}", "iam.service_account.revoke"),
    ("POST", "/api/v1/iam/scim-token", "iam.scim.token.create"),
    ("PUT", "/api/v1/iam/scim/group-mappings", "iam.scim.group_mappings.update"),
    ("POST", "/api/v1/workspace/tenant", "workspace.tenant.create"),
    ("PATCH", "/api/v1/workspace/tenant/{tenant_id}", "workspace.tenant.update"),
    ("DELETE", "/api/v1/workspace/tenant/{tenant_id}", "workspace.tenant.delete"),
    ("POST", "/api/v1/workspace/jobs/retention/purge", "workspace.jobs.retention.purge"),
    ("POST", "/api/v1/workspace/pii-hash-key/rotate", "workspace.pii_hash_key.rotate"),
)

#: Registered descriptions (merged into ``audit_coverage.SECURITY_EVENTS``).
ADMIN_EVENTS: dict[str, str] = {
    action: f"Workspace administration: {action.split('.', 1)[1].replace('.', ' ').replace('_', ' ')}."
    for _m, _t, action in ADMIN_ROUTES
}
# Refusals of deployment-wide settings writes (services.platform_settings):
# one name for every route in the general / SSO / AI-provider groups, with the
# route's own action in the details.
ADMIN_EVENTS["platform_settings.write.denied"] = (
    "A write to deployment-wide settings (general, SSO, AI providers, Pilot engine) "
    "was refused because the caller is not a platform admin."
)


def _compile(template: str) -> re.Pattern[str]:
    parts = re.split(r"(\{[^/{}]+\})", template)
    return re.compile(
        "".join(
            f"(?P<{part[1:-1]}>[^/]+)" if part.startswith("{") and part.endswith("}") else re.escape(part)
            for part in parts
        )
    )


_COMPILED = tuple((method, _compile(template), action) for method, template, action in ADMIN_ROUTES)


def admin_action_for(method: str, path: str) -> tuple[str, dict[str, str]] | None:
    """The named action for this admin request, with its path parameters."""
    method = method.upper()
    for rule_method, pattern, action in _COMPILED:
        if rule_method != method:
            continue
        match = pattern.fullmatch(path.rstrip("/") or "/")
        if match:
            return action, match.groupdict()
    return None


def outcome_for_status(status: int) -> tuple[str, str | None, str]:
    """(outcome, reason_code, level) for a response status."""
    if status < 400:
        return "succeeded", None, "success"
    if status >= 500:
        return "errored", "server_error", "error"
    reason = {
        401: "unauthenticated",
        403: "forbidden",
        404: "not_found",
        409: "conflict",
        422: "invalid_request",
        429: "rate_limited",
    }.get(status, "invalid_request")
    return "failed", reason, "warn"


def _target(path: str, params: dict[str, str]) -> str:
    return next(iter(params.values()), "") or path


def record_admin_change(
    request: Request,
    action: str,
    *,
    target: str = "",
    details: dict[str, Any] | None = None,
    outcome: str = "succeeded",
    reason_code: str | None = None,
    status: int | None = None,
) -> None:
    """Write one named admin event scoped to the request's workspace."""
    from services.audit_coverage import current_request_audit, record_security_event

    payload = {"target": target or request.url.path, **(details or {})}
    record_security_event(
        action,
        outcome=outcome,
        request=request,
        resource=request.url.path,
        reason_code=reason_code,
        status=status,
        details=payload,
    )
    tracker = current_request_audit()
    if tracker is not None:
        tracker["admin_event"] = action


def record_unhandled_admin_change(request: Request, status: int) -> bool:
    """Name an admin request the handler did not record (refusals included).

    Returns True when an event was written, so the caller skips its generic
    ``http.<method>`` row.
    """
    from services.audit_coverage import current_request_audit

    tracker = current_request_audit()
    if tracker is not None and tracker.get("admin_event"):
        return True
    found = admin_action_for(request.method, request.url.path)
    if found is None:
        return False
    action, params = found
    outcome, reason, _level = outcome_for_status(status)
    details: dict[str, Any] = {"method": request.method.upper()}
    if status >= 400:
        details["refused"] = True
    record_admin_change(
        request,
        action,
        target=_target(request.url.path, params),
        details=details,
        outcome=outcome,
        reason_code=reason,
        status=status,
    )
    return True
