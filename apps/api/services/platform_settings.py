"""Deployment-wide settings: only a platform admin changes them (owner decision, round 10).

Three settings groups are each ONE document for the whole deployment, not one
per workspace:

- ``general``: the organization profile in ``services.workspace_store``
  (org name, default timezone, job retention, studio gates);
- ``sso``: the SAML / OIDC / Azure AD configuration in ``integrations_store``;
- ``ai_providers``: provider keys, models, the Ollama base URL and the Pilot
  engine choice, also in ``integrations_store``.

Before this module any workspace admin could rewrite them for every tenant. The
owner's decision is that only a *platform* admin may. Platform admin is the
account role (``services.tenant_scope.actor_is_unrestricted`` — the one
definition the tenant scope, the team routes and the key-access label use), so
an API key counts only when a platform admin minted it with no workspace; a key
a platform admin minted inside a workspace belongs to that workspace.

Workspace admins keep reading these settings (secrets stay masked): every read
says ``editable`` and, when false, why.
"""

from __future__ import annotations

from typing import Any, Callable

from fastapi import HTTPException, Request

#: Shown by the API and the web on every read-only settings surface.
PLATFORM_SETTINGS_REASON = "Only a platform admin can change deployment-wide settings"
#: Machine-readable code on the 403 and the audit event.
PLATFORM_SETTINGS_REASON_CODE = "platform_admin_required"
#: The named audit event every refused write records (registered in
#: ``services.admin_audit.ADMIN_EVENTS``).
DENIED_EVENT = "platform_settings.write.denied"

GROUPS = ("general", "sso", "ai_providers")


def may_change_platform_settings(request: Request) -> bool:
    """Whether the request principal may change deployment-wide settings."""
    from services.tenant_scope import actor_is_unrestricted

    return actor_is_unrestricted(getattr(request.state, "user", None))


def editability(request: Request) -> dict[str, Any]:
    """``editable`` / ``editable_reason`` for a settings read."""
    if may_change_platform_settings(request):
        return {"editable": True, "editable_reason": None}
    return {"editable": False, "editable_reason": PLATFORM_SETTINGS_REASON}


def _principal(request: Request) -> dict[str, Any]:
    try:
        from services.effective_principal import request_principal
        from services.effective_role import workspace_id_from_request_headers

        return request_principal(
            request, workspace_id_from_request_headers(request.headers)
        ).describe()
    except Exception:  # noqa: BLE001 - the refusal must not depend on the description
        return {}


def refuse_platform_settings_write(request: Request, *, group: str, operation: str) -> None:
    """Raise 403 (and audit it) unless the caller is a platform admin."""
    if may_change_platform_settings(request):
        return
    from services.admin_audit import admin_action_for, record_admin_change

    route = admin_action_for(request.method, request.url.path)
    params = dict(request.path_params or {})
    record_admin_change(
        request,
        DENIED_EVENT,
        target=next(iter(params.values()), "") or group,
        details={
            "group": group,
            "operation": operation,
            "route_action": route[0] if route else None,
            "method": request.method.upper(),
            "refused": True,
            "principal": _principal(request),
        },
        outcome="failed",
        reason_code=PLATFORM_SETTINGS_REASON_CODE,
        status=403,
    )
    raise HTTPException(status_code=403, detail=PLATFORM_SETTINGS_REASON)


def platform_settings_guard(group: str, operation: str) -> Callable[[Request], None]:
    """A route dependency: the one guard every deployment-wide settings write uses.

    A dependency, not a call inside the handler, so the refusal comes before
    body validation: a workspace admin learns "not yours to change", not which
    field of a payload it may not send anyway was malformed.
    """
    if group not in GROUPS:
        raise ValueError(f"unknown platform settings group {group!r}")

    def _guard(request: Request) -> None:
        refuse_platform_settings_write(request, group=group, operation=operation)

    _guard.__name__ = f"platform_settings_guard_{group}_{operation}"
    return _guard
