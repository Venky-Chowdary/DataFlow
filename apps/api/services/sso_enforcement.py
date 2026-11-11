"""Per-workspace "require single sign-on" (D343).

The SSO provider itself (SAML / OIDC / Azure AD: issuer, client, scopes,
``email_domains``) stays deployment-wide and platform-admin only
(``/workspace/sso``). What a workspace admin controls is whether members of
THEIR workspace may still sign in with a password: ``tenant.sso_enforced``.

* **Who it binds.** Every member of a workspace whose tenant enforces SSO
  (strictest workspace wins, like the session timeout). Their password
  sign-in (``/auth/login``, the MFA step that follows it, and the session an
  accepted invitation would open) is refused with 403 ``sso_required`` and
  the SSO start URL; SSO sign-in is unchanged.
* **Break-glass.** A platform administrator may still sign in with a
  password; every such sign-in is audited as ``auth.sso_enforced.break_glass``.
* **Not covered.** API keys, service accounts and SCIM tokens are not
  interactive sign-ins and are not affected.
* **Turning it on** needs an enabled, complete SSO provider on the deployment
  and, unless the caller is a platform administrator, a session that itself
  signed in through SSO: proof the admin will not lock themselves out.
* **Fails closed.** If the policy cannot be read, a password sign-in is
  refused (503), never waved through.

Not in this change: an IdP group/claim -> workspace role mapping at SSO
sign-in. Roles still come from workspace membership and SCIM group mappings
(services.scim_service), which already carry the managed/adopted-membership,
role-ceiling and last-admin rules such a mapping must share.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException

logger = logging.getLogger(__name__)

SSO_REQUIRED = "sso_required"


def ready_providers() -> list[str]:
    """SSO types that are enabled and complete on this deployment."""
    from services.integrations_store import get_sso_configs, validate_sso_config

    out: list[str] = []
    for sso_type, cfg in (get_sso_configs() or {}).items():
        if (cfg or {}).get("enabled") and validate_sso_config(sso_type).get("ready"):
            out.append(sso_type)
    return out


def enforcing_workspaces(email: str) -> list[str]:
    """Workspaces of this person whose tenant requires SSO. Raises on a store error."""
    from services.team_store import list_memberships_for_user
    from services.tenant_store import tenants_for_workspaces

    ids = {str(row.get("workspace_id") or "") for row in list_memberships_for_user(email)}
    return sorted(t.workspace_id for t in tenants_for_workspaces(ids) if t.sso_enforced)


def _start_url(email: str) -> str | None:
    try:
        from services.sso_routing import route_for_email

        routed = route_for_email(email).get("start_url")
        if routed:
            return routed
        providers = ready_providers()
    except Exception:  # noqa: BLE001 — a hint only
        return None
    return f"/api/v1/auth/sso/{providers[0]}/start" if providers else None


def _is_platform_admin(email: str) -> bool:
    from services.tenant_scope import actor_is_unrestricted
    from src.services.auth_service import lookup_user

    return actor_is_unrestricted(lookup_user(email))


def refuse_password_sign_in(email: str, *, request: Any = None, resource: str = "/auth/login") -> None:
    """Raise when ``email`` must sign in through SSO; audit break-glass use."""
    from services.audit_coverage import record_security_event

    normalized = (email or "").strip().lower()
    try:
        workspaces = enforcing_workspaces(normalized)
        if not workspaces:
            return
        platform_admin = _is_platform_admin(normalized)
    except Exception:  # noqa: BLE001 — fail closed
        logger.warning("SSO enforcement lookup failed; refusing password sign-in", exc_info=True)
        raise HTTPException(
            status_code=503,
            detail="Sign-in policy could not be checked. Try again in a moment.",
        ) from None
    details = {"workspaces": workspaces[:20]}
    if platform_admin:
        record_security_event(
            "auth.sso_enforced.break_glass", outcome="succeeded", request=request, actor=normalized,
            resource=resource, reason_code="platform_admin_password", level="warn", details=details,
        )
        return
    record_security_event(
        "auth.login.sso_required", outcome="failed", request=request, actor=normalized,
        resource=resource, reason_code=SSO_REQUIRED, status=403, level="warn", details=details,
    )
    from services.api_errors import ApiError

    start_url = _start_url(normalized)
    message = (
        "Your workspace requires single sign-on. Use \"Sign in with SSO\" instead of a password."
        if start_url else
        "Your workspace requires single sign-on, but single sign-on is not available on this "
        "deployment right now. Contact your workspace administrator."
    )
    raise ApiError(403, SSO_REQUIRED, message, extra={"sso_start_url": start_url})


def assert_may_enforce_sso(request: Any) -> None:
    """Refuse turning the policy on when it would lock members (or the caller) out."""
    from services.mfa_enforcement import _request_credentials, session_amr
    from services.tenant_scope import actor_is_unrestricted

    if not ready_providers():
        raise HTTPException(status_code=409, detail=(
            "Single sign-on is not set up on this deployment, so requiring it would lock every "
            "member out. Ask your platform administrator to enable an SSO provider first."
        ))
    user = getattr(getattr(request, "state", None), "user", None)
    if actor_is_unrestricted(user if isinstance(user, dict) else None):
        return
    token, cookie_doc = _request_credentials(request)
    if "sso" in set(session_amr(token, cookie_doc)):
        return
    raise HTTPException(status_code=409, detail=(
        "Sign in with single sign-on first, then require it. Turning it on from a password "
        "session could lock you out if SSO does not work for your account."
    ))


def posture(tenant: Any) -> tuple[dict[str, Any], list[str]]:
    """The ``sso`` block of the enforcement status."""
    on = bool(tenant and getattr(tenant, "sso_enforced", False))
    try:
        providers = ready_providers()
    except Exception:  # noqa: BLE001
        providers = []
    warnings: list[str] = []
    if not on:
        status = "not_configured"
    elif providers:
        status = "enforced"
    else:
        status = "sso_unavailable"
        warnings.append(
            "Single sign-on is required but no SSO provider is enabled on this deployment, "
            "so members cannot sign in. Ask your platform administrator to enable it."
        )
    return {
        "enforced": on and bool(providers),
        "status": status,
        "providers": providers,
        "scope": "password sign-in of members of this workspace",
        "not_covered": "platform administrators (audited break-glass); API keys, service accounts and SCIM",
        "provider_config": "deployment-wide, managed by the platform administrator",
        "role_mapping": "not_supported: roles come from workspace membership and SCIM group mappings",
    }, warnings
