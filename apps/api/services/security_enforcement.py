"""What each workspace security control actually enforces right now.

One function feeds the security posture, the compliance report and the
tenant save responses (``PATCH/POST /workspace/tenant``), so the badge an
admin reads after saving is the same answer the posture gives (QA round 11).
Wording is for workspace administrators: no deployment variable names; a
control the platform must switch on says "ask your platform administrator".
"""

from __future__ import annotations

from typing import Any


_IP_SCOPE = "every signed-in request and API key call to this workspace, on the shared app host and on a custom domain"
_IP_NOT_COVERED = (
    "public sign-in, health and discovery endpoints; platform administrators using the "
    "audited break-glass header"
)
_SESSION_SCOPE = "sign-in sessions of members of this workspace (strictest workspace wins)"
_SESSION_NOT_COVERED = "API keys; the absolute sign-in token lifetime set by the platform"


def _ip_block(tenant: Any, caller_ip: str | None) -> tuple[dict[str, Any], list[str]]:
    from services.client_ip import trusted_proxy_count_configured
    from services.ip_allowlist_policy import (
        HOPS_UNSET_MESSAGE,
        client_ip_source,
        client_ip_trusted,
        enforcement_on,
    )

    listed = bool(tenant and tenant.ip_allowlist)
    switched_on = enforcement_on()
    # Without a caller to judge, an unset hop count reads as not trustworthy.
    trusted = client_ip_trusted(caller_ip) if caller_ip is not None else trusted_proxy_count_configured()
    enforced = listed and switched_on and trusted
    missing: list[str] = []
    warnings: list[str] = []
    if not listed:
        status = "not_configured"
        missing.append("an IP allowlist for this workspace")
    elif not switched_on:
        status = "audit_only"
        missing.append("IP allowlist enforcement for this deployment")
        warnings.append(
            "The IP allowlist is saved but not enforced: IP allowlist enforcement is "
            "switched off for this deployment, so requests from outside the list are "
            "only logged. Ask your platform administrator to enable IP allowlist enforcement."
        )
    elif not trusted:
        status = "untrusted_client_ip"
        missing.append(HOPS_UNSET_MESSAGE)
        warnings.append(
            "The IP allowlist is saved but not enforced: the platform's proxy hop count is "
            "not configured, so the server cannot see real client addresses and requests "
            "from outside the list are only logged. Ask your platform administrator to set it."
        )
    else:
        status = "enforced"
    return {
        "enforced": enforced,
        "status": status,
        "scope": _IP_SCOPE,
        "missing": missing,
        "not_covered": _IP_NOT_COVERED,
        "client_ip_source": client_ip_source(),
    }, warnings


def _session_block(tenant: Any) -> tuple[dict[str, Any], list[str]]:
    on = bool(tenant and getattr(tenant, "enforce_session_timeout", False))
    hours = tenant.session_timeout_hours if tenant else None
    warnings: list[str] = []
    if not tenant:
        status = "not_configured"
    elif on:
        status = "enforced"
    else:
        status = "recorded_only"
        warnings.append(
            f"The {hours}h session timeout is recorded but not enforced. Turn on "
            "\"Enforce as idle timeout\" and save to sign idle members out."
        )
    return {
        "enforced": on,
        "status": status,
        "kind": "idle",
        "hours": hours,
        "scope": _SESSION_SCOPE,
        "not_covered": _SESSION_NOT_COVERED,
    }, warnings


def enforcement_status(tenant: Any, request: Any = None) -> dict[str, Any]:
    """The ``enforcement`` block: per-control status plus admin-facing warnings."""
    caller_ip: str | None = None
    if request is not None:
        from services.client_ip import client_ip_from_request

        caller_ip = client_ip_from_request(request)
    ip, ip_warnings = _ip_block(tenant, caller_ip)
    session, session_warnings = _session_block(tenant)
    warnings = ip_warnings + session_warnings
    from services.mfa_enforcement import posture as mfa_posture

    mfa = mfa_posture(tenant)
    if tenant and mfa.get("status") != "enforced" and getattr(tenant, "mfa_required", False):
        warnings.append(
            "Two-factor authentication is required but not enforced: sign-in is not "
            "required on this deployment. Ask your platform administrator to require sign-in."
        )
    out: dict[str, Any] = {"ip_allowlist": ip, "session_timeout": session, "mfa": mfa}
    if caller_ip is not None:
        from services.ip_allowlist_policy import proxy_hint

        out["caller_ip"] = caller_ip
        hint = proxy_hint(caller_ip) if ip["status"] in ("enforced", "audit_only") else ""
        if hint:
            warnings.append(hint.strip())
    out["warnings"] = warnings
    return out
