"""The workspace "require MFA" policy, enforced on sign-in sessions (QA S7).

A tenant (the workspace's enterprise profile) records ``mfa_required``. Until
round 11 that was policy memory only. Now, when any workspace a person belongs
to — or the workspace a request names — requires MFA, a password session that
has not passed a second factor is *held*: it reaches only the enrolment and
verification routes (plus ``/auth/me``, change-password and logout) until the
person enrols (or, if already enrolled, enters a code). Every other route
answers ``403 {reason: mfa_enrollment_required | mfa_verification_required}``.

Not affected, by design:

* SSO sessions (OIDC / SAML / Azure AD): the identity provider performs MFA.
  ``DATAFLOW_MFA_SSO_EXEMPT=0`` holds them too; the posture says which.
* API keys, service accounts, SCIM tokens and MCP OAuth tokens: not
  interactive sessions.
* Deployments with authentication off (``DATAFLOW_REQUIRE_AUTH=0``): there is
  no session to hold, and the posture reports MFA as not enforced.

A session records how it was authenticated (``amr`` on the session row:
``pwd``, ``otp``, ``recovery``, ``sso``). Rows written before this shipped
carry none and count as password-only — fail closed.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

from services.brand_env import getenv_brand_bool

logger = logging.getLogger(__name__)

SECOND_FACTORS = frozenset({"otp", "recovery"})
HOLD_ENROLL = "mfa_enrollment_required"
HOLD_VERIFY = "mfa_verification_required"

_ALLOWED_EXACT = frozenset(
    {
        "/api/v1/auth/me",
        "/auth/me",
        "/api/v1/auth/mfa",
        "/auth/mfa",
        "/api/v1/auth/change-password",
        "/auth/change-password",
        "/api/v1/auth/logout",
        "/auth/logout",
    }
)
_ALLOWED_PREFIXES = ("/api/v1/auth/mfa/", "/auth/mfa/")

_COPY = {
    HOLD_ENROLL: (
        "Your workspace requires two-factor authentication. Set up an authenticator "
        "app to continue — until then this session can only reach two-factor setup."
    ),
    HOLD_VERIFY: (
        "Your workspace requires two-factor authentication. Enter a code from your "
        "authenticator app to continue."
    ),
}


def sso_exempt() -> bool:
    """SSO sessions skip the workspace MFA gate (default on: the IdP does MFA)."""
    return getenv_brand_bool("MFA_SSO_EXEMPT", True)


def _tenant_requires(workspace_id: str) -> bool:
    if not workspace_id:
        return False
    from services.tenant_store import get_tenant_for_workspace

    tenant = get_tenant_for_workspace(workspace_id)
    return bool(tenant and getattr(tenant, "mfa_required", False))


def requiring_workspaces(email: str, named_workspace_id: str = "") -> list[str]:
    """Workspaces whose policy requires MFA of this person on this request:
    every workspace they are a member of, plus the one the request names (a
    platform admin acting in a workspace is held to that workspace's policy)."""
    from services.team_store import list_memberships_for_user

    ids = {str(row.get("workspace_id") or "") for row in list_memberships_for_user(email)}
    if named_workspace_id:
        ids.add(named_workspace_id)
    return sorted(ws for ws in ids if ws and _tenant_requires(ws))


def _is_interactive(user: Any) -> bool:
    kind = str((user or {}).get("auth_kind") or "") if isinstance(user, dict) else ""
    return kind not in ("api_key", "oauth")


def session_amr(token: str = "", cookie_doc: dict | None = None) -> list[str]:
    if cookie_doc is not None:
        return [str(m) for m in cookie_doc.get("amr") or []]
    from services.auth_sessions import session_amr as bearer_amr
    from src.services.auth_service import token_jti

    jti = token_jti(token or "") or ""
    return bearer_amr(jti) if jti else []


def _request_credentials(request: Request) -> tuple[str, dict | None]:
    """The session this request rides on: its Bearer token, else its cookie session."""
    auth = request.headers.get("authorization") or ""
    token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    if token:
        return token, None
    jti = getattr(request.state, "cookie_session_jti", "") or ""
    if not jti:
        return "", None
    from services.auth_state import get_auth_state

    return "", get_auth_state().get_session(jti) or {}


def _named_workspace(request: Request) -> str:
    return (request.headers.get("x-workspace-id") or "").strip()


def evaluate(request: Request, *, token: str = "", cookie_doc: dict | None = None) -> dict[str, Any]:
    """This request's MFA standing; cached on ``request.state.mfa``."""
    cached = getattr(request.state, "mfa", None)
    if isinstance(cached, dict):
        return cached
    from src.services.auth_service import auth_required

    user = getattr(request.state, "user", None)
    email = str(getattr(request.state, "user_email", "") or "").strip().lower()
    out: dict[str, Any] = {"applies": False, "required": False, "hold": None,
                           "session_verified": False, "via_sso": False, "workspaces": []}
    if not email or not auth_required() or not _is_interactive(user):
        request.state.mfa = out
        return out
    out["applies"] = True
    try:
        if not token and cookie_doc is None:
            token, cookie_doc = _request_credentials(request)
        amr = session_amr(token, cookie_doc)
        out["via_sso"] = "sso" in amr
        out["session_verified"] = bool(SECOND_FACTORS & set(amr))
        if out["via_sso"] and sso_exempt():
            request.state.mfa = out
            return out
        workspaces = requiring_workspaces(email, _named_workspace(request))
        out["workspaces"] = workspaces
        out["required"] = bool(workspaces)
        if workspaces and not out["session_verified"]:
            from services.mfa import is_enrolled

            out["hold"] = HOLD_VERIFY if is_enrolled(email) else HOLD_ENROLL
    except Exception:  # noqa: BLE001 — a broken lookup must hold, never wave through
        logger.error("mfa policy evaluation failed; holding the session", exc_info=True)
        out["required"] = True
        out["hold"] = HOLD_ENROLL
    request.state.mfa = out
    return out


def path_allowed_while_held(path: str) -> bool:
    return path in _ALLOWED_EXACT or path.startswith(_ALLOWED_PREFIXES)


def hold_response(request: Request, state: dict[str, Any]) -> JSONResponse:
    reason = str(state.get("hold") or HOLD_ENROLL)
    _audit_hold(request, reason)
    return JSONResponse(
        status_code=403,
        content={
            "detail": _COPY.get(reason, _COPY[HOLD_ENROLL]),
            "reason": reason,
            "mfa": {"required": True, "hold": reason, "enroll_path": "/api/v1/auth/mfa"},
        },
    )


def _audit_hold(request: Request, reason: str) -> None:
    try:
        from services.audit_coverage import _suppressed_since_last, record_security_event

        actor = str(getattr(request.state, "user_email", "") or "unknown")
        suppressed = _suppressed_since_last(actor, "auth.mfa.enforcement.held", request.url.path)
        if suppressed is None:
            return
        record_security_event(
            "auth.mfa.enforcement.held",
            outcome="held",
            request=request,
            resource=request.url.path,
            reason_code=reason,
            status=403,
            details={"suppressed_since_last": suppressed},
        )
    except Exception:  # noqa: BLE001 — auditing must not change the decision
        logger.warning("mfa hold audit failed", exc_info=True)


def gate(request: Request, *, token: str = "", cookie_doc: dict | None = None) -> JSONResponse | None:
    """403 for a held session on anything but the enrolment routes."""
    state = evaluate(request, token=token, cookie_doc=cookie_doc)
    if not state.get("hold") or path_allowed_while_held(request.url.path):
        return None
    return hold_response(request, state)


def detach_if_held(request: Request, *, token: str = "") -> None:
    """On a public route a held session is treated as anonymous, so a public
    surface that honours an optional identity (MCP's JSON-RPC endpoint) cannot
    be used to get around the hold."""
    state = evaluate(request, token=token)
    if state.get("hold"):
        request.state.user = None
        request.state.user_email = None


def identity_summary(request: Request) -> dict[str, Any]:
    """What ``/auth/me`` reports so the client can route a held session."""
    state = evaluate(request)
    email = str(getattr(request.state, "user_email", "") or "")
    enrolled = False
    if state.get("applies") and email:
        try:
            from services.mfa import is_enrolled

            enrolled = is_enrolled(email)
        except Exception:  # noqa: BLE001
            logger.warning("mfa status read failed", exc_info=True)
    return {
        "enrolled": enrolled,
        "required": bool(state.get("required")),
        "hold": state.get("hold"),
        "session_verified": bool(state.get("session_verified")),
        "via_sso": bool(state.get("via_sso")),
        "sso_exempt": sso_exempt(),
    }


def assert_may_require_mfa(request: Request) -> None:
    """Refuse turning the policy on from a session that would be held by it.

    The admin who flips the switch would otherwise lose the page they are on.
    Enrol first (the Two-factor card), then require it. API keys and exempt SSO
    sessions are not held, so they may turn it on directly.
    """
    from src.services.auth_service import auth_required

    user = getattr(request.state, "user", None)
    if not auth_required() or not _is_interactive(user):
        return
    token, cookie_doc = _request_credentials(request)
    amr = set(session_amr(token, cookie_doc))
    if ("sso" in amr and sso_exempt()) or SECOND_FACTORS & amr:
        return
    raise HTTPException(
        status_code=409,
        detail={
            "reason": "mfa_enroll_self_first",
            "message": (
                "Set up two-factor authentication for your own account first (Settings → "
                "Security), then require it for the workspace. Requiring it now would "
                "hold your own session at the two-factor setup screen."
            ),
        },
    )


def posture(tenant: Any) -> dict[str, Any]:
    """The MFA block of the security posture — enforced only when it is."""
    from src.services.auth_service import auth_required

    required = bool(tenant and getattr(tenant, "mfa_required", False))
    auth_on = bool(auth_required())
    missing = [name for name, ok in (
        ("tenant.mfa_required", required),
        ("sign-in required on this deployment (ask your platform administrator)", auth_on),
    ) if not ok]
    block: dict[str, Any] = {
        "enforced": required and auth_on,
        "status": "enforced" if required and auth_on else "available",
        "factor": "totp_rfc6238",
        "scope": "every password sign-in session of members of this workspace, and of anyone acting in it",
        "held_session_reaches": "two-factor setup and verification, /auth/me, change-password, logout",
        "sso_exempt": sso_exempt(),
        "sso_note": (
            "SSO sessions are exempt: the identity provider performs MFA (your platform administrator can hold them too)"
            if sso_exempt() else "SSO sessions are held like password sessions"
        ),
        "not_covered": "API keys, service accounts, SCIM tokens and MCP OAuth tokens (not interactive)",
        "requires": ["tenant.mfa_required", "sign-in required on this deployment"],
        "missing": missing,
    }
    workspace_id = str(getattr(tenant, "workspace_id", "") or "") if tenant else ""
    if workspace_id:
        try:
            from services.mfa_store import emails_with_active_factor
            from services.team_store import list_workspace_members

            emails = [str(m.get("email") or "") for m in list_workspace_members(workspace_id)]
            block["members_total"] = len(emails)
            block["members_enrolled"] = len(emails_with_active_factor(emails))
        except Exception:  # noqa: BLE001 — counts are informational
            logger.warning("mfa posture member counts unavailable", exc_info=True)
    return block


__all__ = [
    "HOLD_ENROLL",
    "HOLD_VERIFY",
    "SECOND_FACTORS",
    "assert_may_require_mfa",
    "detach_if_held",
    "evaluate",
    "gate",
    "identity_summary",
    "path_allowed_while_held",
    "posture",
    "requiring_workspaces",
    "session_amr",
    "sso_exempt",
]
