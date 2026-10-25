"""Self-serve signup routes — every endpoint gated by DATAWRAP_SIGNUP_ENABLED.

The whole router carries one dependency: when the flag is off each route
answers 404, so mounting this router unconditionally cannot change the
flag-off surface. Mounted at ``/api/v1`` (and at the root compat mount,
matching ``auth_router``).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from services.api_errors import ApiError
from services.signup_config import signup_enabled, turnstile_site_key


def _require_signup_enabled() -> None:
    """404 — not 401/403 — so the flag-off surface looks exactly like today."""
    if not signup_enabled():
        raise HTTPException(status_code=404, detail="Not Found")


def _client_ip(request: Request) -> str:
    try:
        from services.client_ip import client_ip_from_request

        return client_ip_from_request(request)
    except Exception:  # noqa: BLE001
        return request.client.host if request.client else ""


router = APIRouter(
    prefix="/auth",
    tags=["signup"],
    dependencies=[Depends(_require_signup_enabled)],
)


class SignupRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=1, max_length=1024)
    name: str = Field(default="", max_length=128)
    turnstile_token: str = Field(default="", max_length=4096)
    form_token: str = Field(default="", max_length=4096)
    device_id: str = Field(default="", max_length=128)
    honeypot: str = Field(default="", max_length=256)


class VerifyRequest(BaseModel):
    challenge_id: str = Field(min_length=1, max_length=128)
    code: str = Field(min_length=1, max_length=32)
    device_id: str = Field(default="", max_length=128)


class ResendRequest(BaseModel):
    challenge_id: str = Field(min_length=1, max_length=128)
    turnstile_token: str = Field(default="", max_length=4096)


class PasswordForgotRequest(BaseModel):
    email: str


@router.post("/password/forgot", status_code=202)
async def password_forgot(body: PasswordForgotRequest,
                          request: Request) -> dict[str, Any]:
    from services import signup_service
    return signup_service.request_password_reset(
        email=body.email, ip=_client_ip(request))


class PasswordResetRequest(BaseModel):
    challenge_id: str
    code: str
    new_password: str


@router.post("/password/reset")
async def password_reset(body: PasswordResetRequest,
                         request: Request) -> dict[str, Any]:
    from services import signup_service
    return signup_service.confirm_password_reset(
        challenge_id=body.challenge_id, code=body.code,
        new_password=body.new_password, ip=_client_ip(request))


class EmailChangeRequest(BaseModel):
    new_email: str = Field(min_length=3, max_length=320)


@router.post("/email/change", status_code=202)
async def email_change_request(body: EmailChangeRequest,
                               request: Request) -> dict[str, Any]:
    from services import signup_service
    user_email = getattr(request.state, "user_email", None)
    if not user_email:
        user = getattr(request.state, "user", None) or {}
        user_email = user.get("email")
    if not user_email:
        raise ApiError(401, "unauthorized", "sign in to change your email")
    return signup_service.request_email_change(
        user_email=user_email, new_email=body.new_email,
        ip=_client_ip(request))


class EmailChangeConfirm(BaseModel):
    challenge_id: str = Field(min_length=1, max_length=128)
    code: str = Field(min_length=1, max_length=16)


@router.post("/email/change/confirm")
async def email_change_confirm(body: EmailChangeConfirm,
                               request: Request) -> dict[str, Any]:
    from services import signup_service
    return signup_service.confirm_email_change(
        challenge_id=body.challenge_id, code=body.code,
        ip=_client_ip(request))


class OnboardingRequest(BaseModel):
    org_name: str = Field(default="", max_length=128)
    use_case: str = Field(default="", max_length=64)
    team_size: str = Field(default="", max_length=32)
    heard_from: str = Field(default="", max_length=128)


@router.post("/onboarding")
async def onboarding(body: OnboardingRequest,
                     request: Request) -> dict[str, Any]:
    from services import signup_service
    user_email = getattr(request.state, "user_email", None)
    if not user_email:
        user = getattr(request.state, "user", None) or {}
        user_email = user.get("email")
    if not user_email:
        raise ApiError(401, "unauthorized", "sign in to finish onboarding")
    return signup_service.complete_onboarding(
        user_email=user_email, org_name=body.org_name,
        profile={"use_case": body.use_case, "team_size": body.team_size,
                 "heard_from": body.heard_from},
        ip=_client_ip(request))


class SsoRouteRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)


@router.post("/sso/route")
async def sso_route(body: SsoRouteRequest) -> dict[str, Any]:
    """P4-6: which IdP (if any) claims this address's domain."""
    from services.sso_routing import route_for_email
    return route_for_email(body.email)


@router.get("/signup/config")
async def signup_config():
    """Public client config for the signup flow (no secrets)."""
    return {
        "enabled": True,
        "turnstile_site_key": turnstile_site_key(),
    }


@router.post("/signup", status_code=202)
async def signup(body: SignupRequest, request: Request):
    from services import signup_service

    return signup_service.request_signup(
        email=body.email,
        name=body.name,
        password=body.password,
        ip=_client_ip(request),
        device_id=body.device_id,
        ua=request.headers.get("user-agent", "")[:512],
        turnstile_token=body.turnstile_token,
        form_token=body.form_token,
        honeypot=body.honeypot,
    )


@router.get("/form-token")
async def form_token_issuer(purpose: str = "signup"):
    """Signed form-start token (P2-7). Mounted under /auth but it is the
    public form endpoint — signup and sales forms fetch one on mount."""
    from services.form_token import issue

    return {"form_token": issue(purpose[:32] or "signup")}


@router.post("/resend-webhook")
async def resend_webhook(request: Request):
    """Svix-signed Resend events (P2-10): email.bounced / email.complained
    feed the suppression list. Unsigned or forged payloads -> 401."""
    from services import denylist_store, webhook_verify
    from services.audit_coverage import record_security_event

    body = await request.body()
    if not webhook_verify.verify(body, request.headers):
        from services.api_errors import ApiError
        raise ApiError(401, "invalid_signature", "invalid webhook signature")
    import json
    try:
        event = json.loads(body)
    except ValueError:
        return {"ignored": True}
    etype = event.get("type", "")
    if etype not in ("email.bounced", "email.complained"):
        return {"ignored": True}
    data = event.get("data") or {}
    recipients = data.get("to") or ([data["email"]] if data.get("email") else [])
    suppressed = []
    for addr in recipients:
        try:
            denylist_store.deny_email_and_domain(
                addr, source="bounce", reason=f"resend_{etype}",
                added_by="resend-webhook")
            suppressed.append(addr)
        except Exception:  # noqa: BLE001 — suppression is best-effort per address
            pass
    record_security_event("email.event.received", outcome="succeeded",
                          details={"type": etype, "suppressed": len(suppressed)})
    return {"ok": True, "suppressed": len(suppressed)}


@router.post("/verify-email")
async def verify_email(body: VerifyRequest, request: Request, response: Response):
    from services import signup_service

    result = signup_service.verify_email(
        challenge_id=body.challenge_id,
        code=body.code,
        ip=_client_ip(request),
        device_id=body.device_id,
        ua=request.headers.get("user-agent", "")[:512],
    )
    session = result.pop("session")
    # __Host- session cookie — CSRF double-submit lands with P3-1.
    response.set_cookie(
        "__Host-dw_session",
        session["jti"],
        max_age=int(session["absolute_expires_at"] - session["created_at"]),
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
    )
    # Double-submit CSRF cookie — JS echoes it back as x-csrf-token on
    # mutating cookie-authed requests (enforced by AuthMiddleware).
    response.set_cookie(
        "dw_csrf",
        session["csrf"],
        max_age=int(session["absolute_expires_at"] - session["created_at"]),
        httponly=False,
        secure=True,
        samesite="strict",
        path="/",
    )
    return result


def _session_summary(doc: dict, *, current_jti: str = "") -> dict:
    return {
        "jti": doc["jti"],
        "device_id": doc.get("device_id", ""),
        "ip": doc.get("ip", ""),
        "ua": doc.get("ua", ""),
        "created_at": doc.get("created_at"),
        "last_seen_at": doc.get("last_seen_at"),
        "absolute_expires_at": doc.get("absolute_expires_at"),
        "current": doc["jti"] == current_jti,
    }


def _current_jti(request: Request) -> str:
    jti = getattr(request.state, "cookie_session_jti", "") or ""
    if not jti:
        auth = request.headers.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            from src.services import auth_service
            jti = auth_service.token_jti(auth[7:].strip()) or ""
    return jti


@router.get("/sessions")
async def list_sessions(request: Request):
    """P3-3: the caller's own sessions (cookie or bearer auth)."""
    email = getattr(request.state, "user_email", "") or ""
    if not email:
        from services.api_errors import ApiError
        raise ApiError(401, "auth_required", "authentication required")
    from services.auth_state import get_auth_state

    current = _current_jti(request)
    return {"sessions": [
        _session_summary(doc, current_jti=current)
        for doc in get_auth_state().list_sessions(email)
    ]}


@router.delete("/sessions/{jti}")
async def revoke_session(jti: str, request: Request):
    """P3-3: revoke one of the caller's own sessions."""
    email = getattr(request.state, "user_email", "") or ""
    if not email:
        from services.api_errors import ApiError
        raise ApiError(401, "auth_required", "authentication required")
    from services.auth_state import get_auth_state

    doc = get_auth_state().get_session(jti)
    if not doc or doc.get("user_id") != email:
        from services.api_errors import ApiError
        raise ApiError(404, "not_found", "no such session")
    get_auth_state().revoke_session(jti)
    from services.audit_coverage import record_security_event
    record_security_event("session.revoked", outcome="succeeded",
                          actor=email, session_id=jti)
    return {"ok": True}


@router.delete("/sessions")
async def revoke_other_sessions(request: Request):
    """P3-3: revoke every session except the one making the request."""
    email = getattr(request.state, "user_email", "") or ""
    if not email:
        from services.api_errors import ApiError
        raise ApiError(401, "auth_required", "authentication required")
    from services.auth_state import get_auth_state

    current = _current_jti(request)
    state = get_auth_state()
    revoked = 0
    for doc in state.list_sessions(email):
        if doc["jti"] != current:
            state.revoke_session(doc["jti"])
            revoked += 1
    if revoked:
        from services.audit_coverage import record_security_event
        record_security_event("session.revoked", outcome="succeeded",
                              actor=email,
                              details={"count": revoked, "kept": current})
    return {"ok": True, "revoked": revoked}


@router.post("/resend", status_code=202)
async def resend(body: ResendRequest, request: Request):
    from services import signup_service

    return signup_service.resend_code(challenge_id=body.challenge_id)
