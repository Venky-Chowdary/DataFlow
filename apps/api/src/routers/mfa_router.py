"""Two-factor authentication routes (TOTP, RFC 6238) — QA S7, round 11.

Self-service, for the signed-in account (``account.self``):

* ``GET  /auth/mfa`` — status, and whether the workspace policy holds this session;
* ``POST /auth/mfa/totp/enroll`` — new secret + ``otpauth://`` URI (pending);
* ``POST /auth/mfa/totp/confirm`` — a valid code activates it; returns the ten
  recovery codes, once;
* ``POST /auth/mfa/verify`` — a code steps the current session up;
* ``POST /auth/mfa/recovery-codes`` — replace the recovery codes;
* ``POST /auth/mfa/totp/disable`` — remove the factor.

Re-enrolment, new recovery codes and disabling all need a fresh code or the
account password (``code`` / ``recovery_code`` / ``password`` in the body).

Sign-in: ``POST /auth/login`` answers ``202 {mfa_required, challenge}`` for an
enrolled account; ``POST /auth/login/mfa`` exchanges the challenge plus a code
for the session.

Break-glass: ``POST /iam/mfa/reset`` — a platform admin removes another
account's factor with a written reason; audited, and the account's sessions end.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from services import mfa
from services.admin_audit import record_admin_change
from services.audit_coverage import record_security_event, session_label

router = APIRouter(prefix="/auth", tags=["auth"])
admin_router = APIRouter(prefix="/iam", tags=["IAM"])
logger = logging.getLogger(__name__)


class CodeBody(BaseModel):
    code: str = Field(default="", max_length=64)
    recovery_code: str = Field(default="", max_length=64)


class ReauthBody(CodeBody):
    password: str = Field(default="", max_length=256)


class ChallengeBody(CodeBody):
    challenge: str = Field(min_length=16, max_length=2048)


class ResetBody(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    reason: str = Field(min_length=10, max_length=500)


# ── helpers ─────────────────────────────────────────────────────────────────


def _bearer(request: Request) -> str:
    auth = request.headers.get("authorization") or ""
    return auth[7:].strip() if auth.lower().startswith("bearer ") else ""


def _account(request: Request) -> str:
    """The signed-in person, refusing keys and tokens that are not sessions."""
    user = getattr(request.state, "user", None) or {}
    email = str(getattr(request.state, "user_email", "") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=401, detail="Sign in to manage two-factor authentication")
    if str(user.get("auth_kind") or "") in ("api_key", "oauth"):
        raise HTTPException(
            status_code=403,
            detail="Two-factor authentication belongs to a person's sign-in, not to an API key or token",
        )
    return email


def _password_account(email: str) -> None:
    """TOTP protects password sign-in. An SSO-only identity has none — unless the
    operator holds SSO sessions too (DATAFLOW_MFA_SSO_EXEMPT=0), when it must be
    able to enrol to step its session up."""
    from services.mfa_enforcement import sso_exempt
    from src.services.auth_service import lookup_user

    if lookup_user(email) is None and sso_exempt():
        raise HTTPException(
            status_code=400,
            detail=(
                "This account signs in through single sign-on; your identity provider "
                "handles two-factor authentication."
            ),
        )


def _client_ip(request: Request) -> str:
    from services.client_ip import client_ip_from_request

    return client_ip_from_request(request) or "unknown"


def _upgrade_session(request: Request, method: str) -> None:
    """Record the second factor on the session this request rides on."""
    request.state.mfa = None  # re-evaluate on the next read
    jti = getattr(request.state, "cookie_session_jti", "") or ""
    if jti:
        from services.auth_state import get_auth_state

        get_auth_state().add_amr(jti, method)
        return
    from services.auth_sessions import add_session_amr
    from src.services.auth_service import token_jti

    bearer_jti = token_jti(_bearer(request)) or ""
    if bearer_jti:
        add_session_amr(bearer_jti, method)


def _code_failure(request: Request, email: str, result: mfa.CodeResult, *, status: int = 400) -> HTTPException:
    """Audit a refused code and build the response (never echoes the code)."""
    if result.reason in ("bad_code", "replayed"):
        record_security_event(
            "auth.mfa.challenge.failed",
            outcome="failed",
            request=request,
            actor=email,
            resource=request.url.path,
            reason_code=result.reason,
            status=429 if result.newly_locked else status,
        )
    if result.newly_locked:
        record_security_event(
            "auth.mfa.locked",
            outcome="failed",
            request=request,
            actor=email,
            resource=request.url.path,
            reason_code="too_many_bad_codes",
            status=429,
            details={"locked_for_sec": result.retry_after, "max_failures": mfa.max_failures()},
        )
    if result.reason == "locked" or result.newly_locked:
        return HTTPException(
            status_code=429,
            detail="Too many incorrect codes. Two-factor sign-in is locked for this account; try again later.",
            headers={"Retry-After": str(max(1, int(result.retry_after or 60)))},
        )
    messages = {
        "bad_code": "That code is not correct. Check the time on your phone and try the newest code.",
        "replayed": "That code was already used. Wait for the next code from your authenticator app.",
        "not_enrolled": "Two-factor authentication is not set up for this account.",
        "no_pending": "Start setup again — the setup key expired or was never created.",
        "challenge_used": "This sign-in step was already completed. Sign in again.",
        "challenge_stale": "Two-factor settings changed for this account. Sign in again.",
    }
    return HTTPException(status_code=status, detail=messages.get(result.reason, "Code not accepted."))


def _reauth(request: Request, email: str, body: ReauthBody) -> str:
    """A fresh code or the password; returns which was used."""
    if body.code or body.recovery_code:
        result = mfa.verify(email, code=body.code, recovery_code=body.recovery_code)
        if not result.ok:
            raise _code_failure(request, email, result, status=403)
        if result.method == "recovery":
            _audit_recovery_used(request, email, result)
        return result.method
    if body.password:
        from services.auth_rate_limit import (
            check_login_rate_limit,
            record_login_failure,
            record_login_success,
        )
        from src.services.auth_service import authenticate

        ip = _client_ip(request)
        if not check_login_rate_limit(ip=ip, email=email).get("allowed"):
            raise HTTPException(status_code=429, detail="Too many attempts. Try again later.")
        if not authenticate(email, body.password):
            record_login_failure(ip=ip, email=email)
            raise HTTPException(status_code=403, detail="Password is incorrect")
        record_login_success(ip=ip, email=email)
        return "pwd"
    raise HTTPException(
        status_code=400,
        detail="Confirm with a current code from your authenticator app, a recovery code, or your password.",
    )


def _audit_recovery_used(request: Request, email: str, result: mfa.CodeResult) -> None:
    record_security_event(
        "auth.mfa.recovery_code.used",
        outcome="succeeded",
        request=request,
        actor=email,
        resource=request.url.path,
        level="warn",
        details={"recovery_codes_remaining": result.recovery_remaining},
    )


def _status_payload(request: Request, email: str) -> dict[str, Any]:
    from services.mfa_enforcement import identity_summary

    summary = identity_summary(request)
    return {**mfa.status(email), **{k: summary[k] for k in ("required", "hold", "session_verified", "via_sso", "sso_exempt")}}


# ── self-service ────────────────────────────────────────────────────────────


@router.get("/mfa")
async def mfa_status(request: Request):
    email = _account(request)
    return _status_payload(request, email)


@router.post("/mfa/totp/enroll")
async def mfa_enroll(body: ReauthBody, request: Request):
    email = _account(request)
    _password_account(email)
    reauth = ""
    if mfa.is_enrolled(email):
        reauth = _reauth(request, email, body)
    started = mfa.begin_enrollment(email)
    record_security_event(
        "auth.mfa.enrollment.started",
        outcome="started",
        request=request,
        actor=email,
        resource=request.url.path,
        details={"re_enroll": bool(reauth), "confirmed_with": reauth or None},
    )
    return {**started, "confirm_path": "/api/v1/auth/mfa/totp/confirm"}


@router.post("/mfa/totp/confirm")
async def mfa_confirm(body: CodeBody, request: Request):
    email = _account(request)
    _password_account(email)
    result = mfa.confirm_enrollment(email, body.code)
    if not result.ok:
        raise _code_failure(request, email, result)
    _upgrade_session(request, "otp")
    record_security_event(
        "auth.mfa.enrolled",
        outcome="succeeded",
        request=request,
        actor=email,
        resource=request.url.path,
        details={"method": "totp", "recovery_codes_issued": len(result.recovery_codes or [])},
    )
    return {
        "enrolled": True,
        "recovery_codes": result.recovery_codes,
        "recovery_codes_note": "Shown once. Store them somewhere safe; each code works one time.",
    }


@router.post("/mfa/verify")
async def mfa_step_up(body: CodeBody, request: Request):
    email = _account(request)
    result = mfa.verify(email, code=body.code, recovery_code=body.recovery_code)
    if not result.ok:
        raise _code_failure(request, email, result)
    if result.method == "recovery":
        _audit_recovery_used(request, email, result)
    _upgrade_session(request, "otp" if result.method == "totp" else "recovery")
    record_security_event(
        "auth.mfa.challenge.passed",
        outcome="succeeded",
        request=request,
        actor=email,
        resource=request.url.path,
        details={"method": result.method, "step_up": True},
    )
    return {"verified": True, "method": result.method, "recovery_codes_remaining": result.recovery_remaining}


@router.post("/mfa/recovery-codes")
async def mfa_regenerate_recovery(body: ReauthBody, request: Request):
    email = _account(request)
    if not mfa.is_enrolled(email):
        raise HTTPException(status_code=409, detail="Two-factor authentication is not set up for this account.")
    used = _reauth(request, email, body)
    codes = mfa.regenerate_recovery_codes(email)
    record_security_event(
        "auth.mfa.recovery_codes.regenerated",
        outcome="succeeded",
        request=request,
        actor=email,
        resource=request.url.path,
        details={"confirmed_with": used, "recovery_codes_issued": len(codes)},
    )
    return {"recovery_codes": codes, "recovery_codes_note": "Shown once. Your previous recovery codes no longer work."}


@router.post("/mfa/totp/disable")
async def mfa_disable(body: ReauthBody, request: Request):
    email = _account(request)
    if not mfa.is_enrolled(email):
        raise HTTPException(status_code=409, detail="Two-factor authentication is not set up for this account.")
    from services.mfa_enforcement import requiring_workspaces

    if requiring_workspaces(email):
        raise HTTPException(
            status_code=409,
            detail=(
                "A workspace you belong to requires two-factor authentication, so it cannot be "
                "turned off. Set it up again on a new device instead."
            ),
        )
    used = _reauth(request, email, body)
    mfa.remove_factor(email)
    record_security_event(
        "auth.mfa.removed",
        outcome="succeeded",
        request=request,
        actor=email,
        resource=request.url.path,
        level="warn",
        details={"confirmed_with": used, "by": "self"},
    )
    return {"enrolled": False}


# ── sign-in ─────────────────────────────────────────────────────────────────


def login_step_up(request: Request, email: str) -> JSONResponse | None:
    """Called by ``POST /auth/login`` after the password matched: an enrolled
    account gets a challenge (202) instead of a session."""
    minted = mfa.mint_challenge(email)
    if minted is None:
        return None
    challenge, expires = minted
    record_security_event(
        "auth.login",
        outcome="held",
        request=request,
        resource="/auth/login",
        actor=email,
        reason_code="mfa_required",
        status=202,
    )
    return JSONResponse(
        status_code=202,
        content={
            "mfa_required": True,
            "challenge": challenge,
            "challenge_expires_at": expires,
            "methods": ["totp", "recovery_code"],
            "verify_path": "/api/v1/auth/login/mfa",
        },
    )


@router.post("/login/mfa")
async def login_mfa(body: ChallengeBody, request: Request):
    from services.user_store import get_user as get_stored_user
    from src.services.auth_service import create_token, lookup_user, public_user, token_jti

    opened = mfa.open_challenge(body.challenge)
    if opened is None:
        raise HTTPException(status_code=401, detail="This sign-in step expired. Sign in again.")
    email = str(opened["e"])
    stored = get_stored_user(email)
    user = lookup_user(email)
    if user is None or (stored and stored.get("status") in ("disabled", "banned")):
        raise HTTPException(status_code=401, detail="This sign-in step expired. Sign in again.")
    result = mfa.verify(email, code=body.code, recovery_code=body.recovery_code, challenge=opened)
    if not result.ok:
        raise _code_failure(request, email, result, status=401)
    if result.method == "recovery":
        _audit_recovery_used(request, email, result)
    method = "otp" if result.method == "totp" else "recovery"
    try:
        token, expires_at = create_token(email, amr=["pwd", method])
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    jti = token_jti(token)
    record_security_event(
        "auth.mfa.challenge.passed",
        outcome="succeeded",
        request=request,
        actor=email,
        resource="/auth/login/mfa",
        session_id=session_label(jti) if jti else None,
        details={"method": result.method},
    )
    record_security_event(
        "auth.login",
        outcome="succeeded",
        request=request,
        resource="/auth/login/mfa",
        actor=email,
        session_id=session_label(jti) if jti else None,
        status=200,
    )
    return {
        "token": token,
        "expires_at": expires_at,
        "user": public_user(user),
        "must_change_password": bool(stored and stored.get("must_change_password")),
        "mfa_method": result.method,
        "recovery_codes_remaining": result.recovery_remaining,
    }


# ── break-glass ─────────────────────────────────────────────────────────────


@admin_router.post("/mfa/reset")
async def mfa_admin_reset(body: ResetBody, request: Request):
    """Platform admin removes another account's second factor (lost device and
    lost recovery codes). Needs a written reason; audited; the account's
    sessions are revoked so the next sign-in starts clean. See
    docs/MFA_ENFORCEMENT_DESIGN.md → Break-glass for the CLI equivalent."""
    from services.tenant_scope import actor_is_unrestricted
    from services.user_store import normalize_email
    from src.services.auth_service import revoke_sessions_for_email

    actor = _account(request)
    target = normalize_email(body.email)
    reason = " ".join(body.reason.split())
    refusal = None
    if not actor_is_unrestricted(getattr(request.state, "user", None)):
        refusal = (403, "platform_admin_required", "Only a platform admin can reset another account's two-factor authentication.")
    elif target == actor:
        refusal = (400, "self_reset", "Use Settings → Security to change your own two-factor authentication.")
    elif len(reason) < 10:
        refusal = (400, "reason_required", "Give a reason of at least 10 characters; it is recorded in the audit log.")
    if refusal:
        status, code, message = refusal
        record_admin_change(
            request, "auth.mfa.reset", target=target, outcome="failed",
            reason_code=code, status=status, details={"refused": True},
        )
        raise HTTPException(status_code=status, detail=message)
    removed = mfa.remove_factor(target, clear_lockout=True)
    revoked = revoke_sessions_for_email(target)
    try:
        from services.auth_state import get_auth_state

        revoked += get_auth_state().revoke_all_for_user(target)
    except Exception:  # noqa: BLE001 — bearer sessions are already revoked
        logger.warning("cookie session revoke after mfa reset failed", exc_info=True)
    record_admin_change(
        request, "auth.mfa.reset", target=target, status=200,
        details={"reason": reason, "had_factor": removed, "sessions_revoked": revoked},
    )
    return {"email": target, "reset": True, "had_factor": removed, "sessions_revoked": revoked}
