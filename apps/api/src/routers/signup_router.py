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
        from services.auth_rate_limit import client_ip_from_request

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
    )


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
    return result


@router.post("/resend", status_code=202)
async def resend(body: ResendRequest, request: Request):
    from services import signup_service

    return signup_service.resend_code(challenge_id=body.challenge_id)
