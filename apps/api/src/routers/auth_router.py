from __future__ import annotations

import os
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from ..services.auth_service import authenticate, create_token, public_user

try:
    from services.sso_state import generate_state, get_and_pop
except ImportError:  # pragma: no cover - tests with src on PYTHONPATH
    from src.services.sso_state import generate_state, get_and_pop

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=256)


def _web_origin() -> str:
    domain = os.getenv("DATAFLOW_WEB_DOMAIN", "http://localhost:5173").strip()
    if not domain.startswith("http"):
        domain = f"https://{domain}"
    return domain.rstrip("/")


@router.get("/sso/providers")
async def sso_providers():
    from services.integrations_store import list_sso_providers_public

    return {"providers": list_sso_providers_public()}


@router.get("/sso/{sso_type}/start")
async def sso_start(sso_type: str):
    from services.integrations_store import get_sso_config_raw, validate_sso_config

    check = validate_sso_config(sso_type)
    if not check["ready"]:
        raise HTTPException(status_code=400, detail=check["message"])

    cfg = get_sso_config_raw(sso_type)
    state = generate_state(sso_type)

    if sso_type in ("oidc", "azure_ad"):
        if sso_type == "azure_ad":
            tenant = cfg["tenant_id"]
            issuer = f"https://login.microsoftonline.com/{tenant}/v2.0"
            client_id = cfg["client_id"]
            redirect_uri = cfg["redirect_uri"]
        else:
            issuer = cfg["issuer"].rstrip("/")
            client_id = cfg["client_id"]
            redirect_uri = cfg["redirect_uri"]

        params = urlencode({
            "client_id": client_id,
            "response_type": "code",
            "scope": cfg.get("scopes") or "openid email profile",
            "redirect_uri": redirect_uri,
            "state": state,
        })
        authorize = f"{issuer}/authorize?{params}"
        return RedirectResponse(authorize, status_code=302)

    if sso_type == "saml":
        sso_url = cfg.get("sso_url", "")
        if sso_url:
            return RedirectResponse(sso_url, status_code=302)
        raise HTTPException(status_code=400, detail="SAML SSO URL not configured")

    raise HTTPException(status_code=400, detail="Unsupported SSO type")


@router.get("/sso/{sso_type}/callback")
async def sso_callback(sso_type: str, code: str = "", state: str = "", error: str = ""):
    if error:
        raise HTTPException(status_code=400, detail=f"SSO error: {error}")
    if not get_and_pop(state, sso_type):
        raise HTTPException(status_code=400, detail="Invalid SSO state")

    if sso_type not in ("oidc", "azure_ad") or not code:
        raise HTTPException(status_code=400, detail="Authorization code required")

    from services.integrations_store import get_sso_config_raw

    cfg = get_sso_config_raw(sso_type)
    if sso_type == "azure_ad":
        tenant = cfg["tenant_id"]
        token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
        client_id = cfg["client_id"]
        client_secret = cfg["client_secret"]
        redirect_uri = cfg["redirect_uri"]
    else:
        issuer = cfg["issuer"].rstrip("/")
        token_url = f"{issuer}/token"
        client_id = cfg["client_id"]
        client_secret = cfg["client_secret"]
        redirect_uri = cfg["redirect_uri"]

    try:
        import httpx

        token_resp = httpx.post(
            token_url,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=20.0,
        )
        token_resp.raise_for_status()
        tokens = token_resp.json()
        access_token = tokens.get("access_token", "")
        if not access_token:
            raise HTTPException(status_code=502, detail="No access token from identity provider")

        if sso_type == "azure_ad":
            userinfo_url = "https://graph.microsoft.com/oidc/userinfo"
        else:
            userinfo_url = f"{cfg['issuer'].rstrip('/')}/userinfo"

        user_resp = httpx.get(
            userinfo_url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15.0,
        )
        user_resp.raise_for_status()
        profile = user_resp.json()
        email = (
            profile.get("email")
            or profile.get("preferred_username")
            or profile.get("upn")
            or profile.get("sub")
        )
        if not email:
            raise HTTPException(status_code=502, detail="Identity provider did not return an email")

        token, expires_at = create_token(str(email))
        try:
            from services.audit_log import append_audit_event

            append_audit_event(
                action="auth.sso.login",
                resource=f"/auth/sso/{sso_type}/callback",
                actor=str(email),
                level="success",
                details={"provider": sso_type},
            )
        except Exception:
            pass

        redirect = f"{_web_origin()}/?sso_token={token}&expires_at={expires_at}&sso_email={email}"
        return RedirectResponse(redirect, status_code=302)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"SSO callback failed: {exc}") from exc


@router.post("/login")
async def login(body: LoginRequest):
    user = authenticate(body.email, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    token, expires_at = create_token(user["email"])
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action="auth.login",
            resource="/auth/login",
            actor=user["email"],
            level="success",
        )
    except Exception:
        pass
    return {
        "token": token,
        "expires_at": expires_at,
        "user": public_user(user),
    }
