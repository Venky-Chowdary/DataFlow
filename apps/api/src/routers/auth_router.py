from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from services.platform_config import web_url
from pydantic import BaseModel, Field

from ..services.auth_service import (
    auth_bootstrap_status,
    authenticate,
    create_token,
    public_user,
)

try:
    from services.sso_state import generate_state, get_and_pop
except ImportError:  # pragma: no cover - tests with src on PYTHONPATH
    from src.services.sso_state import generate_state, get_and_pop

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=256)


def _web_origin() -> str:
    explicit = web_url()
    if explicit:
        return explicit.rstrip("/")
    domain = os.getenv("DATAFLOW_WEB_DOMAIN", "http://localhost:5173").strip()
    if not domain.startswith("http"):
        domain = f"https://{domain}"
    return domain.rstrip("/")


def _saml_base_url(request: Request) -> str:
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.url.hostname)
    port = request.headers.get("x-forwarded-port", str(request.url.port or (443 if scheme == "https" else 80)))
    port_str = f":{port}" if port not in ("443", "80") and (scheme != "https" or port != "443") else ""
    return f"{scheme}://{host}{port_str}"


def _saml_sp_entity_id(request: Request) -> str:
    explicit = os.getenv("DATAFLOW_SAML_SP_ENTITY_ID", "").strip()
    if explicit:
        return explicit
    return f"{_saml_base_url(request)}/api/v1/auth/sso/saml/metadata"


def _saml_acs_url(request: Request) -> str:
    explicit = os.getenv("DATAFLOW_SAML_ACS_URL", "").strip()
    if explicit:
        return explicit
    return f"{_saml_base_url(request)}/api/v1/auth/sso/saml/callback"


def _saml_settings_dict(request: Request, cfg: dict[str, str]) -> dict[str, Any]:
    entity_id = cfg.get("entity_id", "").strip()
    sso_url = cfg.get("sso_url", "").strip()
    x509_cert = cfg.get("x509_cert", "").strip()
    return {
        "strict": True,
        "debug": False,
        "sp": {
            "entityId": _saml_sp_entity_id(request),
            "assertionConsumerService": {
                "url": _saml_acs_url(request),
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
            },
            "NameIDFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress",
            "x509cert": "",
            "privateKey": "",
        },
        "idp": {
            "entityId": entity_id,
            "singleSignOnService": {
                "url": sso_url,
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
            },
            "x509cert": x509_cert,
        },
        "security": {
            "nameIdEncrypted": False,
            "authnRequestsSigned": False,
            "logoutRequestSigned": False,
            "logoutResponseSigned": False,
            "signMetadata": False,
            "wantAssertionsSigned": True,
            "wantAssertionsEncrypted": False,
            "wantNameId": True,
            "wantNameIdEncrypted": False,
            "requestedAuthnContext": True,
            "requestedAuthnContextComparison": "exact",
            "wantXMLValidation": True,
            "relaxDestinationValidation": True,
            "destinationStrictlyMatches": False,
            "rejectUnsolicitedResponsesWithInResponseTo": False,
            "wantMessagesSigned": False,
        },
    }


def _saml_request_dict(request: Request, post_data: dict[str, str] | None = None) -> dict[str, Any]:
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.url.hostname)
    port = int(request.headers.get("x-forwarded-port", request.url.port or (443 if scheme == "https" else 80)))
    return {
        "https": "on" if scheme == "https" else "off",
        "http_host": host,
        "server_port": port,
        "script_name": "/api/v1/auth/sso/saml",
        "get_data": {},
        "post_data": post_data or {},
        "lowercase_urlencoding": False,
        "request_uri": str(request.url),
    }


@router.get("/sso/providers")
async def sso_providers():
    from services.integrations_store import list_sso_providers_public

    return {"providers": list_sso_providers_public()}


@router.get("/sso/{sso_type}/start")
async def sso_start(sso_type: str, request: Request):
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
        try:
            from onelogin.saml2.auth import OneLogin_Saml2_Auth
        except Exception as exc:
            raise HTTPException(status_code=501, detail="SAML support is not installed") from exc
        saml_settings = _saml_settings_dict(request, cfg)
        req = _saml_request_dict(request)
        auth = OneLogin_Saml2_Auth(req, saml_settings)
        return RedirectResponse(auth.login(return_to=state), status_code=302)

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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

        redirect = f"{_web_origin()}/?sso_token={token}&expires_at={expires_at}&sso_email={email}"
        return RedirectResponse(redirect, status_code=302)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"SSO callback failed: {exc}") from exc


@router.post("/sso/{sso_type}/callback")
async def sso_post_callback(sso_type: str, request: Request):
    if sso_type != "saml":
        raise HTTPException(status_code=405, detail="POST callback is only supported for SAML")

    try:
        from onelogin.saml2.auth import OneLogin_Saml2_Auth
    except Exception as exc:
        raise HTTPException(status_code=501, detail="SAML support is not installed") from exc

    from services.integrations_store import get_sso_config_raw

    cfg = get_sso_config_raw(sso_type)
    form = await request.form()
    saml_response = str(form.get("SAMLResponse", ""))
    relay_state = str(form.get("RelayState", ""))
    if not saml_response:
        raise HTTPException(status_code=400, detail="SAMLResponse is required")
    if relay_state and not get_and_pop(relay_state, sso_type):
        raise HTTPException(status_code=400, detail="Invalid SAML RelayState")

    req = _saml_request_dict(request, post_data={"SAMLResponse": saml_response})
    saml_settings = _saml_settings_dict(request, cfg)
    auth = OneLogin_Saml2_Auth(req, saml_settings)
    auth.process_response()
    errors = auth.get_errors()
    if errors:
        raise HTTPException(status_code=401, detail=f"SAML response invalid: {', '.join(errors)}")
    if not auth.is_authenticated():
        raise HTTPException(status_code=401, detail="SAML authentication failed")

    name_id = auth.get_nameid()
    email = name_id
    if not email or "@" not in email:
        email_attr = cfg.get("email_attribute", "email")
        attributes = auth.get_attributes()
        email = (
            (attributes.get(email_attr, [""])[0] if isinstance(attributes.get(email_attr), list) else attributes.get(email_attr, ""))
            or name_id
        )
    if not email or "@" not in email:
        raise HTTPException(status_code=502, detail="SAML identity did not return an email")

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
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    redirect = f"{_web_origin()}/?sso_token={token}&expires_at={expires_at}&sso_email={email}"
    return RedirectResponse(redirect, status_code=302)


@router.get("/bootstrap")
async def auth_bootstrap():
    """Operator-safe auth diagnostics (no secrets) — which emails are configured."""
    return auth_bootstrap_status()


@router.post("/login")
async def login(body: LoginRequest):
    status = auth_bootstrap_status()
    if status["user_count"] == 0:
        raise HTTPException(
            status_code=503,
            detail=(
                "No workspace users configured. Set DATAFLOW_ADMIN_EMAIL and "
                "DATAFLOW_ADMIN_PASSWORD on the API service, then redeploy."
            ),
        )
    try:
        user = authenticate(body.email, body.password)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not user:
        raise HTTPException(
            status_code=401,
            detail=(
                "Invalid email or password. If your password contains `$`, re-set "
                "DATAFLOW_ADMIN_PASSWORD in Railway (escape as `$$` or wrap in quotes) "
                "and redeploy the API."
            ),
        )
    try:
        token, expires_at = create_token(user["email"])
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action="auth.login",
            resource="/auth/login",
            actor=user["email"],
            level="success",
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    return {
        "token": token,
        "expires_at": expires_at,
        "user": public_user(user),
    }
