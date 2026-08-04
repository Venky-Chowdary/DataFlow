from __future__ import annotations

import base64
import hashlib
import logging
import os
from services.brand_env import getenv_brand
import secrets
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from services.platform_config import is_production, web_url
from pydantic import BaseModel, Field

from ..services.auth_service import (
    auth_bootstrap_status,
    authenticate,
    create_token,
    lookup_user,
    public_user,
)

try:
    from services.sso_state import generate_state, get_and_pop, get_state
except ImportError:  # pragma: no cover - tests with src on PYTHONPATH
    from src.services.sso_state import generate_state, get_and_pop, get_state

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=256)


def _web_origin() -> str:
    explicit = web_url()
    if explicit:
        return explicit.rstrip("/")
    domain = getenv_brand("WEB_DOMAIN", "http://localhost:5173").strip()
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
    explicit = getenv_brand("SAML_SP_ENTITY_ID", "").strip()
    if explicit:
        return explicit
    return f"{_saml_base_url(request)}/api/v1/auth/sso/saml/metadata"


def _saml_acs_url(request: Request) -> str:
    explicit = getenv_brand("SAML_ACS_URL", "").strip()
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


def _pkce_pair() -> tuple[str, str]:
    """Return (code_verifier, code_challenge) for PKCE/S256."""
    verifier = secrets.token_urlsafe(48)
    challenge = (
        base64.urlsafe_b64encode(
            hashlib.sha256(verifier.encode("ascii")).digest()
        )
        .decode("ascii")
        .rstrip("=")
    )
    return verifier, challenge


def _sso_allowed_domains() -> set[str]:
    """Domains that may auto-provision via SSO when no explicit user exists."""
    raw = getenv_brand("SSO_ALLOWED_DOMAINS", "").strip()
    if not raw:
        return set()
    return {d.strip().lower().lstrip("@") for d in raw.split(",") if d.strip()}


def _sso_auto_provision() -> bool:
    return getenv_brand("SSO_AUTO_PROVISION", "0").lower() in ("1", "true", "yes")


def _is_sso_email_allowed(email: str) -> bool:
    """Fail closed: require pre-registered user or allowed domain."""
    normalized = email.strip().lower()
    if lookup_user(normalized):
        return True
    domain = normalized.split("@")[-1] if "@" in normalized else ""
    if domain and domain in _sso_allowed_domains():
        return True
    return False


def _require_sso_authorization(email: str) -> None:
    """Raise a clear 403 if the IdP user is not authorized for this workspace."""
    if _sso_auto_provision():
        # Even auto-provision should respect allowed-domain gating when configured.
        if _sso_allowed_domains() and email.split("@")[-1].lower() not in _sso_allowed_domains():
            raise HTTPException(
                status_code=403,
                detail="SSO email domain is not in DATAFLOW_SSO_ALLOWED_DOMAINS",
            )
        return
    if not _is_sso_email_allowed(email):
        raise HTTPException(
            status_code=403,
            detail="SSO user is not authorized for this workspace",
        )


def _redirect_with_token(email: str, expires_at: int) -> RedirectResponse:
    """Return the token in the URL fragment so it is not sent to the server or logged."""
    token, _ = create_token(str(email))
    params = urlencode(
        {
            "sso_token": token,
            "expires_at": str(expires_at),
            "sso_email": email,
        }
    )
    return RedirectResponse(f"{_web_origin()}/#{params}", status_code=302)


_JWKS_CACHE: dict[str, Any] = {}


def _oidc_discovery_url(issuer: str) -> str:
    """Return the OIDC discovery document URL for an issuer."""
    base = issuer.rstrip("/")
    return f"{base}/.well-known/openid-configuration"


def _fetch_oidc_jwks(issuer: str) -> list[dict[str, Any]]:
    """Fetch and cache the JWKS keys for an OIDC issuer."""
    import httpx

    cached = _JWKS_CACHE.get(issuer)
    if cached is not None:
        return cached

    try:
        discovery = httpx.get(_oidc_discovery_url(issuer), timeout=10.0)
        discovery.raise_for_status()
        jwks_uri = discovery.json().get("jwks_uri", "")
        if not jwks_uri:
            raise RuntimeError("OIDC discovery did not return a jwks_uri")
        jwks_resp = httpx.get(jwks_uri, timeout=10.0)
        jwks_resp.raise_for_status()
        keys = jwks_resp.json().get("keys", [])
        _JWKS_CACHE[issuer] = keys
        return keys
    except Exception as exc:
        logging.getLogger(__name__).warning("Unable to fetch OIDC JWKS for %s: %s", issuer, exc)
        return []


def _id_token_email(id_token: str, state_info: dict[str, Any]) -> str:
    """Validate the OIDC id_token signature and claims, returning the email.

    Uses PyJWT with the issuer's JWKS.  Falls back to unverified claim extraction
    only during tests (``state_info['test_skip_signature']``) — never in production.
    """
    import jwt

    issuer = (state_info.get("issuer") or "").rstrip("/")
    client_id = state_info.get("client_id", "")
    nonce = state_info.get("nonce", "")

    # Basic header inspection.
    try:
        header = jwt.get_unverified_header(id_token)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Invalid id_token header: {exc}") from exc

    keys = _fetch_oidc_jwks(issuer)
    if not keys:
        if state_info.get("test_skip_signature"):
            payload = jwt.decode(id_token, options={"verify_signature": False})
            return _email_from_profile(payload)
        raise HTTPException(
            status_code=502,
            detail="Unable to fetch identity-provider signing keys; cannot validate id_token",
        )

    kid = header.get("kid")
    jwk = next((k for k in keys if k.get("kid") == kid), None)
    if not jwk:
        raise HTTPException(status_code=502, detail=f"No JWKS key found for kid {kid!r}")

    try:
        public_key = jwt.PyJWK(jwk)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not load identity-provider signing key: {exc}") from exc

    try:
        payload = jwt.decode(
            id_token,
            public_key,
            algorithms=[header.get("alg", "RS256")],
            audience=client_id,
            issuer=issuer,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"id_token validation failed: {exc}") from exc

    if nonce and payload.get("nonce") != nonce:
        raise HTTPException(status_code=401, detail="OIDC id_token nonce mismatch")

    return _email_from_profile(payload)


def _email_from_profile(profile: dict[str, Any]) -> str:
    """Extract a usable email from an OIDC/SAML user profile."""
    email = (
        profile.get("email")
        or profile.get("preferred_username")
        or profile.get("upn")
        or profile.get("sub")
    )
    if not email:
        raise HTTPException(status_code=502, detail="Identity provider did not return an email")
    return str(email).strip()


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
        verifier, challenge = _pkce_pair()
        nonce = secrets.token_urlsafe(16)
        if sso_type == "azure_ad":
            tenant = cfg["tenant_id"]
            client_id = cfg["client_id"]
            redirect_uri = cfg["redirect_uri"]
            authorize = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/authorize"
            token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
            issuer = f"https://login.microsoftonline.com/{tenant}/v2.0"
        else:
            issuer = cfg["issuer"].rstrip("/")
            client_id = cfg["client_id"]
            redirect_uri = cfg["redirect_uri"]
            authorize = f"{issuer}/authorize"
            token_url = f"{issuer}/token"

        state = generate_state(
            sso_type,
            extra={
                "code_verifier": verifier,
                "code_challenge": challenge,
                "nonce": nonce,
                "issuer": issuer,
                "token_url": token_url,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "client_secret": cfg.get("client_secret", ""),
            },
        )

        params = urlencode({
            "client_id": client_id,
            "response_type": "code",
            "scope": cfg.get("scopes") or "openid email profile",
            "redirect_uri": redirect_uri,
            "state": state,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
            "nonce": nonce,
        })
        return RedirectResponse(f"{authorize}?{params}", status_code=302)

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

    if sso_type not in ("oidc", "azure_ad"):
        raise HTTPException(status_code=400, detail="Unsupported SSO callback")
    if not code:
        raise HTTPException(status_code=400, detail="Authorization code required")

    state_info = get_state(state, sso_type)
    if not state_info:
        raise HTTPException(status_code=400, detail="Invalid SSO state")

    # Pull PKCE parameters from the state store so they cannot be tampered with.
    verifier = (state_info.get("extra") or {}).get("code_verifier", "")
    redirect_uri = (state_info.get("extra") or {}).get("redirect_uri", "")
    client_id = (state_info.get("extra") or {}).get("client_id", "")
    client_secret = str((state_info.get("extra") or {}).get("client_secret") or "")
    token_url = (state_info.get("extra") or {}).get("token_url", "")

    # Always load confidential-client secret from SSO config (Azure AD / OIDC).
    # Token URL in state must not skip secret — that broke Azure confidential apps.
    from services.integrations_store import get_sso_config_raw

    try:
        cfg = get_sso_config_raw(sso_type)
    except Exception:
        cfg = {}
    if not client_secret and isinstance(cfg, dict):
        client_secret = str(cfg.get("client_secret") or "")
    if not token_url:
        if sso_type == "azure_ad" and isinstance(cfg, dict) and cfg.get("tenant_id"):
            tenant = cfg["tenant_id"]
            token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
        elif isinstance(cfg, dict) and cfg.get("issuer"):
            token_url = f"{str(cfg['issuer']).rstrip('/')}/token"
        if isinstance(cfg, dict):
            redirect_uri = redirect_uri or str(cfg.get("redirect_uri") or "")
            client_id = client_id or str(cfg.get("client_id") or "")
    if not token_url or not client_id:
        raise HTTPException(status_code=400, detail="Missing SSO token endpoint configuration")

    try:
        import httpx

        token_request_data: dict[str, str] = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
        }
        if client_secret:
            token_request_data["client_secret"] = client_secret
        if verifier:
            token_request_data["code_verifier"] = verifier

        token_resp = httpx.post(
            token_url,
            data=token_request_data,
            timeout=20.0,
        )
        token_resp.raise_for_status()
        tokens = token_resp.json()

        # Prefer a validated id_token over a separate userinfo call.
        email = ""
        id_token = tokens.get("id_token", "")
        if id_token:
            state_info_for_id_token = dict(state_info.get("extra") or {})
            # In tests, do not perform real network validation.
            state_info_for_id_token["test_skip_signature"] = not is_production()
            try:
                email = _id_token_email(id_token, state_info_for_id_token)
            except HTTPException:
                email = ""

        if not email:
            access_token = tokens.get("access_token", "")
            if not access_token:
                raise HTTPException(status_code=502, detail="No access token from identity provider")

            if sso_type == "azure_ad":
                userinfo_url = "https://graph.microsoft.com/oidc/userinfo"
            else:
                from services.integrations_store import get_sso_config_raw

                cfg = get_sso_config_raw(sso_type)
                userinfo_url = f"{cfg['issuer'].rstrip('/')}/userinfo"

            user_resp = httpx.get(
                userinfo_url,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=15.0,
            )
            user_resp.raise_for_status()
            email = _email_from_profile(user_resp.json())

        _require_sso_authorization(email)

        _, expires_at = create_token(str(email))
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

        return _redirect_with_token(email, expires_at)
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
    if not relay_state or not get_and_pop(relay_state, sso_type):
        raise HTTPException(status_code=400, detail="Invalid or missing SAML RelayState")

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

    _require_sso_authorization(email)

    _, expires_at = create_token(str(email))
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

    return _redirect_with_token(email, expires_at)


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
