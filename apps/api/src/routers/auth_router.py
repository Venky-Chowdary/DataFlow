from __future__ import annotations

import base64
import hashlib
import logging
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode, urlsplit

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from services.brand_env import getenv_brand
from services.platform_config import is_production, web_url
from pydantic import BaseModel, Field

from services.user_store import get_user as get_stored_user
from services.user_store import normalize_email, set_password
from services.sso_authorization import require_sso_authorization
from services.workspace_access import actor_email

from ..services.auth_service import (
    auth_bootstrap_status,
    auth_required,
    authenticate,
    create_token,
    public_user,
    revoke_token,
)

try:
    from services import oidc_client
    from services.sso_state import (
        SsoStoreUnavailable,
        claim_once,
        generate_state,
        get_state,
        set_state,
    )
except ImportError:  # pragma: no cover - tests with src on PYTHONPATH
    from src.services import oidc_client
    from src.services.sso_state import (
        SsoStoreUnavailable,
        claim_once,
        generate_state,
        get_state,
        set_state,
    )

router = APIRouter(prefix="/auth", tags=["auth"])
logger = logging.getLogger(__name__)


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=256)


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(min_length=8, max_length=256)
    new_password: str = Field(min_length=12, max_length=256)


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
            "relaxDestinationValidation": False,
            "destinationStrictlyMatches": True,
            "rejectUnsolicitedResponsesWithInResponseTo": True,
            "rejectDeprecatedAlgorithm": True,
            "allowSingleLabelDomains": not is_production(),
            "wantMessagesSigned": False,
        },
    }


def _saml_request_dict(request: Request, post_data: dict[str, str] | None = None) -> dict[str, Any]:
    acs = urlsplit(_saml_acs_url(request))
    scheme = acs.scheme
    host = acs.hostname or ""
    port = acs.port or (443 if scheme == "https" else 80)
    request_uri = acs.path or "/"
    if acs.query:
        request_uri = f"{request_uri}?{acs.query}"
    return {
        "https": "on" if scheme == "https" else "off",
        "http_host": host,
        "server_port": port,
        "script_name": acs.path or "/",
        "get_data": {},
        "post_data": post_data or {},
        "lowercase_urlencoding": False,
        "request_uri": request_uri,
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


def _redirect_with_token(email: str, expires_at: int | None = None) -> RedirectResponse:
    """Return the token in the URL fragment so it is not sent to the server or logged."""
    token, minted_expires = create_token(str(email))
    exp = int(expires_at) if expires_at is not None else minted_expires
    params = urlencode(
        {
            "sso_token": token,
            "expires_at": str(exp),
            "sso_email": email,
        }
    )
    return RedirectResponse(f"{_web_origin()}/#{params}", status_code=302)


def _oidc_discovery_url(issuer: str) -> str:
    """Return the OIDC discovery document URL for an issuer."""
    base = issuer.rstrip("/")
    return f"{base}/.well-known/openid-configuration"


def _audit_sso_failure(sso_type: str, reason: str, resource: str) -> None:
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action="auth.sso.failure",
            resource=resource,
            level="error",
            details={"provider": sso_type, "reason": reason},
        )
    except Exception:
        logger.error(
            "SSO failure audit append failed (sso_type=%s reason=audit_write_failed)",
            sso_type,
        )


def _raise_sso_failure(
    sso_type: str,
    reason: str,
    status_code: int,
    detail: str,
    *,
    issuer: str = "",
    resource: str | None = None,
) -> None:
    logger.warning(
        "SSO request failed (sso_type=%s issuer=%s reason=%s)",
        sso_type,
        issuer,
        reason,
    )
    _audit_sso_failure(
        sso_type,
        reason,
        resource or f"/auth/sso/{sso_type}",
    )
    raise HTTPException(status_code=status_code, detail=detail)


def _raise_oidc_failure(
    exc: oidc_client.OidcError,
    sso_type: str,
    issuer: str,
) -> None:
    if isinstance(exc, oidc_client.OidcIdpUnavailable):
        status_code = 503
    elif isinstance(exc, oidc_client.OidcTokenInvalid):
        status_code = 401
    else:
        status_code = 400
    _raise_sso_failure(
        sso_type,
        exc.reason,
        status_code,
        f"SSO login failed ({exc.reason})",
        issuer=issuer,
        resource=f"/auth/sso/{sso_type}",
    )


def _oidc_issuer(sso_type: str, cfg: dict[str, Any]) -> str:
    if sso_type == "azure_ad":
        tenant = str(cfg.get("tenant_id") or "").strip()
        if tenant.lower() in {"common", "organizations", "consumers"}:
            raise oidc_client.OidcConfigError("multi_tenant_not_supported")
        if not tenant or any(char in tenant for char in "/?#"):
            raise oidc_client.OidcConfigError("invalid_tenant")
        return f"https://login.microsoftonline.com/{tenant}/v2.0"
    return str(cfg.get("issuer") or "").strip().rstrip("/")


def _safe_idp_error(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"[^A-Za-z0-9_.-]", "", value[:64])


def _saml_replay_expiry(value: Any) -> datetime:
    now = datetime.now(timezone.utc)
    expiry: datetime | None = None
    if isinstance(value, datetime):
        expiry = value
    elif isinstance(value, (int, float)):
        try:
            expiry = datetime.fromtimestamp(value, timezone.utc)
        except (OverflowError, OSError, ValueError):
            expiry = None
    elif isinstance(value, str):
        try:
            expiry = datetime.fromtimestamp(float(value), timezone.utc)
        except (OverflowError, OSError, ValueError):
            try:
                expiry = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                expiry = None
    if expiry is None:
        expiry = now
    elif expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    return max(expiry, now) + timedelta(seconds=300)


@router.get("/sso/providers")
async def sso_providers():
    from services.integrations_store import list_sso_providers_public

    return {"providers": list_sso_providers_public()}


@router.get("/sso/{sso_type}/start")
async def sso_start(sso_type: str, request: Request):
    from services.integrations_store import get_sso_config_raw, validate_sso_config

    if sso_type not in ("oidc", "azure_ad", "saml"):
        _raise_sso_failure(sso_type, "unsupported_sso_type", 400, "Unsupported SSO type")
    check = validate_sso_config(sso_type)
    if not check["ready"]:
        _raise_sso_failure(
            sso_type,
            "sso_config_incomplete",
            400,
            check["message"],
        )

    cfg = get_sso_config_raw(sso_type)

    if sso_type in ("oidc", "azure_ad"):
        issuer = ""
        try:
            issuer = _oidc_issuer(sso_type, cfg)
            metadata = oidc_client.discover(issuer)
            verifier, challenge = _pkce_pair()
            nonce = secrets.token_urlsafe(16)
            client_id = str(cfg["client_id"])
            redirect_uri = str(cfg["redirect_uri"])
            state = generate_state(
                sso_type,
                extra={
                    "code_verifier": verifier,
                    "nonce": nonce,
                    "issuer": issuer,
                    "redirect_uri": redirect_uri,
                    "client_id": client_id,
                },
            )
        except oidc_client.OidcError as exc:
            _raise_oidc_failure(exc, sso_type, issuer)
        except SsoStoreUnavailable:
            _raise_sso_failure(
                sso_type,
                "store_unavailable",
                503,
                "SSO login failed (store_unavailable)",
                issuer=issuer,
            )

        params = urlencode(
            {
                "client_id": client_id,
                "response_type": "code",
                "scope": cfg.get("scopes") or "openid email profile",
                "redirect_uri": redirect_uri,
                "state": state,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "nonce": nonce,
            }
        )
        return RedirectResponse(
            f"{metadata.authorization_endpoint}?{params}",
            status_code=302,
        )

    if sso_type == "saml":
        try:
            from onelogin.saml2.auth import OneLogin_Saml2_Auth
        except ImportError as exc:
            logger.warning("SAML support unavailable (sso_type=saml reason=library_missing)")
            raise HTTPException(status_code=501, detail="SAML support is not installed") from exc
        saml_settings = _saml_settings_dict(request, cfg)
        req = _saml_request_dict(request)
        auth = OneLogin_Saml2_Auth(req, saml_settings)
        relay = secrets.token_urlsafe(16)
        url = auth.login(return_to=relay)
        try:
            set_state(
                relay,
                "saml",
                extra={"request_id": auth.get_last_request_id()},
            )
        except SsoStoreUnavailable:
            _raise_sso_failure(
                "saml",
                "store_unavailable",
                503,
                "SSO login failed (store_unavailable)",
                resource="/auth/sso/saml/start",
            )
        return RedirectResponse(url, status_code=302)


@router.get("/sso/{sso_type}/callback")
async def sso_callback(sso_type: str, code: str = "", state: str = "", error: str = ""):
    resource = f"/auth/sso/{sso_type}/callback"
    if sso_type not in ("oidc", "azure_ad"):
        _raise_sso_failure(
            sso_type,
            "unsupported_sso_type",
            400,
            "Unsupported SSO callback",
            resource=resource,
        )
    if error:
        if state:
            try:
                get_state(state, sso_type)
            except SsoStoreUnavailable:
                _raise_sso_failure(
                    sso_type,
                    "store_unavailable",
                    503,
                    "SSO login failed (store_unavailable)",
                    resource=resource,
                )
        safe_error = _safe_idp_error(error)
        logger.warning(
            "Identity provider rejected SSO callback (sso_type=%s issuer= reason=idp_rejected idp_error=%s)",
            sso_type,
            safe_error,
        )
        _audit_sso_failure(sso_type, "idp_rejected", resource)
        raise HTTPException(
            status_code=400,
            detail="SSO login was cancelled or rejected by the identity provider",
        )
    if not code:
        _raise_sso_failure(
            sso_type,
            "missing_authorization_code",
            400,
            "Authorization code required",
            resource=resource,
        )
    try:
        state_info = get_state(state, sso_type)
    except SsoStoreUnavailable:
        _raise_sso_failure(
            sso_type,
            "store_unavailable",
            503,
            "SSO login failed (store_unavailable)",
            resource=resource,
        )
    if not state_info:
        _raise_sso_failure(
            sso_type,
            "invalid_state",
            400,
            "Invalid SSO state",
            resource=resource,
        )
    from services.integrations_store import get_sso_config_raw

    extra = state_info.get("extra") or {}
    try:
        cfg = get_sso_config_raw(sso_type)
        issuer = _oidc_issuer(sso_type, cfg)
    except oidc_client.OidcError as exc:
        _raise_oidc_failure(exc, sso_type, "")
    current_client_id = str(cfg.get("client_id") or "")
    if (
        extra.get("issuer") != issuer
        or extra.get("client_id") != current_client_id
    ):
        _raise_sso_failure(
            sso_type,
            "sso_config_changed",
            400,
            "SSO login failed (sso_config_changed)",
            issuer=issuer,
            resource=resource,
        )
    verifier = str(extra.get("code_verifier") or "")
    redirect_uri = str(extra.get("redirect_uri") or "")
    client_id = current_client_id
    nonce = str(extra.get("nonce") or "")
    if not verifier or not redirect_uri or not client_id or not nonce:
        _raise_sso_failure(
            sso_type,
            "invalid_state",
            400,
            "Invalid SSO state",
            issuer=issuer,
            resource=resource,
        )
    try:
        metadata = oidc_client.discover(issuer)
        token_request_data: dict[str, str] = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "code_verifier": verifier,
        }
        token_request_data["client_secret"] = str(cfg.get("client_secret") or "")
        try:
            with oidc_client._http_client() as client:
                token_resp = client.post(
                    metadata.token_endpoint,
                    data=token_request_data,
                )
        except httpx.HTTPError as exc:
            raise oidc_client.OidcIdpUnavailable("token_exchange_failed") from exc
        if token_resp.status_code >= 500:
            raise oidc_client.OidcIdpUnavailable("token_exchange_failed")
        if token_resp.status_code >= 400:
            idp_error = ""
            try:
                error_body = token_resp.json()
                if isinstance(error_body, dict):
                    idp_error = _safe_idp_error(error_body.get("error"))
            except (ValueError, TypeError):
                idp_error = ""
            logger.warning(
                "OIDC code exchange rejected (sso_type=%s issuer=%s reason=code_exchange_rejected idp_error=%s)",
                sso_type,
                issuer,
                idp_error,
            )
            raise oidc_client.OidcTokenInvalid("code_exchange_rejected")
        try:
            tokens = token_resp.json()
        except (ValueError, TypeError) as exc:
            raise oidc_client.OidcTokenInvalid("malformed_token_response") from exc
        if not isinstance(tokens, dict):
            raise oidc_client.OidcTokenInvalid("malformed_token_response")
        id_token = tokens.get("id_token")
        if not isinstance(id_token, str) or not id_token:
            raise oidc_client.OidcTokenInvalid("no_id_token")
        algorithms = oidc_client.allowed_algorithms(sso_type, metadata)
        claims = oidc_client.validate_id_token(
            id_token,
            metadata=metadata,
            client_id=client_id,
            nonce=nonce,
            algorithms=algorithms,
            leeway=oidc_client.clock_skew_leeway(),
            sso_type=sso_type,
        )
        email = oidc_client.email_from_claims(claims, sso_type=sso_type)
        require_sso_authorization(email)
        _audit_sso_success(sso_type, email, resource)
        return _redirect_with_token(email)
    except oidc_client.OidcError as exc:
        _raise_oidc_failure(exc, sso_type, issuer)
    except SsoStoreUnavailable:
        _raise_sso_failure(
            sso_type,
            "store_unavailable",
            503,
            "SSO login failed (store_unavailable)",
            issuer=issuer,
            resource=resource,
        )
    except HTTPException:
        _audit_sso_failure(sso_type, "sso_unauthorized", resource)
        raise
    except Exception:
        _audit_sso_failure(sso_type, "callback_failed", resource)
        logger.exception(
            "SSO callback failed (sso_type=%s issuer=%s reason=callback_failed)",
            sso_type,
            issuer,
        )
        raise HTTPException(
            status_code=502,
            detail="SSO login failed (callback_failed)",
        ) from None


def _audit_sso_success(sso_type: str, email: str, resource: str) -> None:
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action="auth.sso.login",
            resource=resource,
            actor=email,
            level="success",
            details={"provider": sso_type},
        )
    except Exception:
        logger.warning(
            "SSO success audit append failed (sso_type=%s reason=audit_write_failed)",
            sso_type,
        )


@router.post("/sso/{sso_type}/callback")
async def sso_post_callback(sso_type: str, request: Request):
    if sso_type != "saml":
        _raise_sso_failure(
            sso_type,
            "unsupported_callback_method",
            405,
            "POST callback is only supported for SAML",
        )

    try:
        from onelogin.saml2.auth import OneLogin_Saml2_Auth
    except ImportError as exc:
        logger.warning("SAML support unavailable (sso_type=saml reason=library_missing)")
        raise HTTPException(status_code=501, detail="SAML support is not installed") from exc

    from services.integrations_store import get_sso_config_raw

    cfg = get_sso_config_raw(sso_type)
    form = await request.form()
    saml_response = str(form.get("SAMLResponse", ""))
    relay_state = str(form.get("RelayState", ""))
    if not saml_response:
        raise HTTPException(status_code=400, detail="SAMLResponse is required")
    if not relay_state:
        _raise_sso_failure(
            "saml",
            "missing_relay_state",
            400,
            "Invalid or missing SAML RelayState",
            resource="/auth/sso/saml/callback",
        )
    try:
        state_info = get_state(relay_state, "saml")
    except SsoStoreUnavailable:
        _raise_sso_failure(
            "saml",
            "store_unavailable",
            503,
            "SSO login failed (store_unavailable)",
            resource="/auth/sso/saml/callback",
        )
    request_id = str((state_info or {}).get("extra", {}).get("request_id") or "")
    if not state_info or not request_id:
        _raise_sso_failure(
            "saml",
            "invalid_relay_state",
            400,
            "Invalid or missing SAML RelayState",
            resource="/auth/sso/saml/callback",
        )
    req = _saml_request_dict(request, post_data={"SAMLResponse": saml_response})
    saml_settings = _saml_settings_dict(request, cfg)
    auth = OneLogin_Saml2_Auth(req, saml_settings)
    try:
        auth.process_response(request_id=request_id)
    except Exception as exc:
        logger.warning(
            "SAML response processing failed (sso_type=saml reason=response_invalid error_type=%s)",
            type(exc).__name__,
        )
        _audit_sso_failure(
            "saml",
            "response_invalid",
            "/auth/sso/saml/callback",
        )
        raise HTTPException(
            status_code=401,
            detail="SAML response invalid: malformed_response",
        ) from None
    errors = auth.get_errors()
    if errors:
        logger.warning(
            "SAML response rejected (sso_type=saml reason=response_invalid idp_reason=%s)",
            auth.get_last_error_reason(),
        )
        _audit_sso_failure(
            "saml",
            "response_invalid",
            "/auth/sso/saml/callback",
        )
        safe_errors = [str(item) for item in errors if isinstance(item, str)]
        raise HTTPException(
            status_code=401,
            detail=f"SAML response invalid: {', '.join(safe_errors)}",
        )
    assertion_id = auth.get_last_assertion_id()
    if not assertion_id:
        _raise_sso_failure(
            "saml",
            "assertion_id_missing",
            401,
            "SAML response invalid: assertion_id_missing",
            resource="/auth/sso/saml/callback",
        )
    expires = _saml_replay_expiry(auth.get_last_assertion_not_on_or_after())
    try:
        claimed = claim_once("saml_assertion", str(assertion_id), expires)
    except SsoStoreUnavailable:
        _raise_sso_failure(
            "saml",
            "store_unavailable",
            503,
            "SSO login failed (store_unavailable)",
            resource="/auth/sso/saml/callback",
        )
    if not claimed:
        _raise_sso_failure(
            "saml",
            "assertion_replay",
            401,
            "SAML response invalid: assertion_replay",
            resource="/auth/sso/saml/callback",
        )
    if not auth.is_authenticated():
        _raise_sso_failure(
            "saml",
            "authentication_failed",
            401,
            "SAML authentication failed",
            resource="/auth/sso/saml/callback",
        )

    name_id = auth.get_nameid()
    email = str(name_id or "").strip()
    if not email or "@" not in email:
        email_attr = cfg.get("email_attribute", "email")
        attributes = auth.get_attributes()
        email_value = attributes.get(email_attr, [""])
        email = str(email_value[0] if isinstance(email_value, list) and email_value else email_value or "").strip()
    if not email or "@" not in email:
        _raise_sso_failure(
            "saml",
            "no_email",
            401,
            "SAML identity did not return an email",
            resource="/auth/sso/saml/callback",
        )
    email = normalize_email(email)
    try:
        require_sso_authorization(email)
    except HTTPException:
        _audit_sso_failure(
            "saml",
            "sso_unauthorized",
            "/auth/sso/saml/callback",
        )
        raise
    _audit_sso_success("saml", email, "/auth/sso/saml/callback")
    return _redirect_with_token(email)


@router.get("/bootstrap")
async def auth_bootstrap(request: Request):
    """Public auth diagnostics — no account enumeration (audit ITEM 3)."""
    # Authenticated operators get richer deploy diagnostics (still no secrets).
    sensitive = False
    try:
        from ..services.auth_service import lookup_user, verify_token

        auth = (request.headers.get("authorization") or "").strip()
        if auth.lower().startswith("bearer "):
            email = verify_token(auth.split(" ", 1)[1].strip())
            sensitive = bool(email and lookup_user(email))
    except Exception as exc:
        logging.getLogger(__name__).error(
            "auth bootstrap token inspection failed; returning public payload only",
            exc_info=exc,
        )
        sensitive = False
    return auth_bootstrap_status(include_sensitive=sensitive)


def _login_client_ip(request: Request) -> str:
    from services.client_ip import client_ip_from_request

    return client_ip_from_request(request) or "unknown"


@router.post("/logout")
async def logout(request: Request):
    """Revoke the current Bearer session (Phase D3). Idempotent."""
    auth = (request.headers.get("authorization") or "").strip()
    token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    revoked = False
    if token:
        try:
            revoked = bool(revoke_token(token))
        except Exception as exc:
            logging.getLogger(__name__).warning("logout revoke failed: %s", exc)
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action="auth.logout",
            resource="/auth/logout",
            actor=getattr(request.state, "user_email", None) or "anonymous",
            level="success",
            details={"revoked": revoked},
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    return {"ok": True, "revoked": revoked}


@router.post("/login")
async def login(body: LoginRequest, request: Request):
    from services.auth_rate_limit import (
        check_login_rate_limit,
        record_login_failure,
        record_login_success,
    )

    ip = _login_client_ip(request)
    limited = check_login_rate_limit(ip=ip, email=body.email)
    if not limited.get("allowed"):
        retry = float(limited.get("retry_after_sec") or 60)
        raise HTTPException(
            status_code=429,
            detail="Too many login attempts. Try again later.",
            headers={"Retry-After": str(int(max(1, retry)))},
        )

    status = auth_bootstrap_status()
    if not status.get("has_users"):
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
        record_login_failure(ip=ip, email=body.email)
        # What an anonymous caller is told is exactly what happened: the pair did
        # not authenticate. Deployment advice (env var escaping) belongs to the
        # operator configuring the service, not to an unauthenticated 401 — it
        # leaks how identities are provisioned and, as the sign-in screen read it,
        # turned a stale password into "control plane unreachable".
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    record_login_success(ip=ip, email=body.email)
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
    account = get_stored_user(user["email"])
    return {
        "token": token,
        "expires_at": expires_at,
        "user": public_user(user),
        # An admin-issued one-time password is only temporary if the operator is
        # told to rotate it; the client prompts on this flag.
        "must_change_password": bool(account and account.get("must_change_password")),
    }


@router.get("/me")
async def me(request: Request):
    """Who the API decided you are, and what it will let you do here.

    The client used to infer authority from the platform label it stashed at
    login, which says nothing about the workspace being viewed — so it rendered
    every write control for a viewer and let the button discover the refusal.
    This is the single source the UI gates on: the same resolution the request
    gate itself applies, for the workspace named by ``X-Workspace-Id``.
    """
    from services.effective_role import (
        permission_summary,
        resolved_workspace_id,
        workspace_id_from_request_headers,
    )

    user = getattr(request.state, "user", None)
    email = normalize_email(getattr(request.state, "user_email", "") or (user or {}).get("email", ""))
    if not email:
        raise HTTPException(status_code=401, detail="Authentication required")
    named_workspace_id = workspace_id_from_request_headers(request.headers)
    # The workspace the request named, or the single membership that answered it,
    # so the client can name that workspace explicitly from here on. The summary
    # is resolved in that same workspace: a response whose permissions were
    # decided somewhere other than the workspace it reports is not an answer.
    workspace_id = resolved_workspace_id(user, named_workspace_id)
    summary = permission_summary(user, workspace_id)
    account = get_stored_user(email)
    workspace_role = ""
    if workspace_id:
        try:
            from services.team_store import get_workspace_role

            workspace_role = get_workspace_role(workspace_id=workspace_id, email=email)
        except Exception as exc:
            logging.getLogger(__name__).warning("workspace role lookup failed: %s", exc)
    display_name = str((user or {}).get("name") or "").strip()
    if not display_name and account:
        display_name = str(account.get("name") or "").strip()
    return {
        "email": email,
        "name": display_name or email,
        "platform_role": str((user or {}).get("role") or "member"),
        "workspace_id": workspace_id,
        "workspace_role": workspace_role,
        "must_change_password": bool(account and account.get("must_change_password")),
        "auth_required": auth_required(),
        **summary,
    }


@router.post("/change-password")
async def change_password(body: ChangePasswordRequest, request: Request):
    """Rotate your own password — how an admin-issued one-time password is retired."""
    actor = normalize_email(actor_email(request))
    if actor in ("", "anonymous"):
        raise HTTPException(status_code=401, detail="Sign in before changing your password")
    if get_stored_user(actor) is None:
        raise HTTPException(
            status_code=400,
            detail="This account is provisioned by the deployment environment — change it there",
        )
    if not authenticate(actor, body.current_password):
        raise HTTPException(status_code=403, detail="Current password is incorrect")
    if body.new_password == body.current_password:
        raise HTTPException(status_code=400, detail="Choose a password you have not used")
    set_password(email=actor, password=body.new_password)
    try:
        from services.audit_log import append_audit_event

        append_audit_event(
            action="auth.password_change",
            resource="/auth/change-password",
            actor=actor,
            level="warn",
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    return {"ok": True}
