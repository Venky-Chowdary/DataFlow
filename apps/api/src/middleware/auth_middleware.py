from __future__ import annotations

import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

import hmac

from services.platform_config import docs_enabled
from services.tenant_bind import principal_allowed_for_tenant

from ..services import auth_service as _auth_service
from ..services.auth_service import lookup_user, verify_token

logger = logging.getLogger(__name__)

_PUBLIC_PREFIXES = (
    "/health",
    "/api/v1/health",
    "/api/v1/auth/login",
    "/api/v1/auth/logout",
    "/api/v1/auth/bootstrap",
    "/api/v1/auth/sso/providers",
    # Unauthenticated self-serve signup paths (each 404s while
    # DATAWRAP_SIGNUP_ENABLED is off — the router guards, not this list).
    "/api/v1/auth/signup",
    "/api/v1/auth/verify-email",
    "/api/v1/auth/resend",
    "/api/v1/auth/resend-webhook",
    "/api/v1/auth/form-token",
    "/api/v1/auth/password/forgot",
    "/api/v1/auth/password/reset",
    "/api/v1/public/form-token",
    "/api/v1/public/email-events",
    "/auth/signup",
    "/auth/verify-email",
    "/auth/resend",
    "/auth/resend-webhook",
    "/auth/form-token",
    "/auth/password/forgot",
    "/auth/password/reset",
    "/public/form-token",
    "/public/email-events",
    # Alias paths when the web client omits /api/v1 (mis-set VITE_API_BASE).
    "/auth/login",
    "/auth/logout",
    "/auth/bootstrap",
    "/auth/sso/providers",
    # Marketing / docs / landing need catalog stats without a session.
    "/api/v1/catalog",
    # Contact Sales lead form (D103) — visitors have no session.
    "/api/v1/contact-sales",
    "/catalog",
    "/.well-known/oauth-protected-resource",
)

if docs_enabled():
    _PUBLIC_PREFIXES = _PUBLIC_PREFIXES + ("/docs", "/redoc", "/openapi.json")


def _is_public_sso_path(path: str) -> bool:
    if path.startswith("/api/v1/auth/sso/") or path.startswith("/auth/sso/"):
        return (path.endswith("/start") or path.endswith("/callback")
                or path.endswith("/providers") or path.endswith("/route"))
    return False


def _unauthenticated_rate_limit(request: Request):
    """429 on public routes when the caller has no session.

    Health is excluded inside the limiter. Authenticated calls are not
    counted. Set DATAFLOW_PUBLIC_RATE_LIMIT=1 on the API service to turn it
    on (it is off until that flag is set).
    """
    if getattr(request.state, "user_email", None):
        return None
    path = request.url.path
    public = (
        request.method == "OPTIONS"
        or path == "/"
        or any(path.startswith(p) for p in _PUBLIC_PREFIXES)
        or _is_public_sso_path(path)
        or _is_public_mcp_path(path, request.method)
    )
    if not public:
        return None
    from services.auth_rate_limit import check_public_rate_limit
    from services.client_ip import client_ip_from_request

    outcome = check_public_rate_limit(client_ip_from_request(request) or "unknown", path)
    if outcome.get("allowed", True):
        return None
    retry = max(1, int(float(outcome.get("retry_after_sec") or 1)))
    return JSONResponse(
        status_code=429,
        content={
            "error": "rate_limited",
            "detail": "Too many unauthenticated requests.",
        },
        headers={"Retry-After": str(retry)},
    )


def _is_public_mcp_path(path: str, method: str) -> bool:
    """MCP discovery + Streamable handshake are public; tool execution is not.

    ``POST /api/v1/mcp`` stays public so Cursor can ``initialize`` / ``tools/list``
    without a Bearer token. ``tools/call`` (JSON-RPC and REST) requires auth —
    enforced here for REST and in ``mcp_protocol`` for Streamable HTTP.
    """
    if path in ("/api/v1/mcp", "/api/v1/mcp/"):
        return True
    if path.startswith("/api/v1/mcp/manifest") or path.startswith("/api/v1/mcp/status"):
        return True
    if path.rstrip("/") == "/api/v1/mcp/tools" and method.upper() == "GET":
        return True
    return False


def _heal_rekey_if_pending(stored: dict | None, email: str) -> None:
    """Finish a half-moved email rekey on the next authenticated request.

    The flag comes from the already-loaded user doc (no second store read);
    finish_pending_rekeys claims it single-flight, so parallel requests
    cannot double-move memberships, and a failed retry frees the lease,
    keeps the flag, and surfaces a security event."""
    if not stored or not stored.get("rekey_pending"):
        return
    try:
        from services.signup_service import finish_pending_rekeys
        finish_pending_rekeys(email=email)
    except Exception:
        logger.error("rekey_pending retry failed", exc_info=True)
        try:
            from services.audit_coverage import record_security_event
            record_security_event(
                "email_change.rekey_flag_failed",
                outcome="failed", reason_code="heal_retry_failed",
            )
        except Exception:
            logger.error("rekey heal audit event failed", exc_info=True)


def _attach_user_reason(request: Request, token: str) -> str | None:
    email = verify_token(token)
    if email:
        from services.user_store import get_user_record as get_stored_user

        stored = get_stored_user(email)
        if stored and stored.get("status") == "disabled":
            return "disabled_user"
        _heal_rekey_if_pending(stored, email)
        request.state.user_email = email
        user = lookup_user(email)
        if user:
            request.state.user = user
        else:
            # Valid token for a user not in the static user list: default to viewer.
            request.state.user = {"email": email, "role": "viewer"}
        return None

    from services.integrations_store import is_workspace_api_key, verify_workspace_api_key

    key_info = verify_workspace_api_key(token) if is_workspace_api_key(token) else None
    if key_info:
        request.state.user_email = key_info.get("created_by") or "api-key"
        user = {
            "email": request.state.user_email,
            "role": key_info.get("role") or "viewer",
            "auth_kind": "api_key",
            "kind": key_info.get("kind") or "api_key",
            "api_key_id": key_info["id"],
        }
        if key_info.get("scopes") is not None:
            user["scopes"] = key_info["scopes"]
        if key_info.get("workspace_id"):
            user["workspace_id"] = key_info["workspace_id"]
        # Workspace API keys may carry tenant binding (same claims as users).
        if key_info.get("tenant_id"):
            user["tenant_id"] = key_info["tenant_id"]
        if key_info.get("tenant_ids"):
            user["tenant_ids"] = key_info["tenant_ids"]
        request.state.user = user
        request.state.api_key_id = key_info["id"]
        request.state.api_key_created_by = key_info.get("created_by") or ""
        request.state.api_key_auth = True
        return None

    if is_workspace_api_key(token):
        return "unknown_api_key"

    if not request.url.path.startswith("/api/v1/mcp"):
        return "invalid_token"
    from services.mcp_oauth import principal_from_access_token

    user = principal_from_access_token(token, user_lookup=lookup_user)
    if not user:
        return "oauth_token_invalid"
    request.state.user_email = user["email"]
    request.state.user = user
    return None


def _attach_user(request: Request, token: str) -> bool:
    return _attach_user_reason(request, token) is None


def _cookie_session(request: Request) -> dict | str | None:
    """``__Host-dw_session`` -> active session doc (P3-1).

    The CSRF check is done by the caller on non-public paths only, so a
    stale cookie can't turn a public POST (signup/resend) into a 403.
    """
    try:
        from services.signup_config import signup_enabled
        if not signup_enabled():
            return None
    except Exception:  # noqa: BLE001 — flag read must never break auth
        return None
    try:
        jti = (request.cookies or {}).get("__Host-dw_session", "")
    except Exception:  # noqa: BLE001
        jti = ""
    if not jti:
        return None
    from services.auth_state import get_auth_state

    doc = get_auth_state().get_session(jti)
    if not doc or doc.get("revoked_at"):
        return None
    return doc


def _attach_cookie_user(request: Request, doc: dict) -> str | None:
    """Cookie-auth attach + CSRF double-submit for mutating methods."""
    if request.method not in ("GET", "HEAD", "OPTIONS"):
        supplied = request.headers.get("x-csrf-token", "")
        expected = doc.get("csrf") or ""
        if not supplied or not expected or not hmac.compare_digest(supplied, expected):
            return "csrf_failed"
    from services.user_store import get_user_record as get_stored_user

    email = doc.get("user_id") or ""
    stored = get_stored_user(email)
    if stored and stored.get("status") in ("disabled", "banned"):
        return "disabled_user"
    # A flagged half-finished email rekey heals on the next authenticated
    # request: retry the membership/org move; the flag stays set if the
    # retry fails again, so this never swallows the failure.
    _heal_rekey_if_pending(stored, email)
    request.state.user_email = email
    user = lookup_user(email)
    request.state.user = user if user else {"email": email, "role": "viewer"}
    request.state.cookie_session_jti = doc["jti"]
    from services.auth_state import get_auth_state

    get_auth_state().touch_session(doc["jti"])
    return None


def _tenant_bind_forbidden(request: Request) -> JSONResponse | None:
    """Phase D2 — Host tenant must match authenticated identity claims."""
    tenant_id = getattr(request.state, "tenant_id", None) or ""
    if not tenant_id:
        return None
    user = getattr(request.state, "user", None)
    if not user:
        return None
    if principal_allowed_for_tenant(user, tenant_id):
        return None
    from services.audit_coverage import (
        _suppressed_since_last,
        record_security_event,
    )

    client_ip = request.client.host if request.client else ""
    suppressed = _suppressed_since_last(
        client_ip or "unknown",
        "authz.tenant_bind.denied:tenant_not_bound",
        request.url.path,
    )
    if suppressed is not None:
        record_security_event(
            "authz.tenant_bind.denied",
            outcome="failed",
            request=request,
            resource=request.url.path,
            reason_code="tenant_not_bound",
            status=403,
            details={
                "suppressed_since_last": suppressed,
                "client_ip": client_ip,
            },
        )
    return JSONResponse(
        status_code=403,
        content={
            "detail": (
                "Tenant Host does not match authenticated identity — "
                "cross-tenant access refused (re-auth on the correct domain)."
            ),
            "tenant_id": tenant_id,
        },
    )


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        principal_context = None
        principal_token = None
        try:
            auth = request.headers.get("Authorization", "")
            token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
            cookie_doc = None
            if not token:
                cookie_doc = _cookie_session(request)
            # EventSource / SSE cannot send custom headers, so the token may be
            # passed as a query parameter.  Only allow this fallback on the SSE
            # stream paths to keep tokens out of general access logs.
            if not token and request.url.path.endswith("/stream"):
                token = request.query_params.get("token") or request.query_params.get("access_token") or ""

            # Resolved on the module, not copied at import: enforcement is a live
            # setting, and a stale copy means the middleware and the auth service
            # disagree about whether the request needs a token.
            if not _auth_service.auth_required():
                if token and _attach_user_reason(request, token) is None:
                    from services import audit_coverage

                    principal_context = audit_coverage
                    principal_token = audit_coverage.set_request_principal(
                        audit_coverage.request_principal(request)
                    )
                    audit_coverage.note_workspace_header(request)
                    forbidden = _tenant_bind_forbidden(request)
                    if forbidden is not None:
                        return forbidden
                else:
                    from services.audit_coverage import note_workspace_header

                    note_workspace_header(request)
                limited = _unauthenticated_rate_limit(request)
                if limited is not None:
                    return limited
                return await call_next(request)

            path = request.url.path
            if (
                request.method == "OPTIONS"
                or path == "/"
                or any(path.startswith(p) for p in _PUBLIC_PREFIXES)
                or _is_public_sso_path(path)
                or _is_public_mcp_path(path, request.method)
            ):
                # Public routes still attach identity when a Bearer token is present
                # (MCP tools/call uses this; discovery works without a token).
                if token and _attach_user_reason(request, token) is None:
                    from services import audit_coverage

                    principal_context = audit_coverage
                    principal_token = audit_coverage.set_request_principal(
                        audit_coverage.request_principal(request)
                    )
                    audit_coverage.note_workspace_header(request)
                    forbidden = _tenant_bind_forbidden(request)
                    if forbidden is not None:
                        return forbidden
                else:
                    from services.audit_coverage import note_workspace_header

                    note_workspace_header(request)
                limited = _unauthenticated_rate_limit(request)
                if limited is not None:
                    return limited
                return await call_next(request)

            reason_code = (
                "missing_credentials"
                if not (token or cookie_doc)
                else _attach_user_reason(request, token)
                if token
                else _attach_cookie_user(request, cookie_doc)
            )
            if reason_code == "csrf_failed":
                return JSONResponse(
                    status_code=403,
                    content={"detail": "CSRF token required for cookie-authenticated requests"},
                )
            if reason_code:
                from services import audit_coverage

                audit_coverage.note_workspace_header(request)
                client_ip = request.client.host if request.client else ""
                suppressed = audit_coverage._suppressed_since_last(
                    client_ip or "unknown",
                    "auth.request.rejected:" + reason_code,
                    path,
                )
                if suppressed is not None:
                    audit_coverage.record_security_event(
                        "auth.request.rejected",
                        outcome="failed",
                        request=request,
                        resource=path,
                        reason_code=reason_code,
                        status=401,
                        details={
                            "suppressed_since_last": suppressed,
                            "client_ip": client_ip,
                        },
                    )
                if path == "/api/v1/mcp/tools/call":
                    from services.mcp_oauth import bearer_challenge

                    return JSONResponse(
                        status_code=401,
                        content={"detail": "Authentication required"},
                        headers={"WWW-Authenticate": bearer_challenge()},
                    )
                return JSONResponse(status_code=401, content={"detail": "Authentication required"})

            from services import audit_coverage

            principal_context = audit_coverage
            principal_token = audit_coverage.set_request_principal(
                audit_coverage.request_principal(request)
            )
            audit_coverage.note_workspace_header(request)
            forbidden = _tenant_bind_forbidden(request)
            if forbidden is not None:
                return forbidden

            return await call_next(request)
        finally:
            if principal_context is not None and principal_token is not None:
                principal_context.reset_request_principal(principal_token)
