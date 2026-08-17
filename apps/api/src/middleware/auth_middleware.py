from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from services.platform_config import docs_enabled
from services.tenant_bind import principal_allowed_for_tenant

from ..services import auth_service as _auth_service
from ..services.auth_service import lookup_user, verify_token

_PUBLIC_PREFIXES = (
    "/health",
    "/api/v1/health",
    "/api/v1/auth/login",
    "/api/v1/auth/logout",
    "/api/v1/auth/bootstrap",
    "/api/v1/auth/sso/providers",
    # Alias paths when the web client omits /api/v1 (mis-set VITE_API_BASE).
    "/auth/login",
    "/auth/logout",
    "/auth/bootstrap",
    "/auth/sso/providers",
    # Marketing / docs / landing need catalog stats without a session.
    "/api/v1/catalog",
    "/catalog",
)

if docs_enabled():
    _PUBLIC_PREFIXES = _PUBLIC_PREFIXES + ("/docs", "/redoc", "/openapi.json")


def _is_public_sso_path(path: str) -> bool:
    if path.startswith("/api/v1/auth/sso/") or path.startswith("/auth/sso/"):
        return path.endswith("/start") or path.endswith("/callback") or path.endswith("/providers")
    return False


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


def _attach_user(request: Request, token: str) -> bool:
    email = verify_token(token)
    if email:
        request.state.user_email = email
        user = lookup_user(email)
        if user:
            request.state.user = user
        else:
            # Valid token for a user not in the static user list: default to viewer.
            request.state.user = {"email": email, "role": "viewer"}
        return True

    from services.integrations_store import verify_workspace_api_key

    key_info = verify_workspace_api_key(token)
    if key_info:
        request.state.user_email = key_info.get("created_by") or "api-key"
        user = {
            "email": request.state.user_email,
            "role": key_info.get("role") or "viewer",
        }
        # Workspace API keys may carry tenant binding (same claims as users).
        if key_info.get("tenant_id"):
            user["tenant_id"] = key_info["tenant_id"]
        if key_info.get("tenant_ids"):
            user["tenant_ids"] = key_info["tenant_ids"]
        request.state.user = user
        request.state.api_key_id = key_info["id"]
        request.state.api_key_auth = True
        return True
    return False


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
        auth = request.headers.get("Authorization", "")
        token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        # EventSource / SSE cannot send custom headers, so the token may be
        # passed as a query parameter.  Only allow this fallback on the SSE
        # stream paths to keep tokens out of general access logs.
        if not token and request.url.path.endswith("/stream"):
            token = request.query_params.get("token") or request.query_params.get("access_token") or ""

        # Resolved on the module, not copied at import: enforcement is a live
        # setting, and a stale copy means the middleware and the auth service
        # disagree about whether the request needs a token.
        if not _auth_service.auth_required():
            if token:
                _attach_user(request, token)
                forbidden = _tenant_bind_forbidden(request)
                if forbidden is not None:
                    return forbidden
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
            if token:
                _attach_user(request, token)
                forbidden = _tenant_bind_forbidden(request)
                if forbidden is not None:
                    return forbidden
            return await call_next(request)

        if not token or not _attach_user(request, token):
            return JSONResponse(status_code=401, content={"detail": "Authentication required"})

        forbidden = _tenant_bind_forbidden(request)
        if forbidden is not None:
            return forbidden

        return await call_next(request)
