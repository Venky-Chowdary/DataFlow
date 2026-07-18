from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from services.platform_config import docs_enabled

from ..services.auth_service import auth_required, lookup_user, verify_token

_PUBLIC_PREFIXES = (
    "/health",
    "/api/v1/auth/login",
    "/api/v1/auth/bootstrap",
    "/api/v1/auth/sso/providers",
    # MCP discovery + Streamable HTTP handshake (tools/call still checks auth in-handler)
    "/api/v1/mcp",
)

if docs_enabled():
    _PUBLIC_PREFIXES = _PUBLIC_PREFIXES + ("/docs", "/redoc", "/openapi.json")


def _is_public_sso_path(path: str) -> bool:
    return path.startswith("/api/v1/auth/sso/") and (
        path.endswith("/start") or path.endswith("/callback") or path.endswith("/providers")
    )


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
        request.state.user = {
            "email": request.state.user_email,
            "role": key_info.get("role") or "viewer",
        }
        request.state.api_key_id = key_info["id"]
        request.state.api_key_auth = True
        return True
    return False


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        auth = request.headers.get("Authorization", "")
        token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        # EventSource / SSE cannot send custom headers, so the token may be
        # passed as a query parameter.  Only allow this fallback on the SSE
        # stream paths to keep tokens out of general access logs.
        if not token and request.url.path.endswith("/stream"):
            token = request.query_params.get("token") or request.query_params.get("access_token") or ""

        if not auth_required():
            if token:
                _attach_user(request, token)
            return await call_next(request)

        path = request.url.path
        if (
            request.method == "OPTIONS"
            or path == "/"
            or any(path.startswith(p) for p in _PUBLIC_PREFIXES)
            or _is_public_sso_path(path)
        ):
            # Public routes still attach identity when a Bearer token is present
            # (MCP tools/call uses this; discovery works without a token).
            if token:
                _attach_user(request, token)
            return await call_next(request)

        if not token or not _attach_user(request, token):
            return JSONResponse(status_code=401, content={"detail": "Authentication required"})

        return await call_next(request)
