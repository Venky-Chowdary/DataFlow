"""Refuse traffic when production config is incomplete.

Liveness stays open so a deploy probe can read the reason. Every other
route returns 503. The process does not exit: exiting before the socket
accepts connections makes Railway report "service unavailable" for the
whole healthcheck window.
"""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from services.platform_config import is_liveness_path


class ProductionConfigMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        errors = list(getattr(request.app.state, "config_errors", None) or [])
        if not errors or is_liveness_path(request.url.path):
            return await call_next(request)
        return JSONResponse(
            status_code=503,
            content={
                "detail": "API is up but production configuration is incomplete",
                "config_errors": errors,
            },
        )
