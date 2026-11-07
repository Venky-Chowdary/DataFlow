"""Uniform error envelope for the signup/auth surface (P0-5).

New routes raise :class:`ApiError`; the app-level handler renders
``{"error": {"code": ..., "message": ..., "retry_after_sec": N}}`` and sets
``Retry-After`` when a wait is advised. Legacy routes keep their
``{detail: ...}`` shape — see the handover reply (the envelope applies to
the new surface only).
"""

from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse


class ApiError(Exception):
    """An error the client renders as {"error": {...}}."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        retry_after_sec: int | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = int(status_code)
        self.code = code
        self.message = message
        self.retry_after_sec = retry_after_sec
        self.extra = extra or {}


async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
    error: dict[str, Any] = {"code": exc.code, "message": exc.message}
    if exc.retry_after_sec is not None:
        error["retry_after_sec"] = exc.retry_after_sec
    error.update(exc.extra)
    headers = {}
    if exc.retry_after_sec is not None:
        headers["Retry-After"] = str(exc.retry_after_sec)
    return JSONResponse(status_code=exc.status_code, content={"error": error}, headers=headers)


def raise_error(status_code: int, code: str, message: str, **kw: Any) -> None:
    raise ApiError(status_code, code, message, **kw)
