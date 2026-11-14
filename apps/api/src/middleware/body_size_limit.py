"""Request body size limit for non-upload bodies (SEC-8).

A JSON body was parsed in full however large it was: an unauthenticated
50 MB ``/auth/login`` cost about 20 MB of memory before its 422, and an
editor stored a 60 MB plan. Uploads (multipart and raw streams) keep their
own limits (``services.upload_limits``); every other body is refused with 413
once it crosses:

* ``DATAFLOW_MAX_PUBLIC_BODY_BYTES`` (1 MB) for a request that carries no
  credentials (no ``Authorization`` header, no session cookie) while sign-in
  is required;
* ``DATAFLOW_MAX_JSON_BODY_BYTES`` (8 MB) otherwise;
* ``DATAFLOW_BODY_LIMIT_OVERRIDES``: ``/api/v1/path/prefix=bytes`` pairs,
  comma-separated, for a route that needs more (longest prefix wins).

A declared ``Content-Length`` is judged before anything is read; a chunked
body is read up to the limit and refused there.
"""

from __future__ import annotations

import json
from typing import Any

from services.brand_env import getenv_brand

_UPLOAD_TYPES = ("multipart/form-data", "application/octet-stream")
_SESSION_COOKIES = ("__host-dw_session", "dw_session")


def _env_int(name: str, default: int) -> int:
    raw = (getenv_brand(name, "") or "").strip()
    try:
        return max(1024, int(raw)) if raw else default
    except ValueError:
        return default


def _overrides() -> list[tuple[str, int]]:
    raw = (getenv_brand("BODY_LIMIT_OVERRIDES", "") or "").strip()
    out: list[tuple[str, int]] = []
    for part in raw.split(","):
        prefix, _, size = part.strip().partition("=")
        if prefix.startswith("/") and size.strip().isdigit():
            out.append((prefix, int(size)))
    return sorted(out, key=lambda item: -len(item[0]))


def _header(scope: dict[str, Any], name: bytes) -> str:
    for key, value in scope.get("headers") or []:
        if key.lower() == name:
            return value.decode("latin-1")
    return ""


def _auth_required() -> bool:
    """With sign-in off every caller is the local operator, never "public"."""
    try:
        from src.services.auth_service import auth_required

        return bool(auth_required())
    except Exception:  # noqa: BLE001 - judge as public: the smaller limit
        return True


def body_limit(scope: dict[str, Any]) -> tuple[int, str] | None:
    """``(limit_bytes, setting)`` for this request, or None when it is not limited here."""
    content_type = _header(scope, b"content-type").lower()
    if content_type.startswith(_UPLOAD_TYPES):
        return None
    path = str(scope.get("path") or "")
    for prefix, size in _overrides():
        if path.startswith(prefix):
            return size, "DATAFLOW_BODY_LIMIT_OVERRIDES"
    cookie = _header(scope, b"cookie").lower()
    credentialed = bool(_header(scope, b"authorization")) or any(c in cookie for c in _SESSION_COOKIES)
    if credentialed or not _auth_required():
        return _env_int("MAX_JSON_BODY_BYTES", 8 * 1024 * 1024), "DATAFLOW_MAX_JSON_BODY_BYTES"
    return _env_int("MAX_PUBLIC_BODY_BYTES", 1024 * 1024), "DATAFLOW_MAX_PUBLIC_BODY_BYTES"


async def _refuse(send: Any, limit: int, setting: str) -> None:
    body = json.dumps({
        "error": "payload_too_large",
        "detail": f"The request body is larger than the {limit} byte limit.",
        "limit_bytes": limit,
        "setting": setting,
    }).encode()
    await send({"type": "http.response.start", "status": 413,
                "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode()),
                            (b"connection", b"close")]})
    await send({"type": "http.response.body", "body": body})


class BodySizeLimitMiddleware:
    """Pure ASGI: refuse an oversized non-upload body with 413 before the app parses it."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or scope.get("method") in ("GET", "HEAD", "OPTIONS"):
            await self.app(scope, receive, send)
            return
        limited = body_limit(scope)
        if limited is None:
            await self.app(scope, receive, send)
            return
        limit, setting = limited
        declared = _header(scope, b"content-length")
        if declared.strip().isdigit():
            if int(declared) > limit:
                await _refuse(send, limit, setting)
                return
            await self.app(scope, receive, send)
            return
        # No length declared (chunked): read up to the limit, then replay.
        chunks: list[dict[str, Any]] = []
        size = 0
        while True:
            message = await receive()
            chunks.append(message)
            if message.get("type") != "http.request":
                break
            size += len(message.get("body") or b"")
            if size > limit:
                await _refuse(send, limit, setting)
                return
            if not message.get("more_body"):
                break

        async def replay() -> dict[str, Any]:
            return chunks.pop(0) if chunks else await receive()

        await self.app(scope, replay, send)
