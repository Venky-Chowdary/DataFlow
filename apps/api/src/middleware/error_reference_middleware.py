"""Every REST error body carries ``error.reference`` (U2), whoever wrote it.

The exception handlers in ``src.main`` put the platform envelope
(``{"error": {code, message, reference, ...}, "detail": ...}``) on every
raised error. Responses written directly by a middleware never reach them:
the authentication 401 (``{"detail": "Authentication required"}``), the CSRF
and tenant 403s, the 413 body limit, the 503 configuration and tenant-store
refusals. Support could not trace those, and a 401 for most routes carried
no ``WWW-Authenticate`` challenge (RFC 9110 15.5.2 requires one).

This outer ASGI layer completes a JSON error body that lacks a reference:

- a body with no ``error`` gets the envelope for its status, and keeps every
  key it had (``detail`` keeps its shape);
- an ``error`` object without a reference gets one;
- a string ``error`` is a code existing clients read (``duplicate_transfer``,
  ``plan_store_busy``, ``rate_limited`` ...), so it stays a string and the
  reference is added beside it as ``reference`` (the 500 body already has it
  as ``correlation_id``).

The reference is the request's correlation id when the response names one
(the timing middleware sets ``X-Correlation-ID``), so the log line and the
body agree. Bodies are buffered only for JSON responses with status >= 400.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

_CHALLENGE = b'Bearer realm="datawrap"'


def complete_error_body(body: Any, status: int, reference: str) -> Any:
    """``body`` with a reference, or ``body`` unchanged when it already has one."""
    if not isinstance(body, dict):
        return body
    error = body.get("error")
    if isinstance(error, dict):
        if error.get("reference"):
            return body
        return {**body, "error": {**error, "reference": reference}}
    if error is not None:
        # The 500 body already names its reference as correlation_id (pinned
        # byte for byte by test_safe_errors_r3): left as it is.
        if body.get("reference") or body.get("correlation_id"):
            return body
        return {**body, "reference": reference}
    from services.safe_errors import _platform_envelope

    raw = body.get("detail")
    if isinstance(raw, str) and raw:
        message = raw
    elif isinstance(raw, dict):
        message = str(raw.get("message") or raw.get("detail") or "Request refused")
    else:
        message = "Request refused" if status < 500 else "Something went wrong"
    envelope = _platform_envelope(message, reference, message, status)
    return {**body, "error": envelope["error"], "detail": raw if "detail" in body else message}


class ErrorReferenceMiddleware:
    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        start: dict[str, Any] = {}
        chunks: list[bytes] = []

        async def _send(message: dict) -> None:
            if message["type"] == "http.response.start":
                status = int(message.get("status") or 200)
                headers = dict((k.lower(), v) for k, v in message.get("headers") or [])
                is_json = headers.get(b"content-type", b"").startswith(b"application/json")
                if status >= 400 and is_json:
                    start.update(message)
                    return
                await send(message)
                return
            if message["type"] == "http.response.body" and start:
                chunks.append(message.get("body") or b"")
                if message.get("more_body"):
                    return
                await _flush(b"".join(chunks))
                return
            await send(message)

        async def _flush(raw: bytes) -> None:
            headers = [(k, v) for k, v in start.get("headers") or [] if k.lower() != b"content-length"]
            names = {k.lower(): v for k, v in headers}
            status = int(start.get("status") or 500)
            reference = (names.get(b"x-correlation-id") or b"").decode("latin-1") or uuid.uuid4().hex
            try:
                body = json.loads(raw or b"null")
            except ValueError:
                body = None
            completed = complete_error_body(body, status, reference)
            if completed is not body:
                raw = json.dumps(completed, separators=(",", ":")).encode()
                if b"x-correlation-id" not in names:
                    headers.append((b"x-correlation-id", reference.encode("latin-1")))
            if status == 401 and b"www-authenticate" not in names:
                headers.append((b"www-authenticate", _CHALLENGE))
            headers.append((b"content-length", str(len(raw)).encode()))
            await send({**start, "headers": headers})
            await send({"type": "http.response.body", "body": raw, "more_body": False})

        await self.app(scope, receive, _send)


__all__ = ["ErrorReferenceMiddleware", "complete_error_body"]
