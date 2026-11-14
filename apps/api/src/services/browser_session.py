"""Hand a freshly opened server session to the browser.

Shared by every flow that signs someone in without the login form — a
verified signup, an accepted workspace invitation — so the cookies and the
Bearer token are issued exactly as ``/auth/login`` would.
"""

from __future__ import annotations

from typing import Any

from fastapi import Response
from services.safe_errors import safe_http_exception


def issue_browser_session(response: Response, session: dict[str, Any], email: str) -> dict[str, Any]:
    """Set the ``__Host-dw_session`` + ``dw_csrf`` cookies for ``session`` and
    return the Bearer payload (``token``, ``expires_at``) the web client
    stores — the cookie alone left it holding an empty token (D153)."""
    max_age = int(session["absolute_expires_at"] - session["created_at"])
    response.set_cookie(
        "__Host-dw_session", session["jti"], max_age=max_age,
        httponly=True, secure=True, samesite="lax", path="/")
    # Double-submit CSRF cookie — JS echoes it back as x-csrf-token on
    # mutating cookie-authed requests (enforced by AuthMiddleware).
    response.set_cookie(
        "dw_csrf", session["csrf"], max_age=max_age,
        httponly=False, secure=True, samesite="strict", path="/")
    from src.services.auth_service import create_token

    try:
        # One listed session (D338): the Bearer row records the cookie's jti so
        # revoking it signs the cookie out too.
        token, expires_at = create_token(
            email, ip=str(session.get("ip") or ""), ua=str(session.get("ua") or ""),
            device_id=str(session.get("device_id") or ""), cookie_jti=str(session["jti"]))
    except RuntimeError as exc:
        raise safe_http_exception(
            exc, event="auth.browser_session.token_unavailable",
            public_message="Sign-in is temporarily unavailable.", status_code=503,
        ) from exc
    return {"token": token, "expires_at": expires_at, "must_change_password": False}
