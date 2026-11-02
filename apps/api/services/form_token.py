"""Signed form-start token + honeypot policy (P2-7).

``issue()`` returns an HMAC-signed token: ``{purpose}:{issued_ms}:{nonce}:{sig}``
signed with AUTH_SECRET. ``check()`` enforces a 3s minimum fill time —
posts faster than a human can type (or with the honeypot filled) fail
the same way: the caller returns a fake 202 and sends nothing.

Tamper/expiry/refill all refuse; tokens are one-use via the shared
auth_state nonce set (TTL = token lifetime).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import time
from typing import Any

from services.auth_state import get_auth_state
from services.brand_env import getenv_brand

MIN_FILL_MS = 3000
TOKEN_TTL_SEC = 3600


def _sign(payload: str) -> str:
    secret = getenv_brand("AUTH_SECRET", "dev-change-me-before-production")
    return hmac.new(secret.encode(), payload.encode(),
                    hashlib.sha256).hexdigest()[:32]


def issue(purpose: str) -> str:
    nonce = secrets.token_urlsafe(8)
    payload = f"{purpose}:{int(time.time() * 1000)}:{nonce}"
    return f"{payload}.{_sign(payload)}"


def check(token: str, *, purpose: str, honeypot: str = "",
          min_fill_ms: int | None = None) -> dict[str, Any]:
    """``{"ok": bool, "reason": str}`` — caller decides fake-202 vs error."""
    if honeypot.strip():
        return {"ok": False, "reason": "honeypot"}
    parts = (token or "").split(".")
    if len(parts) != 2:
        return {"ok": False, "reason": "missing_token"}
    payload, sig = parts
    if not hmac.compare_digest(_sign(payload), sig):
        return {"ok": False, "reason": "bad_signature"}
    try:
        purp, issued_ms, nonce = payload.split(":", 2)
        issued = int(issued_ms)
    except ValueError:
        return {"ok": False, "reason": "malformed"}
    if purp != purpose:
        return {"ok": False, "reason": "wrong_purpose"}
    floor = MIN_FILL_MS if min_fill_ms is None else min_fill_ms
    age_ms = int(time.time() * 1000) - issued
    if age_ms < floor:
        return {"ok": False, "reason": "too_fast"}
    if age_ms > TOKEN_TTL_SEC * 1000:
        return {"ok": False, "reason": "expired"}
    # One use: nonce single-claim via the shared counter.
    allowed, _ = get_auth_state().rate_limit_hit(
        f"form-token:{nonce}", limit=1, window_sec=TOKEN_TTL_SEC)
    if not allowed:
        return {"ok": False, "reason": "replayed"}
    return {"ok": True, "reason": "ok"}


def fake_accept_body() -> dict[str, Any]:
    """The shape a real 202 returns — sent when the bot checks fail so the
    post looks accepted but nothing is stored or mailed."""
    return {"challenge_id": f"otc_{secrets.token_hex(16)}",
            "resend_after_sec": 60, "expires_in_sec": 600}
