"""Svix-signed webhook verification (P2-10).

Resend signs webhooks with Svix: ``svix-id`` + ``svix-timestamp`` +
``svix-signature`` (space-separated ``v1,<base64-hmac>`` entries) over
``{id}.{ts}.{raw_body}`` using the base64url secret from
``DATAWRAP_RESEND_WEBHOOK_SECRET`` (``whsec_`` prefix). An unsigned, forged
or stale (±5 min) payload is rejected.
"""

from __future__ import annotations

import base64
import hmac
import hashlib
import time
from typing import Any

from services.brand_env import getenv_brand

TOLERANCE_SEC = 300


def secret() -> str:
    return getenv_brand("RESEND_WEBHOOK_SECRET", "") or ""


def configured() -> bool:
    return bool(secret())


def _key_bytes() -> bytes:
    raw = secret()
    if raw.startswith("whsec_"):
        raw = raw[len("whsec_"):]
    try:
        return base64.b64decode(raw + "=" * (-len(raw) % 4))
    except Exception:
        return raw.encode()


def verify(body: bytes, headers: Any, *, now: float | None = None) -> bool:
    """True only when a fresh, correctly-signed Svix payload arrives."""
    if not configured():
        return False
    get = (headers.get if hasattr(headers, "get") else {})
    msg_id = get("svix-id") or ""
    ts_raw = get("svix-timestamp") or ""
    sigs = get("svix-signature") or ""
    if not (msg_id and ts_raw and sigs):
        return False
    try:
        ts = int(ts_raw)
    except ValueError:
        return False
    now = time.time() if now is None else now
    if abs(now - ts) > TOLERANCE_SEC:
        return False
    signed = f"{msg_id}.{ts}.".encode() + body
    expected = hmac.new(_key_bytes(), signed, hashlib.sha256).digest()
    for entry in sigs.split(" "):
        scheme, _, sig_b64 = entry.partition(",")
        if scheme != "v1" or not sig_b64:
            continue
        try:
            sig = base64.b64decode(sig_b64)
        except Exception:
            continue
        if hmac.compare_digest(sig, expected):
            return True
    return False
