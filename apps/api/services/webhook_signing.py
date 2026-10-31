"""HMAC-SHA256 signatures on outgoing webhook notifications (QA enterprise #46).

Every webhook channel gets its own signing secret (``whsec_...``), shown once
when the channel is created and again only when it is rotated. Each delivery
carries::

    X-Datawrap-Timestamp: <unix seconds>
    X-Datawrap-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256>

where the HMAC key is the signing secret and the signed message is
``"<timestamp>.<raw request body>"``. A receiver recomputes it over the raw
bytes it received, compares in constant time, and rejects a timestamp more
than five minutes old (replay). See ``docs/WEBHOOK_SIGNING.md``.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time

SIGNATURE_HEADER = "X-Datawrap-Signature"
TIMESTAMP_HEADER = "X-Datawrap-Timestamp"
SECRET_PREFIX = "whsec_"
SECRET_CONFIG_KEY = "signing_secret"
DEFAULT_TOLERANCE_SEC = 300


def new_signing_secret() -> str:
    return SECRET_PREFIX + secrets.token_urlsafe(32)


def signature(secret: str, timestamp: int, body: bytes) -> str:
    message = str(int(timestamp)).encode("ascii") + b"." + body
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def signature_headers(secret: str, body: bytes, *, now: int | None = None) -> dict[str, str]:
    ts = int(now if now is not None else time.time())
    return {
        TIMESTAMP_HEADER: str(ts),
        SIGNATURE_HEADER: f"t={ts},v1={signature(secret, ts, body)}",
    }


def verify(
    secret: str,
    body: bytes,
    header: str,
    *,
    tolerance_sec: int = DEFAULT_TOLERANCE_SEC,
    now: int | None = None,
) -> bool:
    """Reference verifier (the receiver's side), used by the docs and tests."""
    parts = dict(item.split("=", 1) for item in (header or "").split(",") if "=" in item)
    try:
        ts = int(parts.get("t", ""))
    except ValueError:
        return False
    current = int(now if now is not None else time.time())
    if abs(current - ts) > tolerance_sec:
        return False
    expected = signature(secret, ts, body)
    return hmac.compare_digest(expected, parts.get("v1", ""))
