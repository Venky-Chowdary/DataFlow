"""Cloudflare Turnstile verification (P2-5).

Server-side siteverify against Cloudflare. Checks ``success``, the
``hostname`` (datawrap.io only) and the ``action`` label so a token
minted for another page/action is refused. Cloudflare refuses replays
itself — a reused token comes back ``success=false`` — and we refuse
it too.

Behaviour without ``DATAWRAP_TURNSTILE_SECRET``: unconfigured —
``verify()`` refuses every token. Signup treats "turnstile not
configured" as a challenge-level risk signal rather than a hard wall so
the flow can still ship in environments without keys; callers decide
strictness.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from services.brand_env import getenv_brand

logger = logging.getLogger(__name__)

_VERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
_TIMEOUT_SEC = 5.0
_ALLOWED_HOSTNAMES = {"datawrap.io", "www.datawrap.io", "localhost"}


def secret() -> str:
    return getenv_brand("TURNSTILE_SECRET", "")


def configured() -> bool:
    return bool(secret())


def verify(token: str, *, remote_ip: str = "", expected_action: str = "",
           expected_hostname: str = "",
           post_fn=None) -> dict[str, Any]:
    """``{"ok": bool, "reason": str}`` — ok=True only on a clean verdict."""
    token = (token or "").strip()
    if not token:
        return {"ok": False, "reason": "missing_token"}
    sec = secret()
    if not sec:
        return {"ok": False, "reason": "not_configured"}
    try:
        if post_fn is None:
            resp = httpx.post(
                _VERIFY_URL,
                data={"secret": sec, "response": token, "remoteip": remote_ip},
                timeout=_TIMEOUT_SEC,
            )
            data = resp.json()
        else:
            data = post_fn(secret="*redacted*", token=token, remote_ip=remote_ip)
    except Exception as exc:
        logger.warning("turnstile siteverify failed: %s", type(exc).__name__)
        return {"ok": False, "reason": "verify_unreachable"}
    if not data.get("success"):
        return {"ok": False, "reason": "rejected",
                "codes": data.get("error-codes", [])}
    hostname = str(data.get("hostname") or "").lower()
    action = str(data.get("action") or "")
    if expected_hostname and hostname != expected_hostname.lower():
        return {"ok": False, "reason": "hostname_mismatch", "hostname": hostname}
    if hostname and hostname not in _ALLOWED_HOSTNAMES:
        return {"ok": False, "reason": "hostname_not_allowed", "hostname": hostname}
    if expected_action and action != expected_action:
        return {"ok": False, "reason": "action_mismatch", "action": action}
    return {"ok": True, "reason": "ok", "hostname": hostname, "action": action}
