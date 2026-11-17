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
_PUBLIC_HOSTNAMES = {"datawrap.io", "www.datawrap.io"}
_LOCAL_HOSTNAMES = {"localhost", "127.0.0.1", "::1"}


def signup_expected_hostname(request_host: str) -> str:
    """The Host header to pin, or empty when it is not a served public host.

    Signup's test client sends ``testserver``. Pinning that would reject a
    token Cloudflare minted for www.datawrap.io. A public host is pinned so
    the apex and www check is live on the signup route.
    """
    host = _host_token(request_host)
    return host if host in _PUBLIC_HOSTNAMES else ""


def production_turnstile() -> bool:
    """True when this process must enforce production Turnstile policy.

    ``platform_config.is_production`` is fail-closed: an unknown environment
    name does not become development, and a deployed host does not stay open
    because ``ENV`` was left unset (D322).
    """
    from services.platform_config import is_production

    return is_production()


def _host_token(value: str) -> str:
    host = (value or "").strip().lower().rstrip(".")
    if host.startswith("[") and "]" in host:
        host = host[1 : host.index("]")]
    else:
        host = host.split(":")[0]
    return host


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
    hostname = _host_token(str(data.get("hostname") or ""))
    action = str(data.get("action") or "")
    expected = _host_token(expected_hostname)
    served = _PUBLIC_HOSTNAMES | (set() if production_turnstile() else _LOCAL_HOSTNAMES)
    # Apex and www are one site. A token minted on www.datawrap.io matches a
    # caller that names the apex, and the reverse, because both are served.
    if not hostname or hostname not in served:
        reason = "empty_hostname" if not hostname else "hostname_not_allowed"
        logger.info(
            "turnstile hostname refused",
            extra={"reason_code": f"turnstile:{reason}"},
        )
        return {"ok": False, "reason": reason, "hostname": hostname}
    if expected and hostname != expected and not (
        hostname in _PUBLIC_HOSTNAMES and expected in _PUBLIC_HOSTNAMES
    ):
        logger.info(
            "turnstile hostname refused",
            extra={"reason_code": "turnstile:hostname_mismatch"},
        )
        return {"ok": False, "reason": "hostname_mismatch", "hostname": hostname}
    if expected_action and action != expected_action:
        return {"ok": False, "reason": "action_mismatch", "action": action}
    return {"ok": True, "reason": "ok", "hostname": hostname, "action": action}
