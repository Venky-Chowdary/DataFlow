"""Signup risk scorer (P2-8): allow / challenge / hold / deny.

Pure signal aggregation — every input is optional and a missing signal
contributes nothing (fail-open). Actions:

- ``allow``      (< 30)  normal path
- ``challenge``  (30–59) proceed; the score is persisted on the pending row
  for admin review, and Turnstile (when configured) is required by the
  request flow earlier.
- ``hold``       (60–84) pending row created with the score but NO OTP is
  mailed; an ``admin.signup_held`` event is recorded and the caller returns
  the decoy 202. A later login attempt re-issues a code.
- ``deny``       (≥ 85)  nothing stored, nothing mailed, decoy 202.
"""

from __future__ import annotations

import ipaddress
import re
from typing import Any

from services import geoip

HEADLESS_UA = re.compile(
    r"headless|phantomjs|selenium|puppeteer|playwright|curl/|wget/|"
    r"python-requests|httpx|scrapy|aiohttp|libwww|bot[ /]|crawler",
    re.IGNORECASE,
)


def score_signals(*, ip: str = "", ua: str = "", device_id: str = "",
                  turnstile_ok: bool | None = None,
                  geoip_result: dict[str, Any] | None = None,
                  extra_score: int = 0,
                  extra_reasons: list[str] | None = None) -> dict[str, Any]:
    """``{"score": int, "action": str, "reasons": [str]}``."""
    score = 0
    reasons: list[str] = []

    if turnstile_ok is None:
        score += 15
        reasons.append("no_turnstile")

    if ua:
        if HEADLESS_UA.search(ua):
            score += 35
            reasons.append("automation_ua")
    else:
        score += 10
        reasons.append("missing_ua")

    if not device_id:
        score += 5
        reasons.append("missing_device")

    if ip:
        try:
            addr = ipaddress.ip_address(ip)
            if addr.is_private or addr.is_loopback or addr.is_link_local:
                score -= 10
                reasons.append("internal_ip")
        except ValueError:
            pass
        geo = geoip_result if geoip_result is not None else geoip.lookup(ip)
        if geo.get("ok"):
            if geo.get("is_tor"):
                score += 55
                reasons.append("tor_exit")
            if geo.get("is_anonymous"):
                score += 40
                reasons.append("anonymous_proxy")
            if geo.get("is_vpn"):
                score += 25
                reasons.append("vpn_or_hosting")
    score += max(0, extra_score)
    reasons.extend(extra_reasons or [])
    score = max(0, min(100, score))
    if score >= 85:
        action = "deny"
    elif score >= 60:
        action = "hold"
    elif score >= 30:
        action = "challenge"
    else:
        action = "allow"
    return {"score": score, "action": action, "reasons": reasons}
