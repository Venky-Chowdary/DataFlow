"""Optional GeoIP/ASN lookup adapter (P2-8).

OFF by default and fail-open: with no ``DATAWRAP_MAXMIND_LICENSE_KEY`` the
lookup returns ``{"ok": False}`` and never raises, so deployments without a
MaxMind license are unaffected. When enabled, the GeoIP2 Insights web API is
queried with a short timeout and results are cached briefly in-process.
"""

from __future__ import annotations

import time
from typing import Any

from services.brand_env import getenv_brand

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SEC = 3600


def license_key() -> str:
    return getenv_brand("MAXMIND_LICENSE_KEY", "") or ""


def enabled() -> bool:
    return bool(license_key()) and (getenv_brand("GEOIP_ENABLED", "0") == "1")


def lookup(ip: str, *, http_get=None) -> dict[str, Any]:
    """``{"ok", "country", "is_anonymous", "is_tor", "is_vpn", "asn"}``.

    ``ok`` is False whenever the adapter is disabled, the IP is
    private/reserved, or the request fails — callers must treat that as
    "no signal", never as a block.
    """
    if not enabled() or not ip:
        return {"ok": False}
    cached = _CACHE.get(ip)
    if cached and time.time() - cached[0] < _CACHE_TTL_SEC:
        return cached[1]
    if http_get is None:
        try:
            import httpx  # local import so the dep stays optional at import time
        except ImportError:
            return {"ok": False}

        def http_get(url, **kw):  # noqa: ANN001
            return httpx.get(url, timeout=3.0, **kw)
    try:
        resp = http_get(f"https://geoip.maxmind.com/geoip/v2.1/insights/{ip}",
                        auth=(license_key(), ""))
        if resp.status_code != 200:
            return {"ok": False}
        data = resp.json()
    except Exception:
        return {"ok": False}
    traits = data.get("traits", {}) or {}
    result = {
        "ok": True,
        "country": (data.get("country", {}) or {}).get("iso_code", ""),
        "is_anonymous": bool(traits.get("is_anonymous") or traits.get("is_anonymous_proxy")),
        "is_tor": bool(traits.get("is_tor_exit_node")),
        "is_vpn": bool(traits.get("is_anonymous_vpn") or traits.get("is_hosting_provider")),
        "asn": traits.get("autonomous_system_number"),
    }
    _CACHE[ip] = (time.time(), result)
    return result


def _reset_for_tests() -> None:
    _CACHE.clear()
