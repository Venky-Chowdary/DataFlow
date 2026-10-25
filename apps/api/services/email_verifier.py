"""Optional external email verifier (P2-9): Abstract Email Validation API.

OFF by default and fail-open: without ``DATAWRAP_EMAIL_VERIFIER_KEY`` (and
``DATAWRAP_EMAIL_VERIFIER=abstract``) ``configured()`` is False and callers
skip the lookup entirely. On timeout/HTTP error/parse failure the verdict is
``unknown`` — a verifier outage never blocks a real signup.
"""

from __future__ import annotations

from typing import Any

from services.brand_env import getenv_brand

_PROVIDER_URLS = {
    "abstract": "https://emailvalidation.abstractapi.com/v1/?api_key={key}&email={email}",
}

_DELIVERABILITY_MAP = {
    "deliverable": "deliverable",
    "risky": "risky",
    "undeliverable": "undeliverable",
}


def configured() -> bool:
    return (
        (getenv_brand("EMAIL_VERIFIER", "") or "").lower() in _PROVIDER_URLS
        and bool(getenv_brand("EMAIL_VERIFIER_KEY", ""))
    )


def verify(email: str, *, http_get=None) -> dict[str, Any]:
    """``{"ok": bool, "verdict": "deliverable"|"risky"|"undeliverable"|"unknown"}``."""
    if not configured():
        return {"ok": False, "verdict": "unknown"}
    provider = (getenv_brand("EMAIL_VERIFIER", "") or "").lower()
    url = _PROVIDER_URLS[provider].format(
        key=getenv_brand("EMAIL_VERIFIER_KEY", ""), email=email)
    if http_get is None:
        try:
            import httpx
        except ImportError:
            return {"ok": False, "verdict": "unknown"}

        def http_get(u, **kw):  # noqa: ANN001
            return httpx.get(u, timeout=4.0, **kw)
    try:
        resp = http_get(url)
        if resp.status_code != 200:
            return {"ok": False, "verdict": "unknown"}
        raw = (resp.json().get("deliverability") or "").lower()
    except Exception:
        return {"ok": False, "verdict": "unknown"}
    return {"ok": True, "verdict": _DELIVERABILITY_MAP.get(raw, "unknown")}
