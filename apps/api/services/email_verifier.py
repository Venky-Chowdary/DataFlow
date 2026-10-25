"""Optional external email verifier (P2-9): Abstract Email Validation API.

OFF by default and fail-open: without ``DATAWRAP_EMAIL_VERIFIER_KEY`` (and
``DATAWRAP_EMAIL_VERIFIER=abstract``) ``configured()`` is False and callers
skip the lookup entirely. On timeout/HTTP error/parse failure the verdict is
``unknown`` — a verifier outage never blocks a real signup.
"""

from __future__ import annotations

import logging
import urllib.parse
from typing import Any

from services.brand_env import getenv_brand

logger = logging.getLogger(__name__)

_PROVIDER_URLS = {
    "abstract": "https://emailvalidation.abstractapi.com/v1/",
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
    # urlencode: the email goes into the query verbatim — a "+" or "&" in
    # the local part must not become a space or inject parameters.
    query = urllib.parse.urlencode({
        "api_key": getenv_brand("EMAIL_VERIFIER_KEY", ""),
        "email": email,
    })
    url = f"{_PROVIDER_URLS[provider]}?{query}"
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
            # The API key sits in the query string: log status only, never
            # the URL, the exception, or the response body.
            logger.warning(
                "email verifier returned %s", resp.status_code)
            return {"ok": False, "verdict": "unknown"}
        raw = (resp.json().get("deliverability") or "").lower()
    except Exception as exc:
        # Exception text (e.g. httpx errors) may embed the URL — log the
        # exception type only so the key can never reach logs.
        logger.warning(
            "email verifier request failed: %s", type(exc).__name__)
        return {"ok": False, "verdict": "unknown"}
    return {"ok": True, "verdict": _DELIVERABILITY_MAP.get(raw, "unknown")}
