"""Have I Been Pwned password check (P4-3) — k-anonymity range API.

OFF by default (``DATAWRAP_HIBP_ENABLED=1`` to enable) and fail-open: a
HIBP outage must never block a signup or password reset. The lookup is
k-anonymous — only the first 5 hex chars of the SHA-1 go over the wire;
the password and its full hash never leave the process.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from typing import Any

from services.brand_env import getenv_brand

logger = logging.getLogger(__name__)

_RANGE_URL = "https://api.pwnedpasswords.com/range/"
_CACHE_TTL_SEC = 3600.0
_cache: dict[str, tuple[float, int]] = {}
_lock = threading.Lock()


def enabled() -> bool:
    return getenv_brand("HIBP_ENABLED", "0").lower() in ("1", "true", "yes")


def pwned_count(password: str, *, http_get=None, now=None) -> int:
    """Breached-occurrence count, or ``-1`` when the check can't run.

    Callers treat ``-1`` as 'unknown' and proceed — fail-open by design.
    """
    if not enabled():
        return -1
    # The HIBP range API is keyed by the SHA-1 of the password (k-anonymity):
    # the digest is a lookup key the service defines, not a security control.
    digest = hashlib.sha1(password.encode("utf-8"), usedforsecurity=False).hexdigest().upper()
    prefix, suffix = digest[:5], digest[5:]
    import time as _time

    ts = (_time.time() if now is None else now)
    with _lock:
        hit = _cache.get(prefix)
    if hit and ts - hit[0] < _CACHE_TTL_SEC:
        return hit[1]
    if http_get is None:
        try:
            import httpx
        except ImportError:
            return -1

        def http_get(u, **kw):  # noqa: ANN001
            return httpx.get(u, timeout=3.0, **kw)
    try:
        resp = http_get(f"{_RANGE_URL}{prefix}")
        if resp.status_code != 200:
            logger.warning("hibp range returned %s", resp.status_code)
            return -1
        count = 0
        for line in resp.text.splitlines():
            hash_suffix, _, n = line.partition(":")
            if hash_suffix.strip() == suffix:
                count = int(n)
                break
    except Exception as exc:
        # Never log the URL with context that could carry the digest;
        # the exception type alone is enough signal.
        logger.warning("hibp check failed: %s", type(exc).__name__)
        return -1
    with _lock:
        _cache[prefix] = (ts, count)
    return count


def _reset_for_tests() -> None:
    with _lock:
        _cache.clear()
