"""Have I Been Pwned password check (P4-3) — k-anonymity range API.

ON by default in production, off elsewhere (dev, tests, CI make no outbound
call); ``DATAWRAP_HIBP_ENABLED=1``/``0`` forces it either way, e.g. ``0`` on an
air-gapped deployment (D340). Fail-open: the call has a 3 s timeout, and a
HIBP outage, timeout or non-200 answers ``-1`` (unknown) so it never blocks a
signup, invite, reset or password change — the bundled offline list
(``services.breached_passwords``) still refuses the most common breached
passwords then. The lookup is k-anonymous — only the first 5 hex chars of the
SHA-1 go over the wire; the password and its full hash never leave the
process, and only the public range answer is cached.
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
_CACHE_MAX_PREFIXES = 4096
_TIMEOUT_SEC = 3.0
#: prefix -> (fetched_at, {suffix: count}) — the public range answer, never a user's digest.
_cache: dict[str, tuple[float, dict[str, int]]] = {}
_lock = threading.Lock()


def enabled() -> bool:
    raw = (getenv_brand("HIBP_ENABLED", "") or "").strip().lower()
    if raw in ("1", "true", "yes"):
        return True
    if raw in ("0", "false", "no"):
        return False
    from services.platform_config import is_production

    return is_production()


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
        # The range is cached, not this password's count: another password
        # sharing the 5-char prefix must be looked up by its own suffix.
        return hit[1].get(suffix, 0)
    if http_get is None:
        try:
            import httpx
        except ImportError:
            return -1

        def http_get(u, **kw):  # noqa: ANN001
            return httpx.get(u, timeout=_TIMEOUT_SEC, **kw)
    try:
        resp = http_get(f"{_RANGE_URL}{prefix}")
        if resp.status_code != 200:
            logger.warning("hibp range returned %s", resp.status_code)
            return -1
        counts: dict[str, int] = {}
        for line in resp.text.splitlines():
            hash_suffix, _, n = line.partition(":")
            if hash_suffix.strip() and n.strip().isdigit():
                counts[hash_suffix.strip().upper()] = int(n)
    except Exception as exc:
        # Never log the URL with context that could carry the digest;
        # the exception type alone is enough signal.
        logger.warning("hibp check failed: %s", type(exc).__name__)
        return -1
    with _lock:
        if len(_cache) >= _CACHE_MAX_PREFIXES:
            _cache.clear()
        _cache[prefix] = (ts, counts)
    return counts.get(suffix, 0)


def _reset_for_tests() -> None:
    with _lock:
        _cache.clear()
