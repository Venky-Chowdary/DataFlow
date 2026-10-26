"""MX/DNS check (P2-3).

A signup domain must be able to receive mail: MX records, or an A-record
fallback per RFC 5321. Refuses RFC 7505 null MX (``0 .``), missing
MX+A, and nonexistent domains. MX hosts are matched against the
denylist's ``mx_host`` kind (disposable mail servers).

Results cached 24h keyed on the domain; lookup timeout 2s; DNS failure
treats the domain as unverifiable → refuse (the OTP is the final proof,
but a domain that can't answer DNS can't receive it). dnspython is
optional — when absent the check is skipped and only the denylist runs.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from services import denylist_store

logger = logging.getLogger(__name__)

_TIMEOUT_SEC = 2.0
_CACHE_TTL_SEC = 24 * 3600

_cache: dict[str, tuple[float, dict[str, Any]]] = {}

try:
    import dns.exception
    import dns.resolver

    _HAVE_DNS = True
except ImportError:
    _HAVE_DNS = False


def _resolver():
    res = dns.resolver.Resolver()
    res.timeout = _TIMEOUT_SEC
    res.lifetime = _TIMEOUT_SEC
    return res


def check_domain(domain: str) -> dict[str, Any]:
    """``{"ok": bool, "reason": str, "mx": [...]}`` — ok=False means refuse."""
    domain = (domain or "").strip().lower().rstrip(".")
    if not domain:
        return {"ok": False, "reason": "empty_domain", "mx": []}
    cached = _cache.get(domain)
    if cached and cached[0] > time.time():
        return dict(cached[1])
    result = _lookup(domain)
    _cache[domain] = (time.time() + _CACHE_TTL_SEC, dict(result))
    return result


def _lookup(domain: str) -> dict[str, Any]:
    if not _HAVE_DNS:
        return {"ok": True, "reason": "dns_unavailable", "mx": []}
    try:
        answers = _resolver().resolve(domain, "MX")
        hosts = sorted(str(a.exchange).rstrip(".").lower() for a in answers)
        if hosts == [""]:
            # RFC 7505: "MX 0 ." — the domain declares it never accepts mail.
            return {"ok": False, "reason": "null_mx", "mx": []}
        for host in hosts:
            if denylist_store.is_denied(host, kind="mx_host"):
                return {"ok": False, "reason": "mx_host_denied", "mx": hosts}
        return {"ok": True, "reason": "mx", "mx": hosts}
    except dns.resolver.NXDOMAIN:
        return {"ok": False, "reason": "domain_nxdomain", "mx": []}
    except dns.resolver.NoAnswer:
        pass
    except (dns.exception.DNSException, OSError) as exc:
        return {"ok": False, "reason": f"dns_error:{type(exc).__name__}", "mx": []}
    # No MX — RFC 5321 implicit-MX fallback: an A record can receive mail.
    try:
        _resolver().resolve(domain, "A")
        return {"ok": True, "reason": "a_fallback", "mx": []}
    except dns.resolver.NXDOMAIN:
        return {"ok": False, "reason": "domain_nxdomain", "mx": []}
    except (dns.resolver.NoAnswer, dns.resolver.NoNameservers):
        return {"ok": False, "reason": "no_mx_no_a", "mx": []}
    except (dns.exception.DNSException, OSError) as exc:
        return {"ok": False, "reason": f"dns_error:{type(exc).__name__}", "mx": []}


def _reset_for_tests() -> None:
    _cache.clear()
