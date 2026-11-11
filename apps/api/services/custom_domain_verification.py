"""DNS ownership proof for a tenant custom domain (D337).

A saved ``custom_domain`` is only a claim: any workspace admin can type any
host name. Before that host becomes a credentialed browser origin
(services.cors_policy) the tenant proves it controls the domain's DNS:

1. Saving a domain issues a random challenge token (services.tenant_store).
2. The admin publishes a TXT record::

       _datawrap-challenge.<domain>  TXT  "datawrap-domain-verification=<token>"

3. ``POST /workspace/tenant/{id}/custom-domain/verify`` looks the record up
   (bounded by ``_TIMEOUT_SEC``) and, only when the token matches, marks the
   domain verified. Changing the domain clears the proof.

Fails closed: no dnspython, a timeout, NXDOMAIN or a wrong value all leave the
domain unverified, and an unverified domain gets no CORS headers.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from services.brand import PRODUCT_SLUG

logger = logging.getLogger(__name__)

_TIMEOUT_SEC = 3.0
RECORD_PREFIX = f"_{PRODUCT_SLUG}-challenge"
VALUE_PREFIX = f"{PRODUCT_SLUG}-domain-verification="


def record_name(domain: str) -> str:
    return f"{RECORD_PREFIX}.{domain}"


def expected_value(token: str) -> str:
    return f"{VALUE_PREFIX}{token}"


def challenge(tenant: Any) -> dict[str, Any]:
    """What the admin must publish, and whether the domain is proven."""
    domain = str(getattr(tenant, "custom_domain", "") or "")
    if not domain:
        return {"status": "not_configured", "verified": False}
    verified = bool(getattr(tenant, "custom_domain_verified", False))
    token = str(getattr(tenant, "custom_domain_challenge", "") or "")
    return {
        "status": "verified" if verified else "pending_dns_verification",
        "verified": verified,
        "verified_at": getattr(tenant, "custom_domain_verified_at", "") or None,
        "record_type": "TXT",
        "record_name": record_name(domain),
        "record_value": expected_value(token) if token else None,
    }


def lookup_txt(name: str) -> list[str]:
    """TXT strings at ``name``; ``[]`` when there are none or DNS cannot answer."""
    try:
        import dns.exception
        import dns.resolver
    except ImportError:
        logger.warning("dnspython is not installed: custom domains cannot be verified")
        return []
    resolver = dns.resolver.Resolver()
    resolver.timeout = _TIMEOUT_SEC
    resolver.lifetime = _TIMEOUT_SEC
    try:
        answers = resolver.resolve(name, "TXT")
    except (dns.exception.DNSException, OSError) as exc:
        logger.info("TXT lookup for %s failed: %s", name, type(exc).__name__)
        return []
    out: list[str] = []
    for rdata in answers:
        parts = getattr(rdata, "strings", ()) or ()
        out.append(b"".join(parts).decode("utf-8", "replace").strip())
    return out


@dataclass(frozen=True)
class Outcome:
    verified: bool
    reason: str
    tenant: Any = None


def verify(tenant_id: str) -> Outcome:
    """Look the challenge up and mark the domain verified when it matches."""
    from services.tenant_store import ensure_domain_challenge, mark_custom_domain_verified

    tenant = ensure_domain_challenge(tenant_id)
    if tenant is None:
        return Outcome(False, "tenant_not_found")
    domain = tenant.custom_domain
    if not domain:
        return Outcome(False, "no_custom_domain", tenant)
    want = expected_value(tenant.custom_domain_challenge)
    found = lookup_txt(record_name(domain))
    if want not in found:
        return Outcome(False, "txt_record_not_found" if not found else "txt_record_mismatch", tenant)
    marked = mark_custom_domain_verified(tenant.id, domain)
    if marked is None:
        return Outcome(False, "domain_changed_during_verification", tenant)
    return Outcome(True, "verified", marked)


__all__ = ["Outcome", "challenge", "expected_value", "lookup_txt", "record_name", "verify"]
