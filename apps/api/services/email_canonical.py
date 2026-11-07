"""Canonical email identity (P1-1 minimal; P2-1 extends).

Folds the aliases that all reach one inbox so "already signed up" is
detected honestly:

- case fold: ``Ada@Example.COM`` -> ``ada@example.com``
- Gmail: dots in the local part and ``+tags`` are ignored
  (``j.doe+x@gmail.com`` == ``jdoe@gmail.com``; googlemail.com aliases gmail.com)
- ``+tag`` fold on the other providers that advertise sub-addressing.

The provider table is data — P2-1 extends it and adds per-provider dot
rules. Local-part only folding; the domain is never rewritten (except the
googlemail -> gmail alias).
"""

from __future__ import annotations

# Providers supporting plus-addressing whose +tag does not change the inbox.
_PLUS_TAG_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "yahoo.com",
    "icloud.com",
    "me.com",
    "mac.com",
    "proton.me",
    "protonmail.com",
    "pm.me",
    "fastmail.com",
    "zoho.com",
}

# Providers where dots in the local part are ignored entirely.
_DOTLESS_DOMAINS = {"gmail.com", "googlemail.com"}

_DOMAIN_ALIASES = {"googlemail.com": "gmail.com"}


def canonical_email(email: str) -> str:
    """The dedupe key for signups, quota and denylist checks."""
    email = (email or "").strip().lower()
    local, _, domain = email.partition("@")
    if not local or not domain:
        return email
    domain = _DOMAIN_ALIASES.get(domain, domain)
    if domain in _PLUS_TAG_DOMAINS:
        local = local.split("+", 1)[0]
    if domain in _DOTLESS_DOMAINS:
        local = local.replace(".", "")
    return f"{local}@{domain}"
