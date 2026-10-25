"""Disposable-domain list (P2-2).

Three sources, checked in order: the team allowlist wins, then the team
denylist, then the disposable set — a bundled seed of well-known burner
domains plus the synced public feed cache.

Feed sync (``sync_feed``): fetches the domain list URL, verifies the
SHA-256 checksum pinned via ``DATAWRAP_DISPOSABLE_FEED_SHA256``, and
replaces the cache atomically. Any failure keeps the last good set —
the caller surfaces "refuse" per the spec. Runs daily via the scheduled
sync entry point; tests inject ``fetch_fn`` so no network is needed.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from datetime import UTC, datetime
from typing import Any

from services.brand_env import getenv_brand
from services import denylist_store

logger = logging.getLogger(__name__)

#: Bundled floor — always refused even before the first feed sync.
_SEED_DOMAINS = frozenset(
    "mailinator.com mailinator.net mailinator2.com guerrillamail.com "
    "guerrillamail.net guerrillamail.org guerrillamailblock.com "
    "tempmail.com temp-mail.org temp-mail.io tempmailo.com "
    "10minutemail.com 10minutemail.net 10minutemail.org "
    "yopmail.com yopmail.fr trashmail.com trashmail.net trashmail.org "
    "getnada.com maildrop.cc sharklasers.com grr.la "
    "dispostable.com mailnesia.com mailcatch.com mailsac.com "
    "tempinbox.com throwawaymail.com fakemailgenerator.com "
    "emailondeck.com mintemail.com spamgourmet.com spam4.me "
    "burnermail.io inboxbear.com mail-temp.com mohmal.com "
    "temp-mail.ru tempmail.de tmpmail.org tmpmail.net".split()
)

_FEED_URL = "https://raw.githubusercontent.com/disposable-email-domains/disposable-email-domains/master/disposable_email_blocklist.conf"

_lock = threading.Lock()
_feed_domains: frozenset[str] = frozenset()
_feed_state: dict[str, Any] = {"synced_at": None, "sha256": "", "error": ""}


def _domain_of(email: str) -> str:
    return (email or "").strip().lower().split("@", 1)[-1]


def allowlist_domains() -> set[str]:
    """Team allowlist entries live as negative-reasoned domain rows."""
    return {
        e["value"] for e in denylist_store.list_entries()
        if e.get("kind") == "domain" and e.get("source") == "feed"
        and e.get("reason") == "allow"
    }


def is_allowed(email: str) -> bool:
    return _domain_of(email) in allowlist_domains()


def is_disposable(email: str) -> bool:
    """Allowlist beats every deny source."""
    domain = _domain_of(email)
    if domain in allowlist_domains():
        return False
    if denylist_store.is_denied(domain, kind="domain"):
        return True
    return domain in _SEED_DOMAINS or domain in _feed_domains


def feed_state() -> dict[str, Any]:
    return dict(_feed_state)


def sync_feed(*, fetch_fn=None, url: str | None = None,
              expected_sha256: str | None = None) -> dict[str, Any]:
    """Fetch the feed, verify its checksum, swap the cache atomically.

    On any failure the previous set is kept and ``feed_state()["error"]``
    names what went wrong — the caller treats "feed stale" as refuse.
    """
    global _feed_domains
    url = url or getenv_brand("DISPOSABLE_FEED_URL", _FEED_URL)
    expected_sha256 = expected_sha256 or getenv_brand(
        "DISPOSABLE_FEED_SHA256", "")
    try:
        if fetch_fn is None:
            import urllib.request

            with urllib.request.urlopen(url, timeout=15.0) as resp:
                raw = resp.read()
        else:
            raw = fetch_fn(url)
        digest = hashlib.sha256(raw).hexdigest()
        if expected_sha256 and digest != expected_sha256:
            raise ValueError("feed checksum mismatch")
        domains = frozenset(
            line.decode("utf-8", "replace").strip().lower()
            for line in raw.splitlines()
            if line.strip() and not line.startswith(b"#"))
        if len(domains) < 10:  # sanity floor — refuse a broken feed
            raise ValueError("feed too small to be valid")
        with _lock:
            _feed_domains = domains
            _feed_state.update(
                synced_at=datetime.now(UTC).isoformat(),
                sha256=digest, error="")
    except Exception as exc:
        _feed_state["error"] = f"{type(exc).__name__}: {exc}"
        logger.warning("disposable feed sync failed: %s", _feed_state["error"])
    return feed_state()


def _reset_for_tests() -> None:
    global _feed_domains
    _feed_domains = frozenset()
    _feed_state.update(synced_at=None, sha256="", error="")
