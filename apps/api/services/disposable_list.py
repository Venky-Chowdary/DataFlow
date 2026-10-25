"""Disposable-domain list (P2-2) — vendored snapshot, no runtime fetch.

The domain set is the union of:

- the vendored blocklist (``services/_vendored/disposable_email_blocklist.txt``,
  a dated snapshot of the public disposable-email-domains list with its
  source commit and SHA-256 in the file header — loaded at import),
- a small bundled seed (defence in depth — always refused),
- the team denylist (``email_denylist`` kind ``domain``).

The team allowlist (``email_denylist`` kind ``allow_domain``) wins over
every deny source.

Optional live refresh (``sync_feed``): OFF by default — it only runs when
``DATAWRAP_DISPOSABLE_FEED_ENABLED=1``, requires the URL pinned to a
specific commit (``master``/``main`` refs are refused) and a pinned
SHA-256 (``DATAWRAP_DISPOSABLE_FEED_SHA256``). A failed or absent refresh
never removes protection: the vendored snapshot always applies.
"""

from __future__ import annotations

import hashlib
import logging
import re
import threading
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from services.brand_env import getenv_brand
from services import denylist_store

logger = logging.getLogger(__name__)

_VENDORED_PATH = Path(__file__).resolve().parent / "_vendored" / "disposable_email_blocklist.txt"

#: Defence-in-depth floor on top of the vendored file.
_SEED_DOMAINS = frozenset(
    "mailinator.com guerrillamail.com tempmail.com yopmail.com "
    "10minutemail.com maildrop.cc sharklasers.com grr.la "
    "dispostable.com mailsac.com temp-mail.org".split()
)


def _load_vendored() -> tuple[frozenset[str], dict[str, str]]:
    header: dict[str, str] = {}
    domains: list[str] = []
    try:
        lines = _VENDORED_PATH.read_text(encoding="utf-8").splitlines()
    except OSError:
        logger.error("vendored disposable list missing: %s", _VENDORED_PATH)
        return frozenset(), header
    for line in lines:
        line = line.strip()
        if line.startswith("#"):
            key, _, value = line.lstrip("# ").partition(":")
            header[key.strip()] = value.strip()
        elif line:
            domains.append(line.lower())
    return frozenset(domains), header


_VENDORED_DOMAINS, VENDORED_HEADER = _load_vendored()


def vendored_domains() -> frozenset[str]:
    return _VENDORED_DOMAINS


def vendored_sha256() -> str:
    """Checksum recorded in the vendored file header (of the domain payload)."""
    return VENDORED_HEADER.get("domains_sha256", "")


def vendored_payload_sha256() -> str:
    """Recomputed checksum over the non-comment payload lines."""
    payload = "\n".join(sorted(_VENDORED_DOMAINS)) + "\n"
    return hashlib.sha256(payload.encode()).hexdigest()


_lock = threading.Lock()
_feed_domains: frozenset[str] = frozenset()
_feed_state: dict[str, Any] = {"synced_at": None, "sha256": "", "error": ""}


def _domain_of(email: str) -> str:
    return (email or "").strip().lower().split("@", 1)[-1]


def is_allowed(email: str) -> bool:
    return denylist_store.is_denied(_domain_of(email), kind="allow_domain")


def is_disposable(email: str) -> bool:
    """Allowlist beats every deny source."""
    domain = _domain_of(email)
    if is_allowed(email):
        return False
    if denylist_store.is_denied(domain, kind="domain"):
        return True
    return domain in _SEED_DOMAINS or domain in _VENDORED_DOMAINS \
        or domain in _feed_domains


def feed_state() -> dict[str, Any]:
    return dict(_feed_state)


def feed_enabled() -> bool:
    return getenv_brand("DISPOSABLE_FEED_ENABLED", "0").lower() in (
        "1", "true", "yes")


# Hosts permitted for the optional live feed. Any new source must be
# allowlisted here deliberately.
_FEED_ALLOWED_HOSTS = frozenset({"raw.githubusercontent.com"})
_COMMIT_SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")


def _validate_feed_url(url: str) -> None:
    """The feed URL must pin an exact commit on an allowlisted HTTPS host.

    Refuses branches, tags, short SHAs, non-HTTPS schemes, and hosts outside
    the allowlist — substring heuristics can't tell a pinned ref from a
    moving one, so the URL is parsed instead.
    """
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception as exc:
        raise ValueError(f"unparseable feed URL: {exc}") from exc
    if parsed.scheme != "https":
        raise ValueError("feed URL must use https")
    host = (parsed.hostname or "").lower()
    if host not in _FEED_ALLOWED_HOSTS:
        raise ValueError(f"feed host {host!r} not allowlisted")
    segments = [s for s in parsed.path.split("/") if s]
    # raw.githubusercontent.com/<owner>/<repo>/<ref>/<path...>
    if len(segments) < 4:
        raise ValueError("feed URL path must be /<owner>/<repo>/<sha>/<file>")
    ref = urllib.parse.unquote(segments[2])
    if not _COMMIT_SHA_RE.fullmatch(ref):
        raise ValueError(
            "feed URL must pin a full 40-hex commit SHA, not a branch/tag/short ref")


def sync_feed(*, fetch_fn=None, url: str | None = None,
              expected_sha256: str | None = None) -> dict[str, Any]:
    """Optional live refresh — flag-gated, pinned-URL, pinned-checksum.

    Refuses to run when the flag is off, the URL isn't commit-pinned, or
    no checksum is configured. On any failure the previous set is kept;
    the vendored snapshot protects users regardless.
    """
    global _feed_domains
    url = url or getenv_brand("DISPOSABLE_FEED_URL", "")
    expected_sha256 = expected_sha256 or getenv_brand(
        "DISPOSABLE_FEED_SHA256", "")
    try:
        if not feed_enabled():
            raise RuntimeError("feed refresh disabled")
        if not url:
            raise ValueError("no feed URL configured")
        _validate_feed_url(url)
        if not expected_sha256:
            raise ValueError("feed checksum pin required")
        if fetch_fn is None:
            import urllib.request

            with urllib.request.urlopen(url, timeout=15.0) as resp:
                raw = resp.read()
        else:
            raw = fetch_fn(url)
        digest = hashlib.sha256(raw).hexdigest()
        if digest != expected_sha256:
            raise ValueError("feed checksum mismatch")
        domains = frozenset(
            line.decode("utf-8", "replace").strip().lower()
            for line in raw.splitlines()
            if line.strip() and not line.startswith(b"#"))
        if len(domains) < 10:
            raise ValueError("feed too small to be valid")
        with _lock:
            _feed_domains = domains
            _feed_state.update(
                synced_at=datetime.now(UTC).isoformat(),
                sha256=digest, error="")
    except Exception as exc:
        _feed_state["error"] = f"{type(exc).__name__}: {exc}"
        logger.warning("disposable feed sync refused/failed: %s",
                       _feed_state["error"])
    return feed_state()


def _reset_for_tests() -> None:
    global _feed_domains
    _feed_domains = frozenset()
    _feed_state.update(synced_at=None, sha256="", error="")
