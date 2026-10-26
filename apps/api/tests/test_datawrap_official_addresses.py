"""Guard: only official Datawrap addresses/domains ship in the web app.

Official addresses are ``sales@datawrap.io`` and ``contact@datawrap.io``.
Nothing under ``apps/web`` may reference ``datawrap.ai``, ``datawrap.app`` or
``datawrap.com`` (domain or email), and no marketing page may show an email
outside the two official ones.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[3]
_WEB = _REPO / "apps" / "web"

_BANNED = re.compile(r"datawrap\.(?:ai|app|com)\b", re.I)
# Any *datawrap-owned* mailbox — a visitor's own-email placeholder
# (``name@company.com``) is not a Datawrap address and stays out of scope.
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@datawrap\.[A-Za-z0-9.-]+", re.I)
_ALLOWED_EMAILS = {"sales@datawrap.io", "contact@datawrap.io"}

_MARKETING_GLOBS = (
    "src/pages/marketing/**/*.tsx",
    "src/pages/marketing/**/*.ts",
    "src/components/marketing/**/*.tsx",
)


def _web_files() -> list[Path]:
    return [
        p
        for p in _WEB.rglob("*")
        if p.is_file()
        and "node_modules" not in p.parts
        and "dist" not in p.parts
        and p.suffix in {".ts", ".tsx", ".html", ".xml", ".txt", ".json", ".js"}
    ]


def _marketing_files() -> list[Path]:
    out: list[Path] = []
    for glob in _MARKETING_GLOBS:
        out.extend(_WEB.glob(glob))
    return [p for p in out if p.is_file()]


def test_no_banned_datawrap_domain_under_apps_web():
    offenders = []
    for path in _web_files():
        for lineno, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if _BANNED.search(line):
                offenders.append(f"{path.relative_to(_REPO)}:{lineno}")
    assert not offenders, "non-official datawrap domain:\n" + "\n".join(offenders)


def test_marketing_pages_only_official_emails():
    offenders = []
    for path in _marketing_files():
        for lineno, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            for addr in _EMAIL.findall(line):
                if addr.lower().rstrip(".") not in _ALLOWED_EMAILS:
                    offenders.append(f"{path.relative_to(_REPO)}:{lineno} {addr}")
    assert not offenders, "non-official email on a marketing page:\n" + "\n".join(
        offenders
    )


def test_official_addresses_present():
    """Guard stays meaningful: the two official addresses must actually ship."""
    footer = (_WEB / "src/components/marketing/MarketingChrome.tsx").read_text()
    contact = (_WEB / "src/pages/marketing/ContactSalesPage.tsx").read_text()
    assert "contact@datawrap.io" in footer
    assert "contact@datawrap.io" in contact
    assert "sales@datawrap.io" in contact
