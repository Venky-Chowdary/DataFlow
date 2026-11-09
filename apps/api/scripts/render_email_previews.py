#!/usr/bin/env python3
"""Render every transactional email to static HTML (and PNG with --png).

Previews use sample context only — no tokens, no real addresses. Output goes
to ``--out`` (default ``./email-previews``); nothing is sent.

    cd apps/api && ../../.venv/bin/python scripts/render_email_previews.py --png
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.email_templates import render_email, template_names  # noqa: E402

SAMPLE = {
    "name": "Ada Lovelace",
    "code": "483920",
    "expires_minutes": 10,
    "new_email": "ada@new.example",
    "device": "Chrome on macOS",
    "location": "Berlin",
    "company": "Acme Analytics",
    "email": "ada@acme.example",
    "reset_url": "https://www.datawrap.io/reset-password#token=sample-preview-token",
    "workspace_name": "Acme Analytics",
    "app_url": "https://www.datawrap.io/",
    "inviter_name": "Grace Hopper",
    "inviter_email": "grace@acme.example",
    "role": "editor",
    "accept_url": "https://www.datawrap.io/accept-invite#token=sample-preview-token",
    "expires_days": 7,
}

VARIANTS = {"welcome-joined": ("welcome", {"joined": True, "role": "editor"})}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="email-previews")
    ap.add_argument("--png", action="store_true", help="also screenshot each preview (needs Playwright)")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    jobs = {name: (name, {}) for name in template_names()} | VARIANTS
    files = []
    for label, (name, extra) in sorted(jobs.items()):
        mail = render_email(name, {**SAMPLE, **extra})
        (out / f"{label}.html").write_text(mail.html, encoding="utf-8")
        (out / f"{label}.txt").write_text(f"Subject: {mail.subject}\n\n{mail.text}", encoding="utf-8")
        files.append(out / f"{label}.html")
    if args.png:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            page = browser.new_page(viewport={"width": 680, "height": 900}, device_scale_factor=2)
            for path in files:
                page.goto(path.resolve().as_uri())
                page.screenshot(path=str(path.with_suffix(".png")), full_page=True)
            browser.close()
    print(f"rendered {len(files)} previews into {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
