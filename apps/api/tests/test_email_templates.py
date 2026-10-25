"""P0-3 — branded email templates: snapshot + no remote assets.

Every template renders HTML + text, carries no <img> (no tracking pixels or
remote images), rewrites no links, mentions datawrap.io only, and escapes
all dynamic context.
"""

from __future__ import annotations

import re

import pytest

from services.email_templates import render_email, template_names


_CTX = {
    "code": "483920",
    "name": "Ada",
    "expires_minutes": 10,
    "new_email": "ada@new.example",
    "device": "Chrome on macOS",
    "location": "Berlin",
    "company": "Acme",
    "email": "ada@acme.example",
}


def test_all_templates_render_html_and_text():
    assert set(template_names()) == {
        "signup_code",
        "existing_account_notice",
        "reset_code",
        "email_change_code",
        "email_change_old_notice",
        "signin_alert",
        "sales_acknowledgement",
        "sales_lead_internal",
    }
    for name in template_names():
        mail = render_email(name, dict(_CTX))
        assert mail.subject.strip(), name
        assert mail.text.strip(), name
        assert mail.html.lstrip().startswith("<!DOCTYPE html"), name


def test_no_remote_images_or_tracking_pixels():
    for name in template_names():
        mail = render_email(name, dict(_CTX))
        assert "<img" not in mail.html.lower(), name
        assert "http://" not in mail.html and "https://" not in mail.html, name
        assert "url(" not in mail.html, name
        # no link rewriting: no redirect/proxy wrappers
        assert re.search(r"href=", mail.html) is None, name


def test_footer_names_only_datawrap_domain():
    for name in template_names():
        mail = render_email(name, dict(_CTX))
        assert "datawrap.io" in mail.html
        for banned in ("datawrap.ai", "datawrap.app", "datawrap.com", "zeptomail", "resend.com"):
            assert banned not in mail.html.lower(), (name, banned)
            assert banned not in mail.text.lower(), (name, banned)


def test_context_is_html_escaped():
    mail = render_email("signup_code", {"code": "111111", "name": "<script>alert(1)</script>"})
    assert "<script>" not in mail.html
    assert "&lt;script&gt;" in mail.html


def test_signup_code_carries_code_in_body_only():
    mail = render_email("signup_code", dict(_CTX))
    assert "483920" in mail.html and "483920" in mail.text
    assert "483920" not in mail.subject  # codes never in the subject


def test_unknown_template_raises():
    with pytest.raises(KeyError):
        render_email("nope", {})


@pytest.mark.parametrize("name", sorted(template_names()))
def test_snapshot_markers(name):
    """Deterministic structural snapshot per template."""
    mail = render_email(name, dict(_CTX))
    assert mail.html.count("Datawrap") >= 2  # wordmark + footer
    assert 'border-bottom:3px solid #2dd4bf' in mail.html  # brand rule
    assert "contact@datawrap.io" in mail.html  # reply path
