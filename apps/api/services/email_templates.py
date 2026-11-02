"""Branded transactional email templates (P0-3).

Every email renders branded HTML plus a plain-text part from
``render_email(name, context)``. Hard rules from the handover:

- No tracking pixels, no link rewriting, no remote images — the HTML
  contains zero ``<img>`` and zero external URLs other than the optional
  ``action_url`` the template itself was given.
- From ``Datawrap <noreply@datawrap.io>``, Reply-To contact@datawrap.io
  (envelope concern of ``email_sender``); the footer names datawrap.io only.
- All dynamic values are HTML-escaped at render time.
- Logo palette: ink #0f766e, arrow #2dd4bf, tile #0a3d3a (tokens.css).
"""

from __future__ import annotations

import html as _html
from dataclasses import dataclass
from typing import Any, Callable

_INK = "#0f766e"      # --df-logo-ink
_ARROW = "#2dd4bf"    # --df-logo-arrow
_TILE = "#0a3d3a"     # --df-logo-tile
_SLATE = "#334155"
_MUTED = "#64748b"
_BORDER = "#e2e8f0"
_BG = "#f8fafc"


@dataclass(frozen=True)
class RenderedEmail:
    subject: str
    text: str
    html: str


def _esc(value: Any) -> str:
    return _html.escape(str(value or ""), quote=True)


def _shell(title: str, body: str, *, preheader: str = "") -> str:
    """Branded HTML shell — table layout, inline CSS, no remote assets."""
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_esc(title)}</title></head>
<body style="margin:0;padding:0;background:{_BG};font-family:-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:{_SLATE};">
<div style="display:none;max-height:0;overflow:hidden;">{_esc(preheader)}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:{_BG};padding:32px 16px;">
<tr><td align="center">
<table role="presentation" width="520" cellpadding="0" cellspacing="0" style="max-width:520px;width:100%;background:#ffffff;border:1px solid {_BORDER};border-radius:12px;overflow:hidden;">
<tr><td style="padding:24px 32px;border-bottom:3px solid {_ARROW};">
  <span style="font-size:20px;font-weight:700;color:{_TILE};letter-spacing:-0.3px;">Data<span style="color:{_INK};">wrap</span></span>
</td></tr>
<tr><td style="padding:32px;">
{body}
</td></tr>
<tr><td style="padding:20px 32px;border-top:1px solid {_BORDER};font-size:12px;color:{_MUTED};line-height:1.6;">
  Datawrap &middot; datawrap.io<br>
  Questions? Reply to this email or write to contact@datawrap.io.<br>
  You received this because of activity on a Datawrap account.
</td></tr>
</table>
</td></tr></table>
</body></html>"""


def _h1(text: str) -> str:
    return f'<h1 style="margin:0 0 12px;font-size:20px;font-weight:700;color:{_TILE};">{_esc(text)}</h1>'


def _p(text: str) -> str:
    return f'<p style="margin:0 0 16px;font-size:14px;line-height:1.6;color:{_SLATE};">{_esc(text)}</p>'


def _code_box(code: str) -> str:
    return (
        f'<div style="margin:20px 0;padding:16px;text-align:center;'
        f'font-size:32px;font-weight:700;letter-spacing:8px;color:{_TILE};'
        f'background:{_BG};border:1px solid {_BORDER};border-radius:8px;'
        f'font-family:ui-monospace,Menlo,Consolas,monospace;">{_esc(code)}</div>'
    )


def _link_button(url: str, label: str) -> str:
    return (
        f'<div style="margin:24px 0;text-align:center;"><a href="{_esc(url)}" '
        f'style="display:inline-block;padding:12px 28px;background:{_TILE};'
        f'color:#ffffff;border-radius:8px;text-decoration:none;font-size:15px;'
        f'font-weight:600;">{_esc(label)}</a></div>'
    )


def _muted(text: str) -> str:
    return f'<p style="margin:0 0 8px;font-size:12px;line-height:1.6;color:{_MUTED};">{_esc(text)}</p>'


# ---------------------------------------------------------------------------
# Templates — each returns (subject, text_lines, html_body)
# ---------------------------------------------------------------------------

def _signup_code(ctx: dict[str, Any]) -> RenderedEmail:
    code = str(ctx.get("code", ""))
    minutes = int(ctx.get("expires_minutes", 10))
    subject = "Your Datawrap verification code"
    body = (
        _h1("Verify your email")
        + _p(f"Hi {ctx.get('name') or 'there'}, enter this code to finish creating your Datawrap account:")
        + _code_box(code)
        + _p(f"The code expires in {minutes} minutes. It works once.")
        + _muted("If you didn't sign up for Datawrap, you can ignore this email.")
    )
    text = (
        f"Verify your email\n\nHi {ctx.get('name') or 'there'},\n\n"
        f"Enter this code to finish creating your Datawrap account:\n\n"
        f"    {code}\n\n"
        f"The code expires in {minutes} minutes and works once.\n\n"
        "If you didn't sign up for Datawrap, you can ignore this email.\n\n"
        "Datawrap - datawrap.io"
    )
    return RenderedEmail(subject, text, _shell(subject, body, preheader="Your Datawrap verification code"))


def _existing_account_notice(ctx: dict[str, Any]) -> RenderedEmail:
    subject = "You already have a Datawrap account"
    body = (
        _h1("Welcome back")
        + _p("Someone just tried to sign up for Datawrap with this email address — but you already have an account.")
        + _p("Sign in with your existing password. If you forgot it, use \"Forgot password\" on the sign-in page.")
        + _muted("If this wasn't you, you can ignore this email — no changes were made.")
    )
    text = (
        "You already have a Datawrap account\n\n"
        "Someone just tried to sign up for Datawrap with this email address - but you already have an account.\n\n"
        "Sign in with your existing password, or use \"Forgot password\" on the sign-in page.\n\n"
        "If this wasn't you, you can ignore this email - no changes were made.\n\n"
        "Datawrap - datawrap.io"
    )
    return RenderedEmail(subject, text, _shell(subject, body))


def _reset_code(ctx: dict[str, Any]) -> RenderedEmail:
    code = str(ctx.get("code", ""))
    minutes = int(ctx.get("expires_minutes", 10))
    subject = "Reset your Datawrap password"
    body = (
        _h1("Reset your password")
        + _p("Enter this code to reset the password on your Datawrap account:")
        + _code_box(code)
        + _p(f"The code expires in {minutes} minutes. Resetting signs out every device.")
        + _muted("If you didn't request a password reset, you can ignore this email - your password stays the same.")
    )
    text = (
        "Reset your Datawrap password\n\n"
        "Enter this code to reset the password on your Datawrap account:\n\n"
        f"    {code}\n\n"
        f"The code expires in {minutes} minutes. Resetting signs out every device.\n\n"
        "If you didn't request this, ignore this email - your password stays the same.\n\n"
        "Datawrap - datawrap.io"
    )
    return RenderedEmail(subject, text, _shell(subject, body))


def _reset_link(ctx: dict[str, Any]) -> RenderedEmail:
    url = str(ctx.get("reset_url", ""))
    minutes = int(ctx.get("expires_minutes", 30))
    subject = "Reset your Datawrap password"
    body = (
        _h1("Reset your password")
        + _p("Use the link below to choose a new password for your Datawrap account. "
             "This link can be used once.")
        + _link_button(url, "Reset password")
        + _p(f"The link expires in {minutes} minutes. Resetting signs out every device.")
        + _muted("If you didn't request a password reset, you can ignore this email - "
                 "your password stays the same. No tracking links are used.")
    )
    text = (
        "Reset your Datawrap password\n\n"
        "Use this link to choose a new password (it works once):\n\n"
        f"    {url}\n\n"
        f"The link expires in {minutes} minutes. Resetting signs out every device.\n\n"
        "If you didn't request this, ignore this email - your password stays the same.\n\n"
        "Datawrap - datawrap.io"
    )
    return RenderedEmail(subject, text, _shell(subject, body))


def _password_changed_notice(ctx: dict[str, Any]) -> RenderedEmail:
    subject = "Your Datawrap password was changed"
    body = (
        _h1("Password changed")
        + _p("The password on your Datawrap account was just changed. "
             "Every signed-in device has been signed out.")
        + _muted("If this wasn't you, contact contact@datawrap.io right away "
                 "and reset your password again.")
    )
    text = (
        "Your Datawrap password was changed\n\n"
        "The password on your Datawrap account was just changed and every "
        "signed-in device was signed out.\n\n"
        "If this wasn't you, contact contact@datawrap.io right away.\n\n"
        "Datawrap - datawrap.io"
    )
    return RenderedEmail(subject, text, _shell(subject, body))


def _email_change_code(ctx: dict[str, Any]) -> RenderedEmail:
    code = str(ctx.get("code", ""))
    subject = "Confirm your new Datawrap email"
    body = (
        _h1("Confirm your new email")
        + _p("Enter this code to make this the sign-in email on your Datawrap account:")
        + _code_box(code)
        + _p("The code expires in 10 minutes.")
        + _muted("If you didn't request an email change, contact us at contact@datawrap.io right away.")
    )
    text = (
        "Confirm your new Datawrap email\n\n"
        "Enter this code to make this the sign-in email on your Datawrap account:\n\n"
        f"    {code}\n\n"
        "The code expires in 10 minutes.\n\n"
        "If you didn't request this, contact contact@datawrap.io right away.\n\n"
        "Datawrap - datawrap.io"
    )
    return RenderedEmail(subject, text, _shell(subject, body))


def _email_change_old_notice(ctx: dict[str, Any]) -> RenderedEmail:
    subject = "Your Datawrap sign-in email changed"
    body = (
        _h1("Sign-in email changed")
        + _p(f"The sign-in email on your Datawrap account was just changed to {ctx.get('new_email') or 'a new address'}.")
        + _p("If this was you, nothing else to do.")
        + _muted("If this wasn't you, contact contact@datawrap.io immediately.")
    )
    text = (
        "Your Datawrap sign-in email changed\n\n"
        f"The sign-in email on your Datawrap account was just changed to {ctx.get('new_email') or 'a new address'}.\n\n"
        "If this was you, nothing else to do. If it wasn't, contact contact@datawrap.io immediately.\n\n"
        "Datawrap - datawrap.io"
    )
    return RenderedEmail(subject, text, _shell(subject, body))


def _signin_alert(ctx: dict[str, Any]) -> RenderedEmail:
    subject = "New sign-in to your Datawrap account"
    body = (
        _h1("New sign-in")
        + _p(f"Your Datawrap account was just used to sign in from {ctx.get('device') or 'a device'}"
             + (f" near {ctx.get('location')}." if ctx.get("location") else "."))
        + _p("If this was you, you can ignore this email.")
        + _muted("If this wasn't you, reset your password from the sign-in page and contact contact@datawrap.io.")
    )
    text = (
        "New sign-in to your Datawrap account\n\n"
        f"Your account was just used to sign in from {ctx.get('device') or 'a device'}.\n\n"
        "If this wasn't you, reset your password and contact contact@datawrap.io.\n\n"
        "Datawrap - datawrap.io"
    )
    return RenderedEmail(subject, text, _shell(subject, body))


def _sales_acknowledgement(ctx: dict[str, Any]) -> RenderedEmail:
    name = ctx.get("name") or "there"
    subject = "We received your Datawrap inquiry"
    body = (
        _h1("Thanks for reaching out")
        + _p(f"Hi {name}, thanks for contacting the Datawrap team. We received your note and will reply within one business day.")
        + _p("In the meantime you can reply to this email with anything else you'd like us to know.")
        + _muted("Datawrap - data movement that proves itself. datawrap.io")
    )
    text = (
        "We received your Datawrap inquiry\n\n"
        f"Hi {name}, thanks for contacting the Datawrap team. We received your note and will reply within one business day.\n\n"
        "Reply to this email with anything else you'd like us to know.\n\n"
        "Datawrap - datawrap.io"
    )
    return RenderedEmail(subject, text, _shell(subject, body))


def _sales_lead_internal(ctx: dict[str, Any]) -> RenderedEmail:
    subject = f"Sales lead: {ctx.get('company') or 'unknown company'} ({ctx.get('email') or 'no email'})"
    rows = "".join(
        f"<tr><td style='padding:4px 12px 4px 0;color:{_MUTED};font-size:13px;vertical-align:top;'>{_esc(k)}</td>"
        f"<td style='padding:4px 0;font-size:13px;color:{_SLATE};'>{_esc(v)}</td></tr>"
        for k, v in ctx.items()
        if v
    )
    body = _h1("New sales lead") + f"<table role='presentation' cellpadding='0' cellspacing='0'>{rows}</table>"
    text = "New sales lead\n\n" + "\n".join(f"{k}: {v}" for k, v in ctx.items() if v) + "\n\nDatawrap - datawrap.io"
    return RenderedEmail(subject, text, _shell(subject, body))


_TEMPLATES: dict[str, Callable[[dict[str, Any]], RenderedEmail]] = {
    "signup_code": _signup_code,
    "existing_account_notice": _existing_account_notice,
    "reset_code": _reset_code,
    "reset_link": _reset_link,
    "password_changed_notice": _password_changed_notice,
    "email_change_code": _email_change_code,
    "email_change_old_notice": _email_change_old_notice,
    "signin_alert": _signin_alert,
    "sales_acknowledgement": _sales_acknowledgement,
    "sales_lead_internal": _sales_lead_internal,
}


def template_names() -> list[str]:
    return sorted(_TEMPLATES)


def render_email(name: str, context: dict[str, Any] | None = None) -> RenderedEmail:
    builder = _TEMPLATES.get(name)
    if builder is None:
        raise KeyError(f"unknown email template {name!r} (have: {', '.join(template_names())})")
    return builder(context or {})
