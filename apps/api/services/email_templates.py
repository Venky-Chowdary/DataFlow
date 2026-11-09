"""Branded transactional email templates (P0-3).

Every email renders branded HTML plus a plain-text part from
``render_email(name, context)``. Hard rules from the handover:

- No tracking pixels, no link rewriting, no third-party assets — the only
  ``<img>`` is the app's static first-party logo, and the only other URL is
  the first-party action URL the template itself was given.
- From ``Datawrap <noreply@datawrap.io>``, Reply-To contact@datawrap.io
  (envelope concern of ``email_sender``); the footer names datawrap.io only.
- All dynamic values are HTML-escaped at render time; subjects are folded to
  one line so a value can never inject a header.

Brand (Datawrap brand pack v1.0): Night ground #072B29, Deep tile #0A3D3A,
Signal teal #2DD4BF (rules and accents only — never text on a light ground),
Teal 700 #0F766E for links on light, Ink #0F172A headings, Slate #475569 body,
Paper #F6FAF9 page. Amber is the quarantine colour; it appears only inside
the mark image. Type: Plus Jakarta Sans (headings), Inter (body), IBM Plex Mono
(codes, labels) — named with system fallbacks, never loaded remotely.

The pack's email-header PNG is deliberately not used: it would have to be a
remote image (forbidden), a data: URI (stripped by Gmail) or a CID part
(``email_sender`` sends no multipart/related), and it carries the unapproved
tagline. The header uses the app's own mark image plus a live-text wordmark,
so it matches the logo operators see in the product.
"""

from __future__ import annotations

import html as _html
import re
from dataclasses import dataclass
from typing import Any, Callable

_NIGHT = "#072B29"
_DEEP = "#0A3D3A"
_TEAL = "#2DD4BF"
_TEAL_700 = "#0F766E"
_INK = "#0F172A"
_SLATE = "#475569"
_PAPER = "#F6FAF9"
_LINE = "#D7E3E1"
_WHITE = "#F8FAFC"
_ON_NIGHT = "#8FB5B0"  # source-line grey on Night, 6.8:1

_FONT_HEAD = "'Plus Jakarta Sans','Segoe UI',Helvetica,Arial,sans-serif"
_FONT_BODY = "Inter,'Segoe UI',Helvetica,Arial,sans-serif"
_FONT_MONO = "'IBM Plex Mono',Consolas,'Courier New',monospace"

ROLE_LABELS = {"admin": "Admin", "editor": "Editor", "viewer": "Viewer"}
ROLE_SUMMARIES = {
    "admin": "full workspace access, including members and roles",
    "editor": "run transfers, edit connectors and schedules",
    "viewer": "read jobs, connectors and proofs",
}


@dataclass(frozen=True)
class RenderedEmail:
    subject: str
    text: str
    html: str


def _esc(value: Any) -> str:
    return _html.escape(str(value or ""), quote=True)


_SUBJECT_UNSAFE = re.compile(r"[\x00-\x1f\x7f]+")


def _subject_value(value: Any, limit: int = 80) -> str:
    """A context value made safe for a subject line: one line, bounded."""
    text = " ".join(_SUBJECT_UNSAFE.sub(" ", str(value or "")).split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "\u2026"


def _mark() -> str:
    """The app's Datawrap mark (``apps/web/public/brand/datawrap-mark-256.png``,
    the raster ``DtLogo`` uses). One static first-party asset: no query string,
    nothing per-recipient, so it cannot act as a tracking pixel."""
    from services.brand_env import getenv_brand

    base = (getenv_brand("APP_URL") or "https://www.datawrap.io").rstrip("/")
    return (f'<img src="{_esc(base)}/brand/datawrap-mark-256.png" width="36" height="36" '
            f'alt="Datawrap" style="display:block;width:36px;height:36px;border:0;'
            f'border-radius:9px;">')


def _shell(title: str, body: str, *, preheader: str = "") -> str:
    """Branded HTML shell — table layout, inline CSS, no remote assets."""
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light"><meta name="supported-color-schemes" content="light">
<title>{_esc(title)}</title></head>
<body style="margin:0;padding:0;background:{_PAPER};font-family:{_FONT_BODY};color:{_SLATE};">
<div style="display:none;max-height:0;overflow:hidden;mso-hide:all;">{_esc(preheader)}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:{_PAPER};padding:32px 16px;">
<tr><td align="center">
<table role="presentation" width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;background:#FFFFFF;border:1px solid {_LINE};border-radius:12px;overflow:hidden;">
<tr><td bgcolor="{_NIGHT}" style="padding:22px 32px;background:{_NIGHT};border-bottom:3px solid {_TEAL};">
  <table role="presentation" cellpadding="0" cellspacing="0" border="0"><tr>
    <td style="vertical-align:middle;padding-right:12px;">{_mark()}</td>
    <td style="vertical-align:middle;">
      <div style="font-family:{_FONT_HEAD};font-size:20px;line-height:24px;font-weight:700;color:{_WHITE};letter-spacing:-0.3px;">Datawrap</div>
      <div style="font-family:{_FONT_MONO};font-size:10px;line-height:16px;font-weight:600;letter-spacing:1.6px;text-transform:uppercase;color:{_ON_NIGHT};">Governed data transfer</div>
    </td>
  </tr></table>
</td></tr>
<tr><td style="padding:32px;">
{body}
</td></tr>
<tr><td style="padding:20px 32px;background:{_PAPER};border-top:1px solid {_LINE};font-size:12px;color:{_SLATE};line-height:1.6;">
  <span style="font-family:{_FONT_HEAD};font-weight:700;color:{_DEEP};">Datawrap</span> &middot; <span style="font-family:{_FONT_MONO};color:{_TEAL_700};">datawrap.io</span><br>
  Questions? Reply to this email or write to contact@datawrap.io.<br>
  You received this because of activity on a Datawrap account.
</td></tr>
</table>
</td></tr></table>
</body></html>"""


def _h1(text: str) -> str:
    return (f'<h1 style="margin:0 0 12px;font-family:{_FONT_HEAD};font-size:22px;line-height:28px;'
            f'font-weight:700;letter-spacing:-0.3px;color:{_INK};">{_esc(text)}</h1>')


def _p(text: str) -> str:
    return f'<p style="margin:0 0 16px;font-size:15px;line-height:1.6;color:{_SLATE};">{_esc(text)}</p>'


def _eyebrow(text: str) -> str:
    return (f'<p style="margin:0 0 8px;font-family:{_FONT_MONO};font-size:11px;font-weight:600;'
            f'letter-spacing:1.4px;text-transform:uppercase;color:{_TEAL_700};">{_esc(text)}</p>')


def _code_box(code: str) -> str:
    return (
        f'<div style="margin:20px 0;padding:16px;text-align:center;'
        f'font-size:32px;font-weight:700;letter-spacing:8px;color:{_INK};'
        f'background:{_PAPER};border:1px solid {_LINE};border-radius:8px;'
        f'font-family:{_FONT_MONO};">{_esc(code)}</div>'
    )


def _link_button(url: str, label: str) -> str:
    """Bulletproof button: the colour sits on the table cell, so Outlook
    (which ignores padding on links) still paints a solid target."""
    return (
        f'<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="margin:24px 0;">'
        f'<tr><td bgcolor="{_DEEP}" style="background:{_DEEP};border-radius:8px;">'
        f'<a href="{_esc(url)}" style="display:inline-block;padding:12px 28px;'
        f'font-family:{_FONT_BODY};color:{_WHITE};border-radius:8px;text-decoration:none;'
        f'font-size:15px;font-weight:600;">{_esc(label)}</a></td></tr></table>'
    )


def _facts(rows: list[tuple[str, str]]) -> str:
    """A receipt-style key/value panel: mono labels, ink values."""
    cells = "".join(
        f'<tr><td style="padding:6px 16px 6px 0;font-family:{_FONT_MONO};font-size:11px;'
        f'letter-spacing:1px;text-transform:uppercase;color:{_SLATE};vertical-align:top;white-space:nowrap;">'
        f'{_esc(k)}</td><td style="padding:6px 0;font-size:14px;line-height:1.5;color:{_INK};">{_esc(v)}</td></tr>'
        for k, v in rows if v
    )
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" '
            f'style="margin:4px 0 8px;padding:12px 16px;background:{_PAPER};border:1px solid {_LINE};'
            f'border-left:3px solid {_TEAL};border-radius:8px;">{cells}</table>')


def _steps(items: list[str]) -> str:
    rows = "".join(
        f'<tr><td style="padding:6px 12px 6px 0;font-family:{_FONT_MONO};font-size:13px;font-weight:600;'
        f'color:{_TEAL_700};vertical-align:top;">{i:02d}</td>'
        f'<td style="padding:6px 0;font-size:15px;line-height:1.55;color:{_SLATE};">{_esc(text)}</td></tr>'
        for i, text in enumerate(items, start=1)
    )
    return f'<table role="presentation" cellpadding="0" cellspacing="0" style="margin:0 0 8px;">{rows}</table>'


def _muted(text: str) -> str:
    return f'<p style="margin:0 0 8px;font-size:12px;line-height:1.6;color:{_SLATE};">{_esc(text)}</p>'


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
        f"<tr><td style='padding:4px 12px 4px 0;color:{_SLATE};font-size:13px;vertical-align:top;'>{_esc(k)}</td>"
        f"<td style='padding:4px 0;font-size:13px;color:{_SLATE};'>{_esc(v)}</td></tr>"
        for k, v in ctx.items()
        if v
    )
    body = _h1("New sales lead") + f"<table role='presentation' cellpadding='0' cellspacing='0'>{rows}</table>"
    text = "New sales lead\n\n" + "\n".join(f"{k}: {v}" for k, v in ctx.items() if v) + "\n\nDatawrap - datawrap.io"
    return RenderedEmail(subject, text, _shell(subject, body))


def _role_line(role: str) -> str:
    key = str(role or "viewer").strip().lower()
    return f"{ROLE_LABELS.get(key, key.title())} \u2014 {ROLE_SUMMARIES.get(key, '')}".rstrip(" \u2014")


def _welcome(ctx: dict[str, Any]) -> RenderedEmail:
    """Sent once per new account, after the address is verified (signup) or
    proven by an accepted invitation. Copy follows the brand voice: specific,
    calm, no exclamation marks, no claims."""
    name = _subject_value(ctx.get("name"), 128) or "there"
    workspace = _subject_value(ctx.get("workspace_name") or "your workspace", 128)
    url = str(ctx.get("app_url", ""))
    joined = bool(ctx.get("joined"))
    role = str(ctx.get("role") or "admin")
    subject = "Welcome to Datawrap"
    if joined:
        lead = (f"Hi {name}, your Datawrap account is ready and you joined "
                f"{workspace} as {ROLE_LABELS.get(role, role)}.")
        steps = [
            "Open the workspace to see its connectors, pipelines and recent jobs.",
            "Every transfer runs preflight first: if a gate blocks, nothing moves and you get the reason.",
            "Proofs show what each run checked \u2014 row counts, checksums and quarantined rows.",
        ]
    else:
        lead = (f"Hi {name}, your Datawrap account is verified and {workspace} is set up. "
                "You are its admin.")
        steps = [
            "Connect a source and a destination from Connectors.",
            "Run preflight: if a gate blocks, nothing moves and you get the reason.",
            "Invite your team from Settings \u2192 Team & access.",
        ]
    body = (
        _eyebrow("Account ready")
        + _h1("Welcome to Datawrap")
        + _p(lead)
        + _p("Where to start:")
        + _steps(steps)
        + (_link_button(url, "Open Datawrap") if url else "")
        + _muted("This is a one-time welcome email for the account created with this address. "
                 "If you didn't create it, write to contact@datawrap.io.")
    )
    text = (
        "Welcome to Datawrap\n\n"
        f"{lead}\n\n"
        "Where to start:\n"
        + "".join(f"  {i:02d}  {line}\n" for i, line in enumerate(steps, start=1))
        + (f"\nOpen Datawrap: {url}\n" if url else "")
        + "\nThis is a one-time welcome email for the account created with this address. "
        "If you didn't create it, write to contact@datawrap.io.\n\n"
        "Datawrap - datawrap.io"
    )
    return RenderedEmail(subject, text, _shell(subject, body, preheader=f"Your Datawrap account is ready \u2014 {workspace}"))



def _invited_line(who: str, workspace: str) -> str:
    """"invited you to the Acme workspace" — without doubling the noun when the
    name already ends in it (every self-serve default: "Sam's workspace")."""
    if workspace.strip().lower().endswith("workspace"):
        return f"{who} invited you to {workspace}."
    return f"{who} invited you to the {workspace} workspace."

def _workspace_invite(ctx: dict[str, Any]) -> RenderedEmail:
    """A workspace admin's invitation. The inviter's address is always shown
    next to their display name, so a name like "Datawrap Security" cannot
    pass for us. The accept link carries its token in the URL fragment."""
    inviter = _subject_value(ctx.get("inviter_name"), 128)
    inviter_email = _subject_value(ctx.get("inviter_email"), 254)
    who = f"{inviter} ({inviter_email})" if inviter and inviter_email and inviter != inviter_email \
        else (inviter_email or inviter or "A workspace admin")
    workspace = _subject_value(ctx.get("workspace_name") or "a workspace", 128)
    role = str(ctx.get("role") or "viewer").strip().lower()
    url = str(ctx.get("accept_url", ""))
    days = int(ctx.get("expires_days", 7))
    subject = (f"{_subject_value(inviter or inviter_email or 'A workspace admin', 60)} invited you to "
               f"{_subject_value(workspace, 60)} on Datawrap")
    body = (
        _eyebrow("Workspace invitation")
        + _h1(f"Join {workspace} on Datawrap")
        + _p(_invited_line(who, workspace))
        + _facts([
            ("Workspace", workspace),
            ("Role", _role_line(role)),
            ("Invited by", who),
            ("Expires", f"in {days} days"),
        ])
        + _link_button(url, "Accept invitation")
        + _p(f"The link works once and expires in {days} days. If you don't have a Datawrap "
             "account yet, you'll create one when you accept.")
        + _muted("Not expecting this? Ignore this email \u2014 nothing happens until the link is used. "
                 "No tracking links are used.")
    )
    text = (
        f"Join {workspace} on Datawrap\n\n"
        f"{_invited_line(who, workspace)}\n\n"
        f"  Workspace:  {workspace}\n"
        f"  Role:       {_role_line(role)}\n"
        f"  Invited by: {who}\n\n"
        "Accept the invitation (the link works once):\n\n"
        f"    {url}\n\n"
        f"The link expires in {days} days. If you don't have a Datawrap account yet, "
        "you'll create one when you accept.\n\n"
        "Not expecting this? Ignore this email - nothing happens until the link is used.\n\n"
        "Datawrap - datawrap.io"
    )
    return RenderedEmail(subject, text, _shell(subject, body, preheader=f"{who} invited you to {workspace}"))


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
    "welcome": _welcome,
    "workspace_invite": _workspace_invite,
}


def template_names() -> list[str]:
    return sorted(_TEMPLATES)


def render_email(name: str, context: dict[str, Any] | None = None) -> RenderedEmail:
    builder = _TEMPLATES.get(name)
    if builder is None:
        raise KeyError(f"unknown email template {name!r} (have: {', '.join(template_names())})")
    return builder(context or {})
