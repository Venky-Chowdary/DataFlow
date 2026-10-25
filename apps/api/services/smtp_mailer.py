"""Small reusable outbound mailer — Zoho SMTP over stdlib smtplib (D103).

No third-party dependencies. All credentials come from env vars; none are
read anywhere else in the codebase:

- ``DATAFLOW_SMTP_HOST``      default ``smtp.zoho.com``
- ``DATAFLOW_SMTP_PORT``      default ``587``
- ``DATAFLOW_SMTP_USERNAME``  Zoho account login (e.g. sales@datawrap.io)
- ``DATAFLOW_SMTP_PASSWORD``  Zoho app password — SECRET, env only
- ``DATAFLOW_SMTP_FROM``      envelope/header From; defaults to the username
- ``DATAFLOW_SMTP_SECURITY``  ``starttls`` (default) | ``ssl`` | ``none``
- ``DATAFLOW_SMTP_TIMEOUT_SEC`` default ``10``

``send_mail`` never raises — it returns ``{"sent": True}`` or
``{"sent": False, "error": "<reason>"}`` so callers persist-then-mail without
losing the payload when the mail path is down.
"""

from __future__ import annotations

import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Any


def smtp_configured() -> bool:
    return bool(
        str(os.getenv("DATAFLOW_SMTP_USERNAME") or "").strip()
        and str(os.getenv("DATAFLOW_SMTP_PASSWORD") or "").strip()
    )


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def send_mail(
    *,
    to: str,
    subject: str,
    text_body: str,
    reply_to: str = "",
) -> dict[str, Any]:
    """Send one plain-text email via SMTP. Returns a result dict, never raises."""
    host = str(os.getenv("DATAFLOW_SMTP_HOST") or "smtp.zoho.com").strip()
    port = _env_int("DATAFLOW_SMTP_PORT", 587)
    username = str(os.getenv("DATAFLOW_SMTP_USERNAME") or "").strip()
    password = str(os.getenv("DATAFLOW_SMTP_PASSWORD") or "").strip()
    security = str(os.getenv("DATAFLOW_SMTP_SECURITY") or "starttls").strip().lower()
    timeout = _env_int("DATAFLOW_SMTP_TIMEOUT_SEC", 10)
    from_addr = str(os.getenv("DATAFLOW_SMTP_FROM") or "").strip() or username

    if not host or not username or not password or not from_addr:
        return {"sent": False, "error": "smtp_not_configured"}
    to = str(to or "").strip()
    if not to:
        return {"sent": False, "error": "missing_recipient"}

    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to
    msg["Subject"] = subject
    if reply_to:
        msg["Reply-To"] = reply_to.strip()
    msg.set_content(str(text_body or ""))

    try:
        if security == "ssl":
            with smtplib.SMTP_SSL(
                host, port, timeout=timeout, context=ssl.create_default_context()
            ) as smtp:
                smtp.login(username, password)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=timeout) as smtp:
                if security != "none":
                    smtp.starttls(context=ssl.create_default_context())
                smtp.login(username, password)
                smtp.send_message(msg)
    except Exception as exc:  # network/auth/refusal — caller keeps the lead
        return {"sent": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"sent": True, "error": ""}
