"""Transactional email sender for the signup/auth surface (P0-2).

One ``EmailSender`` interface, three drivers selected by
``DATAWRAP_EMAIL_PROVIDER`` (``DATAFLOW_`` fallback via ``getenv_brand``):

- ``resend``  — Resend API (https://api.resend.com/emails). Default in
  production; reuses the egress-guarded HTTP path in
  ``services.notification_service`` rather than a second HTTP client.
- ``smtp``    — the existing SMTP transport (Zoho SMTP is a local/dev
  fallback; transactional volume belongs on the API provider).
- ``console`` — dev/test capture; refuses to run in production.

Provider default: explicit ``DATAWRAP_EMAIL_PROVIDER`` wins; unset →
``resend`` in production, ``console`` elsewhere so a developer box sends to
the outbox capture without any credentials.

Contract guarantees:
- HTML + plain-text parts; ``Reply-To`` support; per-send ``Idempotency-Key``.
- Typed errors: 429 → :class:`EmailQuotaExceeded`, other 4xx → permanent
  :class:`EmailProviderError`, 5xx/transport → retryable.
- The API key and message bodies are never logged (bodies may carry OTPs).
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol

from services.brand_env import getenv_brand
from services.platform_config import is_production

logger = logging.getLogger(__name__)

_RESEND_ENDPOINT = "https://api.resend.com/emails"
_RESEND_TIMEOUT_SEC = 10.0


class EmailSendError(Exception):
    """Base class for send failures. ``code`` is the public error code."""

    code = "email_send_failed"

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


class EmailQuotaExceeded(EmailSendError):
    """Provider rate-limit / account quota — caller surfaces retry_after."""

    code = "email_quota_exceeded"

    def __init__(self, message: str, *, retry_after_sec: int = 60) -> None:
        super().__init__(message, retryable=True)
        self.retry_after_sec = max(1, int(retry_after_sec))


class EmailProviderError(EmailSendError):
    """Non-quota provider failure (bad key, rejected payload, 5xx, transport)."""

    code = "email_provider_error"


@dataclass(frozen=True)
class OutboundEmail:
    to: str
    subject: str
    text: str
    html: str | None = None
    reply_to: str | None = None
    idempotency_key: str | None = None
    headers: dict[str, str] = field(default_factory=dict)


class EmailSender(Protocol):
    """One transactional send. Raises EmailSendError subclasses on failure."""

    def send(self, email: OutboundEmail) -> dict[str, Any]: ...


def _email_from() -> str:
    return (getenv_brand("EMAIL_FROM") or "Datawrap <noreply@datawrap.io>").strip()


def _email_reply_to() -> str:
    return (getenv_brand("EMAIL_REPLY_TO") or "contact@datawrap.io").strip()


class ResendEmailSender:
    """Resend API driver — shares notification_service's egress-guarded POST."""

    name = "resend"

    def send(self, email: OutboundEmail) -> dict[str, Any]:
        api_key = (getenv_brand("RESEND_API_KEY") or "").strip()
        if not api_key:
            raise EmailProviderError(
                "Resend API key not configured (DATAWRAP_RESEND_API_KEY)",
                retryable=False,
            )
        payload: dict[str, Any] = {
            "from": _email_from(),
            "to": [email.to],
            "subject": email.subject,
            "text": email.text,
        }
        if email.html:
            payload["html"] = email.html
        reply_to = email.reply_to or _email_reply_to()
        if reply_to:
            payload["reply_to"] = reply_to
        headers = {"Authorization": f"Bearer {api_key}"}
        headers["Idempotency-Key"] = email.idempotency_key or uuid.uuid4().hex

        from services.notification_service import _http_post

        result = _http_post(
            _RESEND_ENDPOINT,
            payload,
            headers=headers,
            timeout=_RESEND_TIMEOUT_SEC,
        )
        status = int(result.get("status") or 0)
        if not result.get("ok"):
            raise EmailProviderError(
                "Resend request failed (transport)",
                retryable=True,
            )
        if status in (200, 201, 202):
            message_id = ""
            try:
                message_id = str(json.loads(result.get("body") or "{}").get("id") or "")
            except (ValueError, TypeError):
                message_id = ""
            return {"ok": True, "provider": self.name, "message_id": message_id}
        retry_after = _retry_after_hint(result.get("body"))
        if status == 429:
            raise EmailQuotaExceeded(
                "Resend rate limit reached", retry_after_sec=retry_after or 60
            )
        # The body can echo the recipient list; log only the status class.
        logger.warning("Resend send rejected (status=%s)", status)
        retryable = status >= 500
        raise EmailProviderError(
            f"Resend send rejected (status={status})", retryable=retryable
        )


def _retry_after_hint(body: Any) -> int:
    try:
        data = json.loads(body or "{}")
        for key in ("retry_after", "retryAfter", "retry_after_sec"):
            if isinstance(data.get(key), (int, float)):
                return int(data[key])
    except (ValueError, TypeError):
        pass
    return 0


class SmtpEmailSender:
    """SMTP driver — the existing transport, for local/dev and self-hosted."""

    name = "smtp"

    def send(self, email: OutboundEmail) -> dict[str, Any]:
        from services.notification_service import _smpt_send

        result = _smpt_send(
            [email.to],
            email.subject,
            email.text,
            html=email.html,
            reply_to=email.reply_to or _email_reply_to(),
        )
        if result.get("ok"):
            return {"ok": True, "provider": self.name}
        raise EmailProviderError("SMTP send failed", retryable=True)


class ConsoleEmailSender:
    """Captures mail in-process for dev/test. Never permitted in production.

    Only metadata is logged — never bodies (they may carry an OTP or a
    password link). Tests read :data:`OUTBOX` (capped).
    """

    name = "console"
    OUTBOX: list[OutboundEmail] = []
    _CAP = 500

    def __init__(self) -> None:
        if is_production():
            raise EmailProviderError(
                "console email driver is forbidden in production", retryable=False
            )

    def send(self, email: OutboundEmail) -> dict[str, Any]:
        if is_production():
            raise EmailProviderError(
                "console email driver is forbidden in production", retryable=False
            )
        if len(self.OUTBOX) >= self._CAP:
            self.OUTBOX.pop(0)
        self.OUTBOX.append(email)
        logger.info(
            "console mailer captured (to=%s subject=%s)",
            email.to,
            email.subject[:80],
        )
        return {"ok": True, "provider": self.name}


def email_provider_name() -> str:
    raw = (getenv_brand("EMAIL_PROVIDER") or "").strip().lower()
    if raw:
        return raw
    return "resend" if is_production() else "console"


_SENDER: EmailSender | None = None


def get_email_sender() -> EmailSender:
    """Resolve the configured driver. Cached per process."""
    global _SENDER
    if _SENDER is not None:
        return _SENDER
    provider = email_provider_name()
    if provider == "resend":
        _SENDER = ResendEmailSender()
    elif provider == "smtp":
        _SENDER = SmtpEmailSender()
    elif provider == "console":
        _SENDER = ConsoleEmailSender()
    else:
        raise EmailProviderError(
            f"unknown DATAWRAP_EMAIL_PROVIDER {provider!r} (resend|smtp|console)",
            retryable=False,
        )
    return _SENDER


def reset_email_sender() -> None:
    """Test hook — drop the cached driver so env changes take effect."""
    global _SENDER
    _SENDER = None
