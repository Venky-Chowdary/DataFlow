"""P0-2 — EmailSender driver contract: resend / smtp / console.

Console driver refuses in production; resend maps 429/5xx to typed errors,
sends an Idempotency-Key, and never puts the API key or body in logs.
"""

from __future__ import annotations

import pytest

from services import email_sender
from services.email_sender import (
    ConsoleEmailSender,
    EmailProviderError,
    EmailQuotaExceeded,
    OutboundEmail,
    ResendEmailSender,
    email_provider_name,
    get_email_sender,
    reset_email_sender,
)


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()
    yield
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()


def _mail(**kw) -> OutboundEmail:
    return OutboundEmail(
        to=kw.get("to", "user@example.com"),
        subject=kw.get("subject", "Your code"),
        text=kw.get("text", "code: 123456"),
        html=kw.get("html", "<p>code</p>"),
        idempotency_key=kw.get("idempotency_key", "idem-1"),
    )


def test_provider_defaults_console_outside_production(monkeypatch):
    monkeypatch.delenv("DATAWRAP_EMAIL_PROVIDER", raising=False)
    monkeypatch.delenv("DATAFLOW_EMAIL_PROVIDER", raising=False)
    monkeypatch.setattr(email_sender, "is_production", lambda: False)
    assert email_provider_name() == "console"
    assert get_email_sender().name == "console"


def test_provider_defaults_resend_in_production(monkeypatch):
    monkeypatch.delenv("DATAWRAP_EMAIL_PROVIDER", raising=False)
    monkeypatch.delenv("DATAFLOW_EMAIL_PROVIDER", raising=False)
    monkeypatch.setattr(email_sender, "is_production", lambda: True)
    assert email_provider_name() == "resend"
    assert get_email_sender().name == "resend"


def test_console_driver_refuses_in_production(monkeypatch):
    monkeypatch.setattr(email_sender, "is_production", lambda: True)
    with pytest.raises(EmailProviderError):
        ConsoleEmailSender()
    with pytest.raises(EmailProviderError):
        ConsoleEmailSender.send(
            object.__new__(ConsoleEmailSender), _mail()
        )


def test_console_captures_metadata(monkeypatch):
    monkeypatch.setattr(email_sender, "is_production", lambda: False)
    sender = ConsoleEmailSender()
    assert sender.send(_mail())["ok"]
    assert ConsoleEmailSender.OUTBOX[-1].to == "user@example.com"


def test_resend_success_maps_response(monkeypatch):
    monkeypatch.setenv("DATAWRAP_RESEND_API_KEY", "re_test_key")
    calls = {}

    def fake_post(url, payload, headers=None, timeout=None):
        calls["url"] = url
        calls["payload"] = payload
        calls["headers"] = headers
        calls["timeout"] = timeout
        return {"ok": True, "status": 200, "body": '{"id":"email_abc"}'}

    monkeypatch.setattr(
        "services.notification_service._http_post", fake_post
    )
    out = ResendEmailSender().send(_mail())
    assert out["ok"] and out["message_id"] == "email_abc"
    assert calls["payload"]["html"] == "<p>code</p>"
    assert calls["payload"]["reply_to"] == "contact@datawrap.io"
    assert calls["headers"]["Idempotency-Key"] == "idem-1"
    assert calls["headers"]["Authorization"] == "Bearer re_test_key"
    assert calls["timeout"] and calls["timeout"] <= 10.0


def test_resend_429_maps_to_quota_error(monkeypatch):
    monkeypatch.setenv("DATAWRAP_RESEND_API_KEY", "re_test_key")
    monkeypatch.setattr(
        "services.notification_service._http_post",
        lambda *a, **k: {"ok": True, "status": 429, "body": '{"retry_after": 30}'},
    )
    with pytest.raises(EmailQuotaExceeded) as exc:
        ResendEmailSender().send(_mail())
    assert exc.value.code == "email_quota_exceeded"
    assert exc.value.retry_after_sec == 30


def test_resend_5xx_retryable_4xx_permanent(monkeypatch):
    monkeypatch.setenv("DATAWRAP_RESEND_API_KEY", "re_test_key")
    monkeypatch.setattr(
        "services.notification_service._http_post",
        lambda *a, **k: {"ok": True, "status": 500, "body": "{}"},
    )
    with pytest.raises(EmailProviderError) as exc:
        ResendEmailSender().send(_mail())
    assert exc.value.retryable is True

    monkeypatch.setattr(
        "services.notification_service._http_post",
        lambda *a, **k: {"ok": True, "status": 403, "body": "{}"},
    )
    with pytest.raises(EmailProviderError) as exc:
        ResendEmailSender().send(_mail())
    assert exc.value.retryable is False


def test_resend_missing_key_is_permanent(monkeypatch):
    monkeypatch.delenv("DATAWRAP_RESEND_API_KEY", raising=False)
    monkeypatch.delenv("DATAFLOW_RESEND_API_KEY", raising=False)
    with pytest.raises(EmailProviderError) as exc:
        ResendEmailSender().send(_mail())
    assert exc.value.retryable is False


def test_smtp_driver_delegates(monkeypatch):
    captured = {}

    def fake_smtp(recipients, subject, body, smtp_cfg=None, **kw):
        captured.update(recipients=recipients, subject=subject, body=body, **kw)
        return {"ok": True}

    monkeypatch.setattr("services.notification_service._smpt_send", fake_smtp)
    from services.email_sender import SmtpEmailSender

    assert SmtpEmailSender().send(_mail())["ok"]
    assert captured["html"] == "<p>code</p>"
    assert captured["reply_to"] == "contact@datawrap.io"


def test_unknown_provider_raises(monkeypatch):
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "pigeon")
    with pytest.raises(EmailProviderError):
        get_email_sender()
