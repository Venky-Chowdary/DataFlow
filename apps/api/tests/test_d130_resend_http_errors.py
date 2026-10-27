"""D130 — Resend HTTP API sends: explicit User-Agent and typed refusals.

Cloudflare in front of api.resend.com refuses the default ``Python-urllib``
agent with 403 "error code: 1010", and ``_http_post`` used to swallow every
``HTTPError`` so ``ResendEmailSender`` reported a retryable "transport"
failure for a bad key, a blocked agent or a rejected payload alike. urlopen
is mocked throughout: no request leaves the process.
"""

from __future__ import annotations

import io
import logging
import urllib.error
from email.message import Message

import pytest

from services import notification_service
from services.email_sender import (
    EmailProviderError,
    EmailQuotaExceeded,
    OutboundEmail,
    ResendEmailSender,
)

_KEY = "re_d130_secret_key"


class _Resp:
    status = 200

    def __init__(self, body: bytes):
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _http_error(code: int, body: bytes, headers: dict[str, str] | None = None):
    hdrs = Message()
    for k, v in (headers or {}).items():
        hdrs[k] = v
    return urllib.error.HTTPError(
        "https://api.resend.com/emails", code, "refused", hdrs, io.BytesIO(body))


@pytest.fixture
def opener(monkeypatch):
    """Capture the outgoing Request; answer with ``state['outcome']``."""
    state: dict = {"requests": [], "outcome": _Resp(b'{"id":"msg_1"}')}

    class _Opener:
        def open(self, req, timeout=None):
            state["requests"].append(req)
            outcome = state["outcome"]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

    monkeypatch.setattr(notification_service, "_allowed_url", lambda url: True)
    monkeypatch.setattr(notification_service.urllib.request, "build_opener",
                        lambda *handlers: _Opener())
    monkeypatch.setenv("DATAWRAP_RESEND_API_KEY", _KEY)
    return state


def _mail() -> OutboundEmail:
    return OutboundEmail(to="ada@example.com", subject="Your code",
                         text="code: 123456", idempotency_key="idem-d130")


def test_outgoing_request_names_datawrap_not_python_urllib(opener):
    result = ResendEmailSender().send(_mail())
    assert result["message_id"] == "msg_1"
    ua = opener["requests"][0].get_header("User-agent")
    assert ua == "Datawrap/1.0 (+https://www.datawrap.io)"
    assert "urllib" not in ua.lower()


def test_cloudflare_403_is_permanent_and_carries_status(opener, caplog):
    opener["outcome"] = _http_error(403, b"error code: 1010")
    with caplog.at_level(logging.WARNING):
        with pytest.raises(EmailProviderError) as exc:
            ResendEmailSender().send(_mail())
    assert exc.value.retryable is False
    assert exc.value.status == 403
    assert "transport" not in str(exc.value)
    assert "edge_error_1010" in caplog.text
    assert _KEY not in caplog.text


@pytest.mark.parametrize("code,name", [(401, "invalid_api_key"), (422, "validation_error")])
def test_bad_key_and_rejected_payload_are_permanent(opener, caplog, code, name):
    opener["outcome"] = _http_error(
        code,
        b'{"statusCode": %d, "name": "%s", "message": "bad thing for ada@example.com"}'
        % (code, name.encode()))
    with caplog.at_level(logging.WARNING):
        with pytest.raises(EmailProviderError) as exc:
            ResendEmailSender().send(_mail())
    assert exc.value.retryable is False
    assert exc.value.status == code
    # Only the provider's error *name* is logged: never the key, the message
    # body or the recipient it echoes.
    assert name in caplog.text
    assert "ada@example.com" not in caplog.text
    assert "bad thing" not in caplog.text
    assert _KEY not in caplog.text


def test_429_is_quota_with_retry_after_header(opener):
    opener["outcome"] = _http_error(
        429, b'{"name": "rate_limit_exceeded"}', {"Retry-After": "7"})
    with pytest.raises(EmailQuotaExceeded) as exc:
        ResendEmailSender().send(_mail())
    assert exc.value.retryable is True
    assert exc.value.retry_after_sec == 7


def test_5xx_is_retryable(opener):
    opener["outcome"] = _http_error(503, b'{"name": "internal_server_error"}')
    with pytest.raises(EmailProviderError) as exc:
        ResendEmailSender().send(_mail())
    assert exc.value.retryable is True
    assert exc.value.status == 503


def test_network_error_stays_retryable_transport(opener):
    opener["outcome"] = urllib.error.URLError("connection refused")
    with pytest.raises(EmailProviderError) as exc:
        ResendEmailSender().send(_mail())
    assert exc.value.retryable is True
    assert exc.value.status == 0
    assert "transport" in str(exc.value)


def test_http_post_refusal_reports_status_and_redacted_body(opener):
    """Every other caller (webhooks, ServiceNow, SendGrid) sees a refusal as
    ``ok: False`` — never success — now with the status and a short body."""
    opener["outcome"] = _http_error(404, b'{"error": "no route for ops@example.com"}' + b"x" * 5000)
    result = notification_service._http_post("https://hooks.example.com/x", {"text": "hi"})
    assert result["ok"] is False
    assert result["status"] == 404
    assert "ops@example.com" not in result["body"]
    assert len(result["body"]) <= 500
    assert result["error"]
