"""D130 — a refused auth mail is visible to an operator, with its cause.

QA (prod, signup enabled): signup OTPs and forgot-password links never
arrived; the inbox showed no mail and there was no tool to see the provider's
answer. The send path already names Datawrap in its User-Agent and types a
403 as a permanent refusal (77b56289a), but the outbox row was marked
``failed`` with no reason and the ``mail.send_failed`` audit event carried
only ip/purpose/attempt — a Cloudflare edge block, a bad key and a transport
blip all looked the same, and nothing listed them.

Here the provider is stubbed at the HTTP layer with the exact refusal
(``403`` body ``error code: 1010``); nothing leaves the process.
"""

from __future__ import annotations

import io
import urllib.error
from email.message import Message

import pytest

from services import mail_outbox, notification_service, signup_service


class _Opener:
    def __init__(self, state):
        self.state = state

    def open(self, req, timeout=None):
        self.state["requests"].append(req)
        hdrs = Message()
        raise urllib.error.HTTPError(
            "https://api.resend.com/emails", 403, "Forbidden", hdrs, io.BytesIO(b"error code: 1010")
        )


@pytest.fixture
def world(tmp_path, monkeypatch):
    state: dict = {"requests": [], "events": []}
    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(mail_outbox, "mongo_database", lambda: None)
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "resend")
    monkeypatch.setenv("DATAWRAP_RESEND_API_KEY", "re_d130_test_key")
    monkeypatch.setattr(notification_service, "_allowed_url", lambda url: True)
    monkeypatch.setattr(
        notification_service.urllib.request, "build_opener", lambda *h: _Opener(state)
    )
    from services import email_sender

    email_sender.reset_email_sender()
    monkeypatch.setattr(signup_service, "consume_send_slot", lambda: None)

    def _event(name, **kw):
        state["events"].append((name, kw))

    monkeypatch.setattr(signup_service, "record_security_event", _event)
    yield state
    email_sender.reset_email_sender()


def _enqueue(purpose: str = "signup.verify") -> dict:
    return mail_outbox.enqueue(
        to="ada@example.com", subject="Your code", text="code 123456", html="<p>123456</p>",
        idempotency_key=f"d130:{purpose}", purpose=purpose,
    )


def test_an_edge_refusal_marks_the_row_failed_with_its_cause_and_audits_it(world):
    row = _enqueue()
    signup_service._deliver_outbox_row(row, actor_ip="203.0.113.9")

    # The request left with a real agent, not Python-urllib.
    sent = world["requests"]
    assert len(sent) == 1
    assert sent[0].get_header("User-agent") == notification_service.USER_AGENT
    assert "Python-urllib" not in sent[0].get_header("User-agent")

    stored = mail_outbox.recent_failures()
    assert len(stored) == 1, stored
    failed = stored[0]
    assert failed["status"] == "failed"
    assert failed["attempts"] == 0  # a 403 is permanent: no retries were spent
    assert "status=403" in failed["last_error"]
    assert "edge_error_1010" in failed["last_error"]
    assert failed["to"] != "ada@example.com" and "@" in failed["to"]  # masked
    assert "text_enc" not in failed and "html_enc" not in failed

    names = [n for n, _ in world["events"]]
    assert names == ["mail.send_failed"], names
    details = world["events"][0][1]["details"]
    assert details["status"] == 403
    assert details["retryable"] is False
    assert details["terminal"] is True
    assert details["provider"] == "resend"
    assert "edge_error_1010" in details["error"]
    assert "ada@example.com" not in str(details)


def test_a_reset_link_refusal_is_audited_under_its_own_event(world):
    row = _enqueue("password.reset")
    signup_service._deliver_outbox_row(row)
    names = [n for n, _ in world["events"]]
    assert names == ["password.reset.send_failed"]
    assert world["events"][0][1]["details"]["status"] == 403
    assert mail_outbox.counts()["failed"] == 1


def test_ops_mail_endpoint_shows_the_failure_to_a_platform_admin(world, monkeypatch):
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)
    row = _enqueue()
    signup_service._deliver_outbox_row(row)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as client:
        res = client.get("/api/v1/ops/mail")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["provider"] == "resend"
    assert body["user_agent"] == notification_service.USER_AGENT
    assert body["counts"]["failed"] == 1
    assert body["healthy"] is False
    assert body["recent_failures"][0]["purpose"] == "signup.verify"
    assert "edge_error_1010" in body["recent_failures"][0]["last_error"]
    assert "ada@example.com" not in res.text


def test_ops_mail_endpoint_is_platform_admin_only(world, monkeypatch):
    from services.tenant_scope import TenantScope
    import src.routers.ops_router as ops

    restricted = TenantScope(actor="owner@example.test", unrestricted=False, enforced=True,
                             memberships=(("ws-1", "admin"),))
    monkeypatch.setattr("services.workspace_access.tenant_scope", lambda request, *a, **k: restricted)
    denied: list = []
    monkeypatch.setattr("services.audit_coverage.record_authz_denial",
                        lambda request, **kw: denied.append(kw))
    from fastapi import HTTPException
    from starlette.requests import Request

    request = Request({"type": "http", "method": "GET", "path": "/api/v1/ops/mail",
                       "headers": [], "query_string": b"", "state": {}})
    import asyncio

    with pytest.raises(HTTPException) as exc:
        asyncio.run(ops.get_mail_outbox_health(request, limit=20))
    assert exc.value.status_code == 403
    assert denied and denied[0]["required_permission"] == "ops.mail.read"


def test_transient_failures_keep_their_cause_on_the_retry_row(world, monkeypatch):
    from services.email_sender import EmailProviderError

    row = _enqueue()
    monkeypatch.setattr(signup_service.threading, "Timer",
                        lambda *a, **k: type("T", (), {"start": lambda self: None,
                                                        "daemon": False})())
    signup_service._outbox_send_failed(
        row, EmailProviderError("Resend send rejected (status=503)", retryable=True, status=503),
        claim_token=None,
    )
    pending = mail_outbox.recent_failures()
    assert pending and pending[0]["status"] == "queued" and pending[0]["attempts"] == 1
    assert "status=503" in pending[0]["last_error"]
    details = world["events"][0][1]["details"]
    assert details["retryable"] is True and details["terminal"] is False and details["status"] == 503
