"""Welcome mail for self-serve signups: branded, queued once per new account
after verification, off the request path, and never able to fail signup."""

from __future__ import annotations

import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
import services.signup_service as signup_service
from services import mail_outbox, signup_store, team_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender
from tests.helpers.mail_wait import drain_mail_lane


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setattr(signup_service.mx_check, "check_domain", lambda d: {"ok": True})
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "_BACKFILLED", False)
    import services.org_store as org_store
    monkeypatch.setattr(org_store, "_orgs_path", lambda: tmp_path / "orgs.json")
    monkeypatch.setattr(org_store, "_plans_path", lambda: tmp_path / "plans.json")
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(mail_outbox, "mongo_database", lambda: None)
    from services import auth_state, email_quota
    auth_state.reset_auth_state()
    email_quota._reset_backends()
    reset_email_sender()
    drain_mail_lane()
    ConsoleEmailSender.OUTBOX.clear()
    yield
    reset_email_sender()


def _client() -> TestClient:
    from services.api_errors import ApiError, api_error_handler
    from src.routers.signup_router import router

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _signup_and_verify(c: TestClient, email: str = "ada@example.com"):
    resp = c.post("/api/v1/auth/signup", json={
        "email": email, "password": "a-very-long-password", "name": "Ada",
        "device_id": "d1", "form_token": form_token_mod.issue("signup")})
    assert resp.status_code == 202
    drain_mail_lane()
    code = re.search(r"\b(\d{6})\b", ConsoleEmailSender.OUTBOX[-1].text).group(1)
    return c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": resp.json()["challenge_id"], "code": code})


def _welcomes(to: str = "ada@example.com"):
    drain_mail_lane()
    return [m for m in ConsoleEmailSender.OUTBOX if m.to == to and m.subject == "Welcome to Datawrap"]


def test_verify_sends_one_branded_welcome():
    resp = _signup_and_verify(_client())
    assert resp.status_code == 200
    [mail] = _welcomes()
    assert "Ada's workspace is set up" in mail.text and "You are its admin" in mail.text
    assert mail.idempotency_key == "welcome:ada@example.com"
    assert "#072B29" in mail.html and "<img" not in mail.html.lower()


def test_welcome_is_once_per_account_even_if_queued_again():
    _signup_and_verify(_client())
    assert signup_service.queue_welcome(email="ada@example.com", name="Ada",
                                        workspace_name="Ada's workspace") is False
    assert signup_service.queue_welcome(email="ADA@example.com", name="Ada",
                                        workspace_name="Ada's workspace") is False
    assert len(_welcomes()) == 1


def test_welcome_failure_never_fails_signup(monkeypatch):
    events = []
    real = signup_service.record_security_event
    monkeypatch.setattr(signup_service, "record_security_event",
                        lambda event, **kw: (events.append(event), real(event, **kw)))

    def boom(*_a, **_kw):
        raise RuntimeError("renderer down")

    monkeypatch.setattr(signup_service, "render_email",
                        lambda name, ctx=None: boom() if name == "welcome"
                        else __import__("services.email_templates", fromlist=["x"]).render_email(name, ctx))
    resp = _signup_and_verify(_client())
    assert resp.status_code == 200 and resp.json()["token"]
    assert _welcomes() == []
    assert "signup.welcome_failed" in events


def test_welcome_quota_is_taken_on_the_lane_not_the_request(monkeypatch):
    """A full send quota at verify time must not 429/503 the signup."""
    from services.email_sender import EmailQuotaExceeded

    calls = {"n": 0}
    real = signup_service.consume_send_slot

    def quota():
        calls["n"] += 1
        return real()

    monkeypatch.setattr(signup_service, "consume_send_slot", quota)
    c = _client()
    _signup_and_verify(c)
    before = calls["n"]

    def full():
        raise EmailQuotaExceeded("full", retry_after_sec=30)

    monkeypatch.setattr(signup_service, "consume_send_slot", full)
    assert signup_service.queue_welcome(email="bo@example.com", name="Bo",
                                        workspace_name="Bo's workspace") is True
    assert before >= 1
