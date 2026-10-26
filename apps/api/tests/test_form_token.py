"""P2-7 — honeypot + signed form-token: fake 202, nothing stored/mailed."""

from __future__ import annotations

import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token
import services.signup_service as signup_service
from services import signup_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(signup_service.mx_check, "check_domain", lambda d: {"ok": True})
    from services import auth_state, email_quota
    auth_state.reset_auth_state()
    email_quota._reset_backends()
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()
    yield
    reset_email_sender()


def _client() -> TestClient:
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _post(client, **kw):
    body = {"email": "ada@example.com", "password": "a-very-long-password",
            "form_token": kw.pop("form_token", None)}
    if body["form_token"] is None:
        monkeytok = form_token.issue("signup")
        body["form_token"] = monkeytok
    body.update(kw)
    return client.post("/api/v1/auth/signup", json=body)


def test_honeypot_filled_fake_202_nothing_sent():
    c = _client()
    resp = _post(c, honeypot="i am a bot")
    assert resp.status_code == 202
    assert ConsoleEmailSender.OUTBOX == []
    assert signup_store.get_pending_signup(email="ada@example.com") is None


def test_too_fast_post_fake_202(monkeypatch):
    monkeypatch.setattr(form_token, "MIN_FILL_MS", 60_000)
    c = _client()
    resp = _post(c)
    assert resp.status_code == 202
    assert ConsoleEmailSender.OUTBOX == []
    assert signup_store.get_pending_signup(email="ada@example.com") is None


def test_tampered_token_fake_202():
    c = _client()
    resp = _post(c, form_token="signup:123:nonce.forged")
    assert resp.status_code == 202
    assert ConsoleEmailSender.OUTBOX == []


def test_replayed_token_fake_202():
    monkeypatch_client = _client()
    tok = form_token.issue("signup")
    # first use would pass the gate; second is a replay
    import time as _t
    payload, _sig = tok.rsplit(".", 1)
    purp, ms, nonce = payload.split(":")
    import hmac, hashlib
    old_ms = int(time.time() * 1000) - 4000
    payload = f"signup:{old_ms}:{nonce}"
    from services.brand_env import getenv_brand
    sig = hmac.new(getenv_brand("AUTH_SECRET", "dev-change-me-before-production").encode(),
                   payload.encode(), hashlib.sha256).hexdigest()[:32]
    tok = f"{payload}.{sig}"
    # consume once via check(), then the API post must refuse as replay
    assert form_token.check(tok, purpose="signup")["ok"]
    resp = _post(monkeypatch_client, form_token=tok)
    assert resp.status_code == 202
    assert ConsoleEmailSender.OUTBOX == []


def test_issuer_endpoint_returns_token():
    c = _client()
    r = c.get("/api/v1/auth/form-token?purpose=signup")
    assert r.status_code == 200
    assert form_token.check(r.json()["form_token"], purpose="signup",
                            min_fill_ms=0)["ok"]
