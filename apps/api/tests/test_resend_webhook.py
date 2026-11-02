"""P2-10 — Resend Svix webhook: signature verify + bounce suppression."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.denylist_store as denylist_store
import services.form_token as form_token_mod
import services.signup_service as signup_service
from services import signup_store, user_store
from services.brand_env import getenv_brand
from services.email_sender import ConsoleEmailSender, reset_email_sender

SECRET_B64 = base64.b64encode(b"test-webhook-key-32bytes-padded!!").decode()


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setenv("DATAWRAP_RESEND_WEBHOOK_SECRET", f"whsec_{SECRET_B64}")
    monkeypatch.setattr(denylist_store, "_path", lambda: tmp_path / "deny.json")
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(signup_service.mx_check, "check_domain", lambda d: {"ok": True})
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
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


def _signed_headers(body: bytes, *, secret_b64: str = SECRET_B64,
                    msg_id: str = "msg_1", ts: int | None = None) -> dict:
    ts = int(time.time()) if ts is None else ts
    signed = f"{msg_id}.{ts}.".encode() + body
    key = base64.b64decode(secret_b64)
    sig = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode()
    return {"svix-id": msg_id, "svix-timestamp": str(ts),
            "svix-signature": f"v1,{sig}"}


def test_unsigned_payload_rejected():
    body = json.dumps({"type": "email.bounced", "data": {"to": ["a@b.com"]}}).encode()
    resp = _client().post("/api/v1/auth/resend-webhook", content=body)
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "invalid_signature"


def test_forged_signature_rejected():
    body = json.dumps({"type": "email.bounced", "data": {"to": ["a@b.com"]}}).encode()
    headers = _signed_headers(body, secret_b64=base64.b64encode(b"wrong-key").decode())
    resp = _client().post("/api/v1/auth/resend-webhook", content=body, headers=headers)
    assert resp.status_code == 401
    assert not denylist_store.is_denied("a@b.com", kind="email")


def test_stale_timestamp_rejected():
    body = json.dumps({"type": "email.bounced", "data": {"to": ["a@b.com"]}}).encode()
    headers = _signed_headers(body, ts=int(time.time()) - 900)
    resp = _client().post("/api/v1/auth/resend-webhook", content=body, headers=headers)
    assert resp.status_code == 401


def test_bounce_suppresses_email_and_blocks_signup():
    body = json.dumps({"type": "email.bounced",
                       "data": {"to": ["bounced@example.com"]}}).encode()
    c = _client()
    resp = c.post("/api/v1/auth/resend-webhook", content=body,
                  headers=_signed_headers(body))
    assert resp.status_code == 200
    assert resp.json()["suppressed"] == 1
    assert denylist_store.is_denied("bounced@example.com", kind="email")
    # the suppressed address can no longer sign up
    r = c.post("/api/v1/auth/signup", json={
        "email": "bounced@example.com", "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup")})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "blocked_address"


def test_complaint_also_suppresses():
    body = json.dumps({"type": "email.complained",
                       "data": {"to": ["spammy@example.com"]}}).encode()
    resp = _client().post("/api/v1/auth/resend-webhook", content=body,
                          headers=_signed_headers(body))
    assert resp.status_code == 200
    assert denylist_store.is_denied("spammy@example.com", kind="email")


def test_unrelated_event_ignored():
    body = json.dumps({"type": "email.delivered", "data": {"to": ["a@b.com"]}}).encode()
    resp = _client().post("/api/v1/auth/resend-webhook", content=body,
                          headers=_signed_headers(body))
    assert resp.status_code == 200
    assert resp.json() == {"ignored": True}
    assert not denylist_store.is_denied("a@b.com", kind="email")
