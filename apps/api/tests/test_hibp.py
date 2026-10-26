"""P4-3 — HIBP pwned-password check: flag-gated, k-anonymous, fail-open."""

from __future__ import annotations

import hashlib
import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
import services.hibp as hibp
import services.signup_service as signup_service
from services import auth_state, email_quota, signup_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender
from tests.helpers.mail_wait import drain_mail_lane


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.delenv("DATAWRAP_HIBP_ENABLED", raising=False)
    monkeypatch.setattr(signup_store, "_pending_path",
                        lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path",
                        lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path",
                        lambda: tmp_path / "users.json")
    monkeypatch.setattr(signup_service.mx_check, "check_domain",
                        lambda d: {"ok": True})
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    auth_state.reset_auth_state()
    email_quota._reset_backends()
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()
    hibp._reset_for_tests()
    yield
    reset_email_sender()
    hibp._reset_for_tests()


def _client() -> TestClient:
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _range_response(count: int = 0, sha1_suffix: str = "DEADBEEF"):
    class _R:
        status_code = 200
        text = f"{sha1_suffix}:{count}\nAAAAA:3"

    return _R()


def _pwned_for(password: str, count: int):
    digest = hashlib.sha1(password.encode()).hexdigest().upper()
    return _range_response(count, sha1_suffix=digest[5:])


def test_off_by_default_never_calls_out():
    assert hibp.pwned_count("anything") == -1


def test_k_anonymity_only_prefix_leaves(monkeypatch):
    monkeypatch.setenv("DATAWRAP_HIBP_ENABLED", "1")
    seen: list[str] = []

    def spy(url, **kw):
        seen.append(url)
        return _pwned_for("my-secret-password", 42)

    count = hibp.pwned_count("my-secret-password", http_get=spy)
    assert count == 42
    full = hashlib.sha1(b"my-secret-password").hexdigest().upper()
    assert len(seen) == 1
    assert full[:5] in seen[0]
    assert full[5:] not in seen[0]
    assert "my-secret-password" not in seen[0]


def test_outage_fails_open(monkeypatch):
    monkeypatch.setenv("DATAWRAP_HIBP_ENABLED", "1")

    def boom(url, **kw):
        raise TimeoutError("hibp down")

    assert hibp.pwned_count("x", http_get=boom) == -1


def test_signup_refuses_pwned_password(monkeypatch):
    monkeypatch.setenv("DATAWRAP_HIBP_ENABLED", "1")
    password = "a-very-long-password"
    monkeypatch.setattr(
        hibp, "pwned_count", lambda p, **kw: 9000 if p == password else 0)
    resp = _client().post("/api/v1/auth/signup", json={
        "email": "ada@example.com", "password": password,
        "form_token": form_token_mod.issue("signup")})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "pwned_password"
    assert signup_store.get_pending_signup(email="ada@example.com") is None


def test_signup_proceeds_when_not_pwned(monkeypatch):
    monkeypatch.setenv("DATAWRAP_HIBP_ENABLED", "1")
    monkeypatch.setattr(hibp, "pwned_count", lambda p, **kw: 0)
    resp = _client().post("/api/v1/auth/signup", json={
        "email": "ada@example.com", "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"), "device_id": "d1"})
    assert resp.status_code == 202


def test_reset_refuses_pwned_password(monkeypatch):
    monkeypatch.setenv("DATAWRAP_HIBP_ENABLED", "1")
    c = _client()
    r = c.post("/api/v1/auth/signup", json={
        "email": "ada@example.com", "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"), "device_id": "d1"})
    drain_mail_lane()
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)
    assert c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": r.json()["challenge_id"],
                        "code": code, "device_id": "d1"}).status_code == 200
    c.post("/api/v1/auth/password/forgot",
           json={"email": "ada@example.com",
                 "form_token": form_token_mod.issue("reset")})
    drain_mail_lane()
    token = re.search(r"reset-password#token=([A-Za-z0-9_\-]+)",
                      ConsoleEmailSender.OUTBOX[-1].text).group(1)
    monkeypatch.setattr(hibp, "pwned_count", lambda p, **kw: 5)
    resp = c.post("/api/v1/auth/password/reset", json={
        "token": token,
        "new_password": "breached-but-long-password"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "pwned_password"
