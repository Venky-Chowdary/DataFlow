"""P4-1 — forgot/reset password: uniform 202, code verify, argon2id
rehash, all sessions revoked."""

from __future__ import annotations

import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
import services.signup_service as signup_service
from services import auth_state, email_quota, signup_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
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
    yield
    reset_email_sender()


def _client() -> TestClient:
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    return TestClient(app, base_url="https://testserver")


def _verified_user(c: TestClient, email: str = "ada@example.com") -> dict:
    r = c.post("/api/v1/auth/signup", json={
        "email": email, "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"), "device_id": "d1"})
    code = re.search(r"\b(\d{6})\b", ConsoleEmailSender.OUTBOX[-1].text).group(1)
    resp = c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": r.json()["challenge_id"],
                        "code": code, "device_id": "d1"})
    assert resp.status_code == 200
    return resp.json()


def test_forgot_returns_202_for_unknown_and_known_alike():
    c = _client()
    _verified_user(c)
    r_unknown = c.post("/api/v1/auth/password/forgot",
                       json={"email": "ghost@example.com",
                     "form_token": form_token_mod.issue("reset")})
    r_known = c.post("/api/v1/auth/password/forgot",
                     json={"email": "ada@example.com",
                     "form_token": form_token_mod.issue("reset")})
    assert r_unknown.status_code == r_known.status_code == 202
    for resp in (r_unknown, r_known):
        body = resp.json()
        assert set(body) == {"ok", "challenge_id", "resend_after_sec",
                             "expires_in_sec"}
        assert body["ok"] is True
        assert len(body["challenge_id"]) >= 10


def test_forgot_unknown_sends_no_mail():
    c = _client()
    _verified_user(c)
    before = len(ConsoleEmailSender.OUTBOX)
    c.post("/api/v1/auth/password/forgot",
           json={"email": "nobody@example.com",
                 "form_token": form_token_mod.issue("reset")})
    assert len(ConsoleEmailSender.OUTBOX) == before


def test_reset_flow_updates_password_and_revokes_sessions():
    c = _client()
    _verified_user(c)
    state = auth_state.get_auth_state()
    assert state.list_sessions("ada@example.com")
    r = c.post("/api/v1/auth/password/forgot",
               json={"email": "ada@example.com",
                     "form_token": form_token_mod.issue("reset")})
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)
    resp = c.post("/api/v1/auth/password/reset", json={
        "challenge_id": r.json()["challenge_id"], "code": code,
        "new_password": "an-even-better-password"})
    assert resp.status_code == 200
    creds = [c for c in user_store.credentials_for_auth()
             if c["email"] == "ada@example.com"]
    assert creds[0]["password_algo"] == "argon2id"
    assert state.list_sessions("ada@example.com") == []


def test_reset_with_fake_challenge_id_is_410():
    c = _client()
    _verified_user(c)
    r = c.post("/api/v1/auth/password/forgot",
               json={"email": "ghost@example.com",
                     "form_token": form_token_mod.issue("reset")})
    resp = c.post("/api/v1/auth/password/reset", json={
        "challenge_id": r.json()["challenge_id"], "code": "123456",
        "new_password": "an-even-better-password"})
    assert resp.status_code == 410
    assert resp.json()["error"]["code"] == "code_expired"


def test_reset_wrong_code_is_400_and_code_single_use():
    c = _client()
    _verified_user(c)
    r = c.post("/api/v1/auth/password/forgot",
               json={"email": "ada@example.com",
                     "form_token": form_token_mod.issue("reset")})
    cid = r.json()["challenge_id"]
    bad = c.post("/api/v1/auth/password/reset", json={
        "challenge_id": cid, "code": "000000",
        "new_password": "an-even-better-password"})
    assert bad.status_code == 400
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)
    ok = c.post("/api/v1/auth/password/reset", json={
        "challenge_id": cid, "code": code,
        "new_password": "an-even-better-password"})
    assert ok.status_code == 200
    again = c.post("/api/v1/auth/password/reset", json={
        "challenge_id": cid, "code": code,
        "new_password": "third-password-here"})
    assert again.status_code == 410


def test_reset_rejects_short_password_400():
    c = _client()
    _verified_user(c)
    r = c.post("/api/v1/auth/password/forgot",
               json={"email": "ada@example.com",
                     "form_token": form_token_mod.issue("reset")})
    resp = c.post("/api/v1/auth/password/reset", json={
        "challenge_id": r.json()["challenge_id"], "code": "123456",
        "new_password": "short"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_password"


def test_forgot_without_form_token_still_202_but_sends_nothing():
    c = _client()
    _verified_user(c)
    before = len(ConsoleEmailSender.OUTBOX)
    r = c.post("/api/v1/auth/password/forgot",
               json={"email": "ada@example.com"})
    # uniform 202 shape even though the bot wall refused the mail
    assert r.status_code == 202
    assert r.json()["challenge_id"]
    assert len(ConsoleEmailSender.OUTBOX) == before


def test_forgot_per_address_velocity_keeps_uniform_202(monkeypatch):
    monkeypatch.setenv("DATAWRAP_FORGOT_EMAIL_DAILY", "1")
    c = _client()
    _verified_user(c)
    for _ in range(3):
        r = c.post("/api/v1/auth/password/forgot",
                   json={"email": "ada@example.com",
                         "form_token": form_token_mod.issue("reset")})
        assert r.status_code == 202
        assert set(r.json()) == {"ok", "challenge_id",
                                 "resend_after_sec", "expires_in_sec"}
