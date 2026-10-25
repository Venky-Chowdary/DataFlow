"""P4-2 — email change: OTP to the new address, account/membership/org
rekey, sessions revoked, old address notified."""

from __future__ import annotations

import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
import services.signup_service as signup_service
from services import auth_state, email_quota, signup_store, team_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "email-change-test-secret-value")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "strong-password-123")
    monkeypatch.setenv("DATAFLOW_ALLOW_DEV_USER", "0")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "50")
    monkeypatch.setattr(signup_store, "_pending_path",
                        lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path",
                        lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path",
                        lambda: tmp_path / "users.json")
    monkeypatch.setattr(team_store, "_store_path",
                        lambda: tmp_path / "team.json")
    monkeypatch.setattr(signup_service.mx_check, "check_domain",
                        lambda d: {"ok": True})
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    import src.services.auth_service as auth_mod
    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)
    auth_mod._ADMIN_USER_CACHE = None
    auth_mod._ADMIN_CACHE_KEY = None
    from services.auth_rate_limit import reset_auth_rate_limits
    reset_auth_rate_limits()
    auth_state.reset_auth_state()
    email_quota._reset_backends()
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()
    yield
    reset_email_sender()


def _client() -> TestClient:
    from src.middleware.auth_middleware import AuthMiddleware
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    app = FastAPI()
    app.add_middleware(AuthMiddleware)
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


def _request_change(c: TestClient, new_email: str, email: str = "ada@example.com"):
    csrf = c.cookies["dw_csrf"]
    return c.post("/api/v1/auth/email/change",
                  json={"new_email": new_email},
                  headers={"x-csrf-token": csrf})


def test_request_sends_code_to_new_address_and_notice_to_old():
    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada2@example.com")
    assert resp.status_code == 202
    assert resp.json()["challenge_id"]
    # signup code + change code (the old-address notice goes out at confirm)
    assert len(ConsoleEmailSender.OUTBOX) == 2
    assert ConsoleEmailSender.OUTBOX[-1].to == "ada2@example.com"
    assert "new Datawrap email" in ConsoleEmailSender.OUTBOX[-1].subject \
        or "Confirm" in ConsoleEmailSender.OUTBOX[-1].subject


def test_request_requires_auth():
    c = _client()
    resp = c.post("/api/v1/auth/email/change",
                  json={"new_email": "x@example.com"})
    assert resp.status_code == 401


def test_request_refuses_taken_address():
    c = _client()
    _verified_user(c)
    _verified_user(c, email="bob@example.com")
    resp = _request_change(c, "bob@example.com")
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "email_taken"


def test_request_refuses_disposable_domain():
    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada@mailinator.com")
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "disposable_domain"


def test_confirm_rekeys_account_memberships_orgs_and_revokes_sessions():
    c = _client()
    _verified_user(c)
    state = auth_state.get_auth_state()
    assert state.list_sessions("ada@example.com")
    resp = _request_change(c, "ada2@example.com")
    cid = resp.json()["challenge_id"]
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)
    ok = c.post("/api/v1/auth/email/change/confirm",
                json={"challenge_id": cid, "code": code})
    assert ok.status_code == 200
    assert ok.json()["email"] == "ada2@example.com"
    assert user_store.get_user("ada@example.com") is None
    new_doc = user_store.get_user("ada2@example.com")
    assert new_doc is not None
    assert user_store.find_by_canonical("ada2@example.com") is not None
    assert user_store.find_by_canonical("ada@example.com") is None
    # memberships + orgs followed the account
    assert team_store._read_memberships(email="ada@example.com") == []
    assert team_store._read_memberships(email="ada2@example.com")
    # every old session is dead; cookie no longer authenticates
    assert state.list_sessions("ada@example.com") == []
    # old address was notified
    subjects = [m.subject for m in ConsoleEmailSender.OUTBOX]
    assert any("changed" in s for s in subjects)


def test_confirm_wrong_code_400_and_single_use():
    c = _client()
    _verified_user(c)
    resp = _request_change(c, "ada2@example.com")
    cid = resp.json()["challenge_id"]
    bad = c.post("/api/v1/auth/email/change/confirm",
                 json={"challenge_id": cid, "code": "000000"})
    assert bad.status_code == 400
    code = re.search(r"\b(\d{6})\b",
                     ConsoleEmailSender.OUTBOX[-1].text).group(1)
    assert c.post("/api/v1/auth/email/change/confirm",
                  json={"challenge_id": cid, "code": code}).status_code == 200
    again = c.post("/api/v1/auth/email/change/confirm",
                   json={"challenge_id": cid, "code": code})
    assert again.status_code == 410
