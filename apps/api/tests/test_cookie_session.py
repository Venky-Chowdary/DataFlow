"""P3 — __Host-dw_session cookie auth, CSRF double-submit, session mgmt."""

from __future__ import annotations

import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
import services.signup_service as signup_service
from services import signup_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender
from tests.helpers.mail_wait import drain_mail_lane


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAWRAP_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "cookie-session-test-secret-value")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "strong-password-123")
    monkeypatch.setenv("DATAFLOW_ALLOW_DEV_USER", "0")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "50")
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(signup_service.mx_check, "check_domain", lambda d: {"ok": True})
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    import src.services.auth_service as auth_mod
    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)
    auth_mod._ADMIN_USER_CACHE = None
    auth_mod._ADMIN_CACHE_KEY = None
    from services.auth_rate_limit import reset_auth_rate_limits
    from services import auth_state, email_quota
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
    app.include_router(router)
    # Secure cookies only travel over https — TestClient must use it.
    return TestClient(app, base_url="https://testserver")


def _signup_and_verify(c: TestClient, email: str = "ada@example.com") -> dict:
    r = c.post("/api/v1/auth/signup", json={
        "email": email, "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"), "device_id": "d1"})
    assert r.status_code == 202
    challenge_id = r.json()["challenge_id"]
    drain_mail_lane()
    code = re.search(r"\b(\d{6})\b", ConsoleEmailSender.OUTBOX[-1].text).group(1)
    resp = c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": challenge_id, "code": code,
                        "device_id": "d1"})
    assert resp.status_code == 200
    return resp.json()


def test_verify_sets_session_and_csrf_cookies():
    c = _client()
    _signup_and_verify(c)
    assert c.cookies.get("__Host-dw_session")
    assert c.cookies.get("dw_csrf")
    # session cookie is HttpOnly; the csrf cookie must be readable by JS
    assert "__Host-dw_session" in c.cookies


def test_cookie_auth_lists_own_sessions():
    c = _client()
    _signup_and_verify(c)
    resp = c.get("/api/v1/auth/sessions")
    assert resp.status_code == 200
    sessions = resp.json()["sessions"]
    assert len(sessions) == 1
    assert sessions[0]["current"] is True
    assert sessions[0]["device_id"] == "d1"


def test_mutating_request_without_csrf_is_403():
    c = _client()
    _signup_and_verify(c)
    jti = c.cookies["__Host-dw_session"]
    resp = c.delete(f"/api/v1/auth/sessions/{jti}")
    assert resp.status_code == 403
    assert "CSRF" in resp.json()["detail"]


def test_mutating_request_with_csrf_passes():
    c = _client()
    _signup_and_verify(c)
    jti = c.cookies["__Host-dw_session"]
    csrf = c.cookies["dw_csrf"]
    resp = c.delete(f"/api/v1/auth/sessions/{jti}",
                    headers={"x-csrf-token": csrf})
    assert resp.status_code == 200
    # the session is gone — a further authed call is refused
    resp = c.get("/api/v1/auth/sessions")
    assert resp.status_code == 401


def test_csrf_compare_is_exact_not_prefix():
    c = _client()
    _signup_and_verify(c)
    jti = c.cookies["__Host-dw_session"]
    csrf = c.cookies["dw_csrf"]
    # tokens that share a prefix, differ in length, or are empty must all 403 —
    # a naive startswith/substring compare would pass the prefix case
    for bad in (csrf[:-1], csrf + "x", csrf[: len(csrf) // 2], "", " "):
        resp = c.delete(f"/api/v1/auth/sessions/{jti}",
                        headers={"x-csrf-token": bad})
        assert resp.status_code == 403, bad
    # GET is exempt from the double-submit check even with a bad header
    resp = c.get("/api/v1/auth/sessions", headers={"x-csrf-token": "bogus"})
    assert resp.status_code == 200


def test_public_post_unaffected_by_stale_cookie():
    c = _client()
    _signup_and_verify(c)
    # resend is a public route — a cookie without csrf must NOT 403 it
    resp = c.post("/api/v1/auth/resend",
                  json={"challenge_id": "otc_nonexistent"})
    assert resp.status_code in (202, 400, 404, 410)


def test_revoke_other_sessions_keeps_current():
    c = _client()
    _signup_and_verify(c)
    from services.auth_state import get_auth_state
    state = get_auth_state()
    state.create_session(user_id="ada@example.com", device_id="other")
    resp = c.delete("/api/v1/auth/sessions",
                    headers={"x-csrf-token": c.cookies["dw_csrf"]})
    assert resp.status_code == 200
    assert resp.json()["revoked"] == 1
    assert len(c.get("/api/v1/auth/sessions").json()["sessions"]) == 1


def test_revoked_cookie_session_not_accepted():
    c = _client()
    _signup_and_verify(c)
    jti = c.cookies["__Host-dw_session"]
    from services.auth_state import get_auth_state
    get_auth_state().revoke_session(jti)
    assert c.get("/api/v1/auth/sessions").status_code == 401


def test_rotate_session_preserves_identity_and_kills_old():
    c = _client()
    _signup_and_verify(c)
    jti = c.cookies["__Host-dw_session"]
    csrf = c.cookies["dw_csrf"]
    from services.auth_state import get_auth_state
    state = get_auth_state()
    new = state.rotate_session(jti)
    assert new["jti"] != jti
    assert new["user_id"] == "ada@example.com"
    assert new["csrf"] == csrf
    assert not state.session_active(jti)
    assert state.session_active(new["jti"])


def test_no_cookie_no_session_returns_401():
    c = _client()
    resp = c.get("/api/v1/auth/sessions")
    assert resp.status_code == 401


def test_verify_returns_login_shaped_bearer_session_d153():
    """D153: the web client signs every call with a Bearer token, so verify-email
    must hand back the same session payload login does — a cookie alone left the
    browser holding an empty token and the first click bounced to sign-in."""
    from src.routers.auth_router import router as auth_router

    c = _client()
    c.app.include_router(auth_router, prefix="/api/v1")
    body = _signup_and_verify(c)
    assert body["token"], "verify-email must return a usable session token"
    assert body["expires_at"] > 0
    assert body["user"]["email"] == "ada@example.com"
    assert body["must_change_password"] is False

    # A fresh browser context with no cookies: the Bearer alone authenticates,
    # exactly as it does after a password login.
    fresh = TestClient(c.app, base_url="https://testserver")
    me = fresh.get("/api/v1/auth/me",
                   headers={"Authorization": f"Bearer {body['token']}"})
    assert me.status_code == 200, me.text
    assert me.json()["email"] == "ada@example.com"
    assert fresh.get("/api/v1/auth/me").status_code == 401
