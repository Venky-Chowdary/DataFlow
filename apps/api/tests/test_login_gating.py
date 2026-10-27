"""P1-6 — login refuses unverified (403 + fresh code) and banned (neutral).

Flag on: a pending signup gets code=email_unverified + challenge_id and a
mailed code, never a session. Banned accounts get the identical 401 a bad
password gets. Flag off: nothing changes — pending still hits 401.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import signup_store, user_store  # noqa: E402
from services.email_sender import ConsoleEmailSender, reset_email_sender  # noqa: E402
from tests.helpers.mail_wait import await_outbox  # noqa: E402


@pytest.fixture
def gating_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAWRAP_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "p16-secret-value")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "strong-password-123")
    monkeypatch.setenv("DATAFLOW_ALLOW_DEV_USER", "0")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "1000")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    # REQUIRE_AUTH marks production per is_production(); tests use console mail.
    monkeypatch.setenv("DATAWRAP_ENV", "test")

    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "_BACKFILLED", False)

    from services import auth_state, email_quota
    auth_state.reset_auth_state()
    email_quota._reset_backends()

    import src.services.auth_service as auth_mod
    from services.auth_rate_limit import reset_auth_rate_limits

    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)
    auth_mod._ADMIN_USER_CACHE = None
    auth_mod._ADMIN_CACHE_KEY = None
    reset_auth_rate_limits()
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()
    yield
    reset_email_sender()


def _client(flag_on=True):
    from src.middleware.auth_middleware import AuthMiddleware
    from src.routers.auth_router import router as auth_router
    from src.routers.signup_router import router as signup_router
    from services.api_errors import ApiError, api_error_handler

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.add_middleware(AuthMiddleware)
    app.include_router(auth_router, prefix="/api/v1")
    if flag_on:
        app.include_router(signup_router, prefix="/api/v1")
    return TestClient(app)


def _pending(email="pending@example.com"):
    signup_store.upsert_pending_signup(
        email=email, name="P", password_hash="h",
        ip="", device_id="", risk_score=0)
    return email


def test_pending_signup_gets_403_email_unverified_and_fresh_code(
        gating_env, monkeypatch):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    c = _client()
    _pending()
    resp = c.post("/api/v1/auth/login", json={
        "email": "pending@example.com", "password": "whatever-it-is-pending"})
    assert resp.status_code == 403
    err = resp.json()["error"]
    assert err["code"] == "email_unverified"
    assert err["challenge_id"].startswith("otc_")
    # fresh code mailed, never a token
    hits = await_outbox("pending@example.com")
    assert hits and re.search(r"\b\d{6}\b", hits[-1].text)
    assert "token" not in resp.json()


def test_banned_user_gets_neutral_401(gating_env, monkeypatch):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    c = _client()
    user_store.create_signup_user(
        email="banned@example.com", name="B", password_hash="h",
        password_algo="argon2id", canonical="banned@example.com")
    user_store.update_user(email="banned@example.com", status="banned")
    resp = c.post("/api/v1/auth/login", json={
        "email": "banned@example.com", "password": "any-password"})
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid email or password."


def test_flag_off_pending_still_neutral_401(gating_env, monkeypatch):
    monkeypatch.delenv("DATAWRAP_SIGNUP_ENABLED", raising=False)
    c = _client(flag_on=False)
    _pending()
    resp = c.post("/api/v1/auth/login", json={
        "email": "pending@example.com", "password": "xx-password"})
    assert resp.status_code == 401
    assert "challenge_id" not in resp.text


def test_unknown_email_still_neutral_401(gating_env, monkeypatch):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    c = _client()
    resp = c.post("/api/v1/auth/login", json={
        "email": "nobody@example.com", "password": "xx-password"})
    assert resp.status_code == 401


def test_failed_rekey_heal_on_login_is_audited(gating_env, monkeypatch):
    """A failed rekey heal at sign-in must land its security event. The audit
    call named ``record_security_event`` before the function's later local
    import bound it, so it raised UnboundLocalError and the event was lost."""
    import services.signup_service as signup_service
    from services import audit_coverage

    def boom(*, email):
        raise RuntimeError("membership move failed")

    events: list[tuple[str, dict]] = []
    real = audit_coverage.record_security_event

    def capture(action, *args, **kwargs):
        events.append((action, kwargs))
        return real(action, *args, **kwargs)

    monkeypatch.setattr(signup_service, "finish_pending_rekeys", boom)
    monkeypatch.setattr(audit_coverage, "record_security_event", capture)
    resp = _client(flag_on=False).post(
        "/api/v1/auth/login",
        json={"email": "admin@example.com", "password": "strong-password-123"})
    assert resp.status_code == 200, resp.text
    assert resp.json()["token"]
    heal = [kw for action, kw in events if action == "email_change.rekey_flag_failed"]
    assert heal and heal[0]["reason_code"] == "heal_retry_failed"
    assert heal[0]["actor"] == "admin@example.com"
