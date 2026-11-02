"""P5 — admin signup metrics: pending + challenges + quota position."""

from __future__ import annotations

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
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "metrics-test-secret-value")
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


def _admin_cookie(c: TestClient, email: str = "admin@example.com") -> None:
    """Cookie-auth session for a stored admin user (what prod uses)."""
    user_store.create_user(email=email, name="Admin", password="x" * 14,
                           role="admin")
    sess = auth_state.get_auth_state().create_session(
        user_id=email, csrf_token="csrf-token")
    c.cookies.set("__Host-dw_session", sess["jti"])


def test_metrics_requires_admin():
    c = _client()
    assert c.get("/api/v1/auth/admin/signup-metrics").status_code == 401


def test_metrics_counts_pending_and_challenges():
    c = _client()
    _admin_cookie(c)
    r = c.post("/api/v1/auth/signup", json={
        "email": "ada@example.com", "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"), "device_id": "d1"})
    assert r.status_code == 202
    m = c.get("/api/v1/auth/admin/signup-metrics")
    assert m.status_code == 200
    body = m.json()
    assert body["pending_signups"] == 1
    assert body["active_challenges"] == 1
    assert body["email_quota"]["day_used"] >= 1
