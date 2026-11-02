"""P2-4 — role-address policy."""

from __future__ import annotations

import services.form_token as form_token_mod
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.signup_service as signup_service
from services import signup_store, user_store
from services.email_policy import is_blocked_role, role_assessment
from services.email_sender import ConsoleEmailSender, reset_email_sender


def test_blocked_prefixes():
    for e in ("noreply@x.co", "postmaster@x.co", "abuse@x.co",
              "mailer-daemon@x.co", "no-reply@x.co"):
        assert is_blocked_role(e), e


def test_blocked_ignores_plus_and_case():
    assert is_blocked_role("NoReply+tag@x.co")


def test_flagged_prefixes():
    for e in ("info@x.co", "admin@x.co", "test@x.co"):
        assert role_assessment(e) == "flagged", e


def test_normal_address_ok():
    assert role_assessment("ada.lovelace@x.co") is None


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setattr(signup_service.mx_check, "check_domain", lambda d: {"ok": True})
    import services.form_token as form_token_mod
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "_BACKFILLED", False)
    from services import auth_state, email_quota
    auth_state.reset_auth_state()
    email_quota._reset_backends()
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()
    yield
    reset_email_sender()


def test_blocked_role_refused_by_signup():
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    resp = TestClient(app).post("/api/v1/auth/signup", json={
        "email": "postmaster@example.com", "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup")})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "blocked_address"
    assert ConsoleEmailSender.OUTBOX == []
