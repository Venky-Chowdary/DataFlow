"""P4-4 — onboarding: org profile + onboarded stamp on the account."""

from __future__ import annotations

import re

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
import services.signup_service as signup_service
from services import auth_state, email_quota, org_store, signup_store, team_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender
from tests.helpers.mail_wait import drain_mail_lane


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "onboarding-test-secret-value")
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
    monkeypatch.setattr(org_store, "_orgs_path",
                        lambda: tmp_path / "orgs.json")
    monkeypatch.setattr(org_store, "_plans_path",
                        lambda: tmp_path / "plans.json")
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
    drain_mail_lane()
    code = re.search(r"\b(\d{6})\b", ConsoleEmailSender.OUTBOX[-1].text).group(1)
    resp = c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": r.json()["challenge_id"],
                        "code": code, "device_id": "d1"})
    assert resp.status_code == 200
    return resp.json()


def test_onboarding_requires_auth():
    c = _client()
    resp = c.post("/api/v1/auth/onboarding", json={"org_name": "X"})
    assert resp.status_code == 401


def test_onboarding_updates_org_and_stamps_user():
    c = _client()
    _verified_user(c)
    csrf = c.cookies["dw_csrf"]
    resp = c.post("/api/v1/auth/onboarding", json={
        "org_name": "Ada Corp", "use_case": "cdc",
        "team_size": "11-50", "heard_from": "blog"},
        headers={"x-csrf-token": csrf})
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    org = org_store.get_organization(resp.json()["org_id"])
    assert org["name"] == "Ada Corp"
    assert org["onboarding_profile"]["use_case"] == "cdc"
    assert org["onboarding_profile"]["team_size"] == "11-50"
    assert org["onboarding_profile"]["completed_at"]


def test_onboarding_blank_name_keeps_org_name():
    c = _client()
    _verified_user(c)
    csrf = c.cookies["dw_csrf"]
    resp = c.post("/api/v1/auth/onboarding", json={"use_case": "etl"},
                  headers={"x-csrf-token": csrf})
    assert resp.status_code == 200
    org = org_store.get_organization(resp.json()["org_id"])
    assert org["name"]  # untouched default name remains
    assert org["onboarding_profile"]["use_case"] == "etl"
