"""P2-9 — optional external email verifier (Abstract provider)."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.email_verifier as email_verifier
import services.form_token as form_token_mod
import services.signup_service as signup_service
from services import signup_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.delenv("DATAWRAP_EMAIL_VERIFIER", raising=False)
    monkeypatch.delenv("DATAWRAP_EMAIL_VERIFIER_KEY", raising=False)
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


def test_off_by_default():
    assert not email_verifier.configured()
    assert email_verifier.verify("a@example.com") == {"ok": False, "verdict": "unknown"}


def _resp(payload: dict, status: int = 200):
    class _R:
        status_code = status

        @staticmethod
        def json():
            return payload

    return _R()


def test_verdict_mapping(monkeypatch):
    monkeypatch.setenv("DATAWRAP_EMAIL_VERIFIER", "abstract")
    monkeypatch.setenv("DATAWRAP_EMAIL_VERIFIER_KEY", "k")
    assert email_verifier.configured()
    for raw, expected in (("DELIVERABLE", "deliverable"),
                          ("RISKY", "risky"),
                          ("UNDELIVERABLE", "undeliverable")):
        r = email_verifier.verify(
            "a@x.com", http_get=lambda u, **k: _resp({"deliverability": raw}))
        assert r == {"ok": True, "verdict": expected}


def test_outage_fails_open(monkeypatch):
    monkeypatch.setenv("DATAWRAP_EMAIL_VERIFIER", "abstract")
    monkeypatch.setenv("DATAWRAP_EMAIL_VERIFIER_KEY", "k")

    def boom(*a, **k):
        raise TimeoutError("verifier down")

    assert email_verifier.verify("a@x.com", http_get=boom) == {
        "ok": False, "verdict": "unknown"}


def _client() -> TestClient:
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def test_undeliverable_refused_400(monkeypatch):
    monkeypatch.setenv("DATAWRAP_EMAIL_VERIFIER", "abstract")
    monkeypatch.setenv("DATAWRAP_EMAIL_VERIFIER_KEY", "k")
    monkeypatch.setattr(email_verifier, "verify",
                        lambda email: {"ok": True, "verdict": "undeliverable"})
    resp = _client().post("/api/v1/auth/signup", json={
        "email": "nope@example.com", "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup")})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "undeliverable_domain"
    assert signup_store.get_pending_signup(email="nope@example.com") is None


def test_risky_feeds_scorer_but_signup_proceeds(monkeypatch):
    monkeypatch.setenv("DATAWRAP_EMAIL_VERIFIER", "abstract")
    monkeypatch.setenv("DATAWRAP_EMAIL_VERIFIER_KEY", "k")
    monkeypatch.setattr(email_verifier, "verify",
                        lambda email: {"ok": True, "verdict": "risky"})
    resp = _client().post("/api/v1/auth/signup", json={
        "email": "risky@example.com", "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"),
        "device_id": "d1"})
    assert resp.status_code == 202
    pending = signup_store.get_pending_signup(email="risky@example.com")
    assert pending is not None
    # 15 (no turnstile) + 20 (risky) = 35 → challenge → score persisted
    assert pending["risk_score"] >= 30
    assert len(ConsoleEmailSender.OUTBOX) == 1


def test_verifier_outage_never_blocks_signup(monkeypatch):
    monkeypatch.setenv("DATAWRAP_EMAIL_VERIFIER", "abstract")
    monkeypatch.setenv("DATAWRAP_EMAIL_VERIFIER_KEY", "k")

    def boom(email):
        raise ConnectionError("verifier unreachable")

    monkeypatch.setattr(email_verifier, "verify", lambda email: {"ok": False, "verdict": "unknown"})
    resp = _client().post("/api/v1/auth/signup", json={
        "email": "ok@example.com", "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"), "device_id": "d1"})
    assert resp.status_code == 202
