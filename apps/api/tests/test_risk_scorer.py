"""P2-8 — risk scorer: allow/challenge/hold/deny + optional GeoIP adapter."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
import services.geoip as geoip
import services.risk_scorer as risk_scorer
import services.signup_service as signup_service
from services import signup_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender
from tests.helpers.mail_wait import await_outbox


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.delenv("DATAWRAP_MAXMIND_LICENSE_KEY", raising=False)
    monkeypatch.delenv("DATAWRAP_GEOIP_ENABLED", raising=False)
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(signup_service.mx_check, "check_domain", lambda d: {"ok": True})
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    geoip._reset_for_tests()
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


def _post(c: TestClient, email: str, *, ua: str = "Mozilla/5.0", dev: str = "dev-1"):
    return c.post(
        "/api/v1/auth/signup",
        json={"email": email, "password": "a-very-long-password",
              "form_token": form_token_mod.issue("signup"),
              "device_id": dev},
        headers={"user-agent": ua, "x-forwarded-for": "203.0.113.7, 10.0.0.1"},
    )


# ---------- unit: score_signals ----------

def test_clean_signals_allow():
    r = risk_scorer.score_signals(
        ip="203.0.113.7", ua="Mozilla/5.0", device_id="d",
        turnstile_ok=True, geoip_result={"ok": False})
    assert r["action"] == "allow" and r["score"] < 30


def test_headless_ua_raises_score():
    r = risk_scorer.score_signals(
        ip="203.0.113.7", ua="HeadlessChrome/120", device_id="",
        turnstile_ok=None, geoip_result={"ok": False})
    assert "automation_ua" in r["reasons"]
    assert r["action"] == "challenge"  # 15 + 35 + 5 = 55


def test_tor_exit_holds():
    r = risk_scorer.score_signals(
        ip="203.0.113.7", ua="Mozilla/5.0", device_id="d",
        turnstile_ok=None,
        geoip_result={"ok": True, "is_tor": True})
    assert r["action"] == "hold"  # 15 + 55 = 70


def test_tor_plus_proxy_denies():
    r = risk_scorer.score_signals(
        ip="203.0.113.7", ua="Mozilla/5.0", device_id="d",
        turnstile_ok=None,
        geoip_result={"ok": True, "is_tor": True, "is_anonymous": True})
    assert r["action"] == "deny"  # 15 + 55 + 40 = 110


# ---------- unit: geoip adapter ----------

def test_geoip_disabled_by_default():
    assert geoip.lookup("203.0.113.7") == {"ok": False}


def test_geoip_parses_insights_payload(monkeypatch):
    monkeypatch.setenv("DATAWRAP_MAXMIND_LICENSE_KEY", "k")
    monkeypatch.setenv("DATAWRAP_GEOIP_ENABLED", "1")

    class _Resp:
        status_code = 200

        @staticmethod
        def json():
            return {"country": {"iso_code": "US"},
                    "traits": {"is_tor_exit_node": True,
                               "autonomous_system_number": 13335}}

    geoip._reset_for_tests()
    result = geoip.lookup("203.0.113.7", http_get=lambda *a, **k: _Resp())
    assert result["ok"] and result["is_tor"] and result["country"] == "US"


def test_geoip_failure_fails_open(monkeypatch):
    monkeypatch.setenv("DATAWRAP_MAXMIND_LICENSE_KEY", "k")
    monkeypatch.setenv("DATAWRAP_GEOIP_ENABLED", "1")

    def boom(*a, **k):
        raise ConnectionError("no network")

    geoip._reset_for_tests()
    assert geoip.lookup("203.0.113.7", http_get=boom) == {"ok": False}


# ---------- flow: actions end to end ----------

def test_deny_fake_202_nothing_stored(monkeypatch):
    monkeypatch.setattr(
        risk_scorer.geoip, "lookup",
        lambda ip: {"ok": True, "is_tor": True, "is_anonymous": True})
    c = _client()
    resp = _post(c, "bot@example.com")
    assert resp.status_code == 202
    assert ConsoleEmailSender.OUTBOX == []
    assert signup_store.get_pending_signup(email="bot@example.com") is None


def test_hold_stores_pending_but_no_mail(monkeypatch):
    monkeypatch.setattr(
        risk_scorer.geoip, "lookup",
        lambda ip: {"ok": True, "is_tor": True})
    c = _client()
    resp = _post(c, "held@example.com")
    assert resp.status_code == 202
    assert ConsoleEmailSender.OUTBOX == []
    pending = signup_store.get_pending_signup(email="held@example.com")
    assert pending is not None
    assert pending["risk_score"] >= 60


def test_normal_signup_unaffected_without_license_key():
    c = _client()
    resp = _post(c, "clean@example.com")
    assert resp.status_code == 202
    pending = signup_store.get_pending_signup(email="clean@example.com")
    assert pending is not None
    assert await_outbox("clean@example.com")
