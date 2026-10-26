"""P1-3 — POST /auth/signup: identical 202 for new/pending/existing emails.

Enumeration safety: same status, same body shape; existing owners get the
notice mail instead of a code; nothing about which path ran is visible.
"""

from __future__ import annotations

import pytest
import services.form_token as ft
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.signup_service as signup_service
from services import signup_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender
from tests.helpers.mail_wait import await_outbox


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setattr(signup_service.mx_check, "check_domain", lambda d: {"ok": True})
    import services.form_token as form_token_mod
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "_BACKFILLED", False)
    import services.org_store as org_store
    monkeypatch.setattr(org_store, "_orgs_path", lambda: tmp_path / "orgs.json")
    monkeypatch.setattr(org_store, "_plans_path", lambda: tmp_path / "plans.json")
    import services.team_store as team_store
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
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


def _body(**kw):
    return {
        "email": kw.get("email", "ada@example.com"),
        "password": kw.get("password", "a-very-long-password"),
        "name": kw.get("name", "Ada"),
        "turnstile_token": "",
        "form_token": ft.issue("signup"),
        "device_id": "d1",
        "honeypot": "",
    }


def test_new_signup_returns_202_with_challenge():
    resp = _client().post("/api/v1/auth/signup", json=_body())
    assert resp.status_code == 202
    body = resp.json()
    assert body["challenge_id"].startswith("otc_")
    assert body["resend_after_sec"] == 60
    assert body["expires_in_sec"] == 600
    # code mail captured, pending row stored
    hits = await_outbox("ada@example.com")
    subjects = [m.subject.lower() for m in hits]
    assert any("verification" in s for s in subjects), subjects
    assert not any("already have" in s for s in subjects), subjects
    assert signup_store.get_pending_signup(email="ada@example.com") is not None


def test_repeat_signup_same_202_and_replaces_pending():
    c = _client()
    r1 = c.post("/api/v1/auth/signup", json=_body())
    r2 = c.post("/api/v1/auth/signup", json=_body(name="Ada2"))
    assert r1.status_code == r2.status_code == 202
    assert set(r1.json()) == set(r2.json())
    assert r1.json()["challenge_id"] != r2.json()["challenge_id"]  # fresh challenge
    row = signup_store.get_pending_signup(email="ada@example.com")
    assert row["name"] == "Ada2"


def test_existing_user_gets_same_202_and_notice_mail():
    user_store.create_user(email="ada@example.com", password="x" * 12)
    resp = _client().post("/api/v1/auth/signup", json=_body())
    assert resp.status_code == 202
    body = resp.json()
    assert set(body) == {"challenge_id", "resend_after_sec", "expires_in_sec"}
    # owner gets the notice, no code mail, no pending row created
    hits = await_outbox("ada@example.com")
    subjects = [m.subject.lower() for m in hits]
    assert any("already have a datawrap account" in s for s in subjects), subjects
    assert not any("verification" in s for s in subjects), subjects
    assert signup_store.get_pending_signup(email="ada@example.com") is None


def test_canonical_alias_signup_hits_existing_account():
    user_store.create_signup_user(
        email="john@gmail.com", name="J", password_hash="h",
        password_algo="argon2id", canonical="john@gmail.com")
    resp = _client().post("/api/v1/auth/signup", json=_body(email="J.O.H.N+x@gmail.com"))
    assert resp.status_code == 202
    # The notice goes to the stored canonical address, not the typed variant.
    hits = await_outbox("john@gmail.com")
    assert hits and "already have" in hits[0].subject.lower()


def test_short_password_400():
    resp = _client().post("/api/v1/auth/signup", json=_body(password="short"))
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_password"


def test_quota_exceeded_is_clean_error(monkeypatch):
    import services.email_quota as eq
    import services.signup_service as ss
    from services.email_sender import EmailQuotaExceeded

    def boom():
        raise EmailQuotaExceeded("cap", retry_after_sec=120)

    monkeypatch.setattr(eq, "consume_send_slot", boom)
    monkeypatch.setattr(ss, "consume_send_slot", boom)
    resp = _client().post("/api/v1/auth/signup", json=_body())
    assert resp.status_code == 503
    err = resp.json()["error"]
    assert err["code"] == "email_quota_exceeded"
    assert err["retry_after_sec"] == 120
    assert resp.headers["Retry-After"] == "120"


def test_timing_parity_new_vs_existing():
    """Constant-work check: hashing runs on both paths (no fast-reject)."""
    import time

    c = _client()
    user_store.create_user(email="exists@example.com", password="x" * 12)
    t0 = time.monotonic()
    c.post("/api/v1/auth/signup", json=_body(email="new@example.com"))
    t_new = time.monotonic() - t0
    t0 = time.monotonic()
    c.post("/api/v1/auth/signup", json=_body(email="exists@example.com"))
    t_existing = time.monotonic() - t0
    # Both perform one argon2id hash; bound is loose to avoid flakes.
    assert t_existing < t_new * 3 + 0.5
    assert t_new < t_existing * 3 + 0.5
