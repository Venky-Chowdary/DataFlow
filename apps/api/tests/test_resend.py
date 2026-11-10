"""P1-5 — POST /auth/resend: 60s cooldown + 5/hr + 10/day per subject.

Resend at 59s is refused with retry_after_sec; the caps hold; a fresh code
voids the old one.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import services.form_token as form_token_mod
import pytest
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


def _signup(client: TestClient, email="ada@example.com") -> str:
    resp = client.post("/api/v1/auth/signup", json={
        "email": email, "password": "a-very-long-password", "name": "A",
        "form_token": form_token_mod.issue("signup")})
    assert resp.status_code == 202
    return resp.json()["challenge_id"]


def _age_last_sent(challenge_id: str, seconds: float):
    """Back-date last_sent_at so the cooldown has elapsed."""
    past = (datetime.now(UTC) - timedelta(seconds=seconds)).isoformat()
    signup_store._update_challenge(challenge_id, last_sent_at=past)


def test_resend_within_60s_refused_with_retry_after():
    c = _client()
    cid = _signup(c)
    resp = c.post("/api/v1/auth/resend", json={"challenge_id": cid})
    assert resp.status_code == 429
    err = resp.json()["error"]
    assert err["code"] == "resend_too_soon"
    assert 1 <= err["retry_after_sec"] <= 60
    assert resp.headers["Retry-After"]


def test_resend_after_cooldown_sends_new_code():
    c = _client()
    cid = _signup(c)
    mails_before = len(await_outbox("ada@example.com"))
    _age_last_sent(cid, 61)
    resp = c.post("/api/v1/auth/resend", json={"challenge_id": cid})
    assert resp.status_code == 202
    hits = await_outbox("ada@example.com", min_count=mails_before + 1)
    assert len(hits) == mails_before + 1
    row = signup_store.get_challenge(cid)
    assert row["sent_count"] == 2


def test_resend_repeats_the_live_code_so_the_first_email_still_works():
    """QA 2026-11-10: the first code email landed late (spam), the user pressed
    Resend, and two emails carried two codes, the first one dead. A resend
    while the code is still valid now repeats it: either email works."""
    import re

    c = _client()
    cid = _signup(c)
    first_mail = await_outbox("ada@example.com")[-1]
    _age_last_sent(cid, 61)
    resp = c.post("/api/v1/auth/resend", json={"challenge_id": cid})
    assert resp.status_code == 202
    assert resp.json()["same_code"] is True
    second_mail = await_outbox("ada@example.com", min_count=2)[-1]
    first_code = re.search(r"\b(\d{6})\b", first_mail.text).group(1)
    second_code = re.search(r"\b(\d{6})\b", second_mail.text).group(1)
    assert first_code == second_code
    r = c.post("/api/v1/auth/verify-email",
               json={"challenge_id": cid, "code": first_code})
    assert r.status_code == 200, r.text


def test_burned_code_gets_a_new_code_and_the_old_one_stays_dead():
    """Five wrong tries burn the code: a resend then issues a NEW code (attempts
    reset), never the burned one, and the old code does not verify."""
    import re

    c = _client()
    cid = _signup(c)
    old_mail = await_outbox("ada@example.com")[-1]
    old_code = re.search(r"\b(\d{6})\b", old_mail.text).group(1)
    signup_store._update_challenge(cid, attempts=5)
    _age_last_sent(cid, 61)
    resp = c.post("/api/v1/auth/resend", json={"challenge_id": cid})
    assert resp.status_code == 202
    assert resp.json()["same_code"] is False
    new_mail = await_outbox("ada@example.com", min_count=2)[-1]
    new_code = re.search(r"\b(\d{6})\b", new_mail.text).group(1)
    assert signup_store.get_challenge(cid)["attempts"] == 0
    if new_code != old_code:  # 1-in-a-million collision aside
        r = c.post("/api/v1/auth/verify-email",
                   json={"challenge_id": cid, "code": old_code})
        assert r.status_code in (400, 410)
    r = c.post("/api/v1/auth/verify-email",
               json={"challenge_id": cid, "code": new_code})
    assert r.status_code == 200, r.text


def test_expired_code_cannot_be_resent():
    c = _client()
    cid = _signup(c)
    past = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    signup_store._update_challenge(cid, expires_at=past)
    _age_last_sent(cid, 61)
    resp = c.post("/api/v1/auth/resend", json={"challenge_id": cid})
    assert resp.status_code == 410
    assert resp.json()["error"]["code"] == "code_expired"


def test_same_code_resend_keeps_the_wrong_attempt_count():
    """Repeating a code must not buy more guesses at it."""
    c = _client()
    cid = _signup(c)
    signup_store._update_challenge(cid, attempts=3)
    _age_last_sent(cid, 61)
    resp = c.post("/api/v1/auth/resend", json={"challenge_id": cid})
    assert resp.status_code == 202 and resp.json()["same_code"] is True
    assert signup_store.get_challenge(cid)["attempts"] == 3


def test_hourly_cap_holds():
    """The counter keys on the challenge's subject (pending id); 5 resends
    per hour are allowed, the 6th is refused."""
    c = _client()
    cid = _signup(c)
    for i in range(5):
        _age_last_sent(cid, 61)
        r = c.post("/api/v1/auth/resend", json={"challenge_id": cid})
        assert r.status_code == 202, i
    _age_last_sent(cid, 61)
    r = c.post("/api/v1/auth/resend", json={"challenge_id": cid})
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "resend_limit"


def test_daily_cap_independent_of_hourly(monkeypatch):
    """Raise the hourly cap so the 10/day cap is what bites."""
    import services.signup_service as ss

    monkeypatch.setattr(ss, "RESEND_HOURLY_CAP", 100)
    c = _client()
    cid = _signup(c)
    for i in range(10):
        _age_last_sent(cid, 61)
        r = c.post("/api/v1/auth/resend", json={"challenge_id": cid})
        assert r.status_code == 202, i
    _age_last_sent(cid, 61)
    r = c.post("/api/v1/auth/resend", json={"challenge_id": cid})
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "resend_limit"
