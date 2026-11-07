"""invalid_code carries attempts_remaining read from the post-increment row.

After 1 wrong code the response says 4 left; after max-1 it says 1; the
burning submission says 0; and concurrent wrong submissions each report a
distinct, monotonically decreasing count — never a stale second read.
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
import services.signup_service as signup_service
from services import otp_service, signup_store, team_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setattr(signup_service.mx_check, "check_domain", lambda d: {"ok": True})
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "_BACKFILLED", False)
    import services.org_store as org_store
    monkeypatch.setattr(org_store, "_orgs_path", lambda: tmp_path / "orgs.json")
    monkeypatch.setattr(org_store, "_plans_path", lambda: tmp_path / "plans.json")
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    from services import auth_state, email_quota
    auth_state.reset_auth_state()
    email_quota._reset_backends()
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()
    yield
    reset_email_sender()


def _client() -> TestClient:
    from services.api_errors import ApiError, api_error_handler
    from src.routers.signup_router import router

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _challenge_id(client: TestClient, email: str) -> str:
    resp = client.post("/api/v1/auth/signup", json={
        "email": email, "password": "a-very-long-password", "name": "Ada",
        "device_id": "d1",
        "form_token": form_token_mod.issue("signup"),
    })
    assert resp.status_code == 202
    return resp.json()["challenge_id"]


def _bad_code(client: TestClient, challenge_id: str):
    return client.post("/api/v1/auth/verify-email",
                       json={"challenge_id": challenge_id, "code": "000000"})


def test_attempts_remaining_after_each_wrong_code():
    c = _client()
    cid = _challenge_id(c, "count@example.com")

    resp = _bad_code(c, cid)
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_code"
    assert resp.json()["error"]["attempts_remaining"] == 4
    # Sentence case, not "that code doesn't match".
    assert resp.json()["error"]["message"].startswith("That code")

    for expected in (3, 2, 1, 0):
        resp = _bad_code(c, cid)
        assert resp.status_code == 400
        assert resp.json()["error"]["attempts_remaining"] == expected

    # Challenge is burned: the next submit reports expiry, not invalid.
    resp = _bad_code(c, cid)
    assert resp.status_code == 410
    assert resp.json()["error"]["code"] == "code_expired"


def test_concurrent_wrong_codes_report_post_increment_values():
    c = _client()
    cid = _challenge_id(c, "race@example.com")

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(lambda _i: _bad_code(c, cid), range(8)))

    leftovers = sorted(
        r.json()["error"]["attempts_remaining"]
        for r in responses
        if r.status_code == 400 and r.json()["error"]["code"] == "invalid_code"
    )
    # Exactly MAX_ATTEMPTS submissions land a wrong-code verdict — each with
    # a distinct post-increment count (a duplicate proves a stale second
    # read) ending at 0 when the challenge burns. The rest see code_expired.
    assert leftovers == [0, 1, 2, 3, 4]
    assert sum(1 for r in responses if r.status_code == 410) == 3
