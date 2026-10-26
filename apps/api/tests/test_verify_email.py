"""P1-4 — POST /auth/verify-email: user+org+workspace+membership in one unit.

Double-submit creates exactly one user; the session cookie is set; the
pending row is gone; the challenge is consumed.
"""

from __future__ import annotations

import services.form_token as form_token_mod
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.signup_service as signup_service
from services import otp_service, signup_store, team_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender
from tests.helpers.mail_wait import drain_mail_lane


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


def _signup_and_code(client: TestClient, email="ada@example.com") -> tuple[str, str]:
    resp = client.post("/api/v1/auth/signup", json={
        "email": email, "password": "a-very-long-password", "name": "Ada",
        "device_id": "d1",
        "form_token": form_token_mod.issue("signup"),
    })
    assert resp.status_code == 202
    drain_mail_lane()
    mail = ConsoleEmailSender.OUTBOX[-1]
    import re
    m = re.search(r"\b(\d{6})\b", mail.text)
    assert m, mail.text
    return resp.json()["challenge_id"], m.group(1)


def test_verify_creates_user_org_workspace_membership():
    c = _client()
    challenge_id, code = _signup_and_code(c)
    resp = c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": challenge_id, "code": code})
    assert resp.status_code == 200
    body = resp.json()
    assert body["next"] == "onboarding"
    assert body["user"]["email"] == "ada@example.com"
    assert body["org"]["type"] == "individual"
    assert body["org"]["plan_tier"] == "free"

    # user row exists, verified, argon2id
    doc = user_store._read_one("ada@example.com")
    assert doc["email_verified_at"]
    assert doc["password_algo"] == "argon2id"

    # workspace + admin membership + org link
    ws = body["workspace"]
    members = team_store._read_memberships(workspace_id=ws["id"])
    assert len(members) == 1 and members[0].role == "admin"
    assert members[0].email == "ada@example.com"
    stored_ws = [w for w in team_store._read_workspaces() if w.id == ws["id"]][0]
    assert stored_ws.org_id == body["org"]["id"]

    # session cookie
    cookie = resp.headers.get("set-cookie", "")
    assert "__Host-dw_session=" in cookie
    assert "HttpOnly" in cookie and "Secure" in cookie
    assert "samesite=lax" in cookie.lower() or "SameSite=Lax" in cookie

    # pending row consumed
    assert signup_store.get_pending_signup(email="ada@example.com") is None


def test_double_verify_creates_exactly_one_user():
    c = _client()
    challenge_id, code = _signup_and_code(c)
    r1 = c.post("/api/v1/auth/verify-email",
                json={"challenge_id": challenge_id, "code": code})
    r2 = c.post("/api/v1/auth/verify-email",
                json={"challenge_id": challenge_id, "code": code})
    assert r1.status_code == 200
    assert r2.status_code == 410  # challenge consumed
    assert [u["email"] for u in user_store.list_users()] == ["ada@example.com"]


def test_wrong_code_rejected():
    c = _client()
    challenge_id, code = _signup_and_code(c)
    wrong = "000000" if code != "000000" else "111111"
    resp = c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": challenge_id, "code": wrong})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_code"
    assert user_store.list_users() == []


def test_unknown_challenge_410():
    resp = _client().post("/api/v1/auth/verify-email",
                          json={"challenge_id": "otc_nope", "code": "123456"})
    assert resp.status_code == 410


def test_canonical_collision_at_verify_race():
    """An account created between signup and verify wins the canonical key —
    the raced verify refuses instead of duplicating it."""
    c = _client()
    challenge_id, code = _signup_and_code(c, email="ada@example.com")
    user_store.create_signup_user(
        email="ada@example.com", name="Ada", password_hash="h",
        password_algo="argon2id", canonical="ada@example.com")
    resp = c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": challenge_id, "code": code})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "account_exists"
    assert [u["email"] for u in user_store.list_users()] == ["ada@example.com"]
