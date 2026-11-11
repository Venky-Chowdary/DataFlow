"""D338: every sign-in is listed in one session registry, and a password change
signs every other session out.

Before the fix ``/auth/login`` registered its session in ``services.auth_sessions``
while ``GET``/``DELETE /auth/sessions`` read ``services.auth_state``, so the list
was always empty, "sign out everywhere else" revoked 0, and
``/auth/change-password`` revoked nothing: a second token kept working.

Through the real app on live MongoDB (``r8_ent_harness``).
"""

from __future__ import annotations

import time
import uuid

import pytest
from fastapi import Response

from r8_ent_harness import ent  # noqa: F401

PW_OLD = "Session-Registry-Pass-2026!"
PW_NEW = "Session-Registry-Pass-2027!"


@pytest.fixture(autouse=True)
def _signup_routes_on(monkeypatch):
    # GET/DELETE /auth/sessions live on the signup router (404 when it is off);
    # production runs with it on.
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")


def _user(ent) -> str:
    from services import user_store

    email = f"d338-{uuid.uuid4().hex[:8]}@example.test"
    user_store.create_user(email=email, password=PW_OLD, role="member")
    return email


def _me(client) -> int:
    return client.get("/api/v1/auth/me").status_code


def _jtis(client) -> list[dict]:
    res = client.get("/api/v1/auth/sessions")
    assert res.status_code == 200, res.text
    return res.json()["sessions"]


def test_two_logins_are_both_listed_with_device_details(ent):
    email = _user(ent)
    a = ent.login(email, PW_OLD)
    b = ent.login(email, PW_OLD)
    sessions = _jtis(a)
    assert len(sessions) == 2
    assert sum(1 for s in sessions if s["current"]) == 1
    for s in sessions:
        assert s["created_at"] and s["last_seen_at"] and s["absolute_expires_at"]
        assert "ip" in s and "ua" in s
    assert {s["jti"] for s in sessions} == {s["jti"] for s in _jtis(b)}


def test_change_password_signs_out_every_other_session_and_keeps_the_current(ent):
    email = _user(ent)
    a = ent.login(email, PW_OLD)
    b = ent.login(email, PW_OLD)
    c = ent.login(email, PW_OLD)
    assert _me(b) == 200 and _me(c) == 200
    res = a.post("/api/v1/auth/change-password",
                 json={"current_password": PW_OLD, "new_password": PW_NEW})
    assert res.status_code == 200, res.text
    assert res.json().get("other_sessions_revoked") == 2
    assert _me(b) == 401
    assert _me(c) == 401
    # The session that changed the password stays signed in (documented).
    assert _me(a) == 200
    assert [s["current"] for s in _jtis(a)] == [True]


def test_sign_out_everywhere_else_revokes_the_other_login(ent):
    email = _user(ent)
    a = ent.login(email, PW_OLD)
    b = ent.login(email, PW_OLD)
    res = a.delete("/api/v1/auth/sessions")
    assert res.status_code == 200, res.text
    assert res.json()["revoked"] >= 1
    assert _me(b) == 401
    assert _me(a) == 200


def test_revoke_one_listed_session_by_id(ent):
    email = _user(ent)
    a = ent.login(email, PW_OLD)
    b = ent.login(email, PW_OLD)
    other = next(s["jti"] for s in _jtis(a) if not s["current"])
    res = a.delete(f"/api/v1/auth/sessions/{other}")
    assert res.status_code == 200, res.text
    assert _me(b) == 401 and _me(a) == 200
    # Another user's session id is not yours to revoke.
    stranger = ent.login(_user(ent), PW_OLD)
    mine = _jtis(a)[0]["jti"]
    assert stranger.delete(f"/api/v1/auth/sessions/{mine}").status_code == 404
    assert _me(a) == 200


def test_logout_of_the_presented_token_still_works(ent):
    email = _user(ent)
    a = ent.login(email, PW_OLD)
    b = ent.login(email, PW_OLD)
    assert a.post("/api/v1/auth/logout").status_code == 200
    assert _me(a) == 401
    assert [s["current"] for s in _jtis(b)] == [True]


def test_password_reset_link_revokes_bearer_sessions_too(ent):
    from services import password_reset_store, signup_service

    email = _user(ent)
    a = ent.login(email, PW_OLD)
    token, _doc = password_reset_store.issue(email=email)
    signup_service.confirm_password_reset(token=token, new_password=PW_NEW)
    assert _me(a) == 401


def test_a_browser_sign_in_registers_one_listed_session_and_revoking_it_kills_the_cookie(ent):
    """OTP signup completion / invite accept: cookie + bearer are ONE listed session."""
    from services.auth_state import get_auth_state
    from src.services.browser_session import issue_browser_session

    email = _user(ent)
    cookie = get_auth_state().create_session(user_id=email, ip="203.0.113.7", ua="probe-ua",
                                             csrf_token="c" * 16)
    payload = issue_browser_session(Response(), cookie, email)
    browser = ent.bearer(payload["token"])
    listed = _jtis(browser)
    assert len(listed) == 1 and listed[0]["current"]
    assert listed[0]["ip"] == "203.0.113.7" and listed[0]["ua"] == "probe-ua"
    # The bearer never carries the cookie's session id.
    assert cookie["jti"] not in payload["token"]
    other = ent.login(email, PW_OLD)
    assert other.delete("/api/v1/auth/sessions").json()["revoked"] == 1
    assert _me(browser) == 401
    assert not get_auth_state().session_active(cookie["jti"])


def test_revoke_all_except_keeps_only_the_named_session(tmp_path, monkeypatch):
    from services import auth_sessions

    monkeypatch.setattr(auth_sessions, "_path", lambda: tmp_path / "sessions.json")
    monkeypatch.setattr(auth_sessions, "_collection", lambda: None)
    exp = int(time.time()) + 3600
    keep = auth_sessions.create_session("k@example.com", expires_at=exp)
    gone = auth_sessions.create_session("k@example.com", expires_at=exp)
    assert auth_sessions.revoke_all_for_email("k@example.com", keep_jti=keep) == 1
    assert auth_sessions.session_active(keep, "k@example.com")
    assert not auth_sessions.session_active(gone, "k@example.com")
    assert [r["jti"] for r in auth_sessions.list_active("k@example.com")] == [keep]
