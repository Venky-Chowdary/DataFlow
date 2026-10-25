"""Forgot/reset via single-use emailed LINK: uniform 202, HMAC-only storage,
30-minute TTL, supersede-on-new, atomic consume, sessions revoked."""

from __future__ import annotations

import re
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
import services.signup_service as signup_service
from services import auth_state, email_quota, password_reset_store, signup_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setenv("DATAWRAP_APP_URL", "https://www.datawrap.io")
    monkeypatch.setattr(signup_store, "_pending_path",
                        lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path",
                        lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(password_reset_store, "_path",
                        lambda: tmp_path / "reset_tokens.json")
    monkeypatch.setattr(user_store, "_store_path",
                        lambda: tmp_path / "users.json")
    monkeypatch.setattr(signup_service.mx_check, "check_domain",
                        lambda d: {"ok": True})
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "reset-link-test-secret-value")
    monkeypatch.setenv("DATAFLOW_ALLOW_DEV_USER", "0")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "50")
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    monkeypatch.setattr(signup_service, "RESEND_COOLDOWN_SEC", 0)
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
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    from src.middleware.auth_middleware import AuthMiddleware

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.add_middleware(AuthMiddleware)
    app.include_router(router, prefix="/api/v1")
    return TestClient(app, base_url="https://testserver")


def _verified_user(c: TestClient, email: str = "ada@example.com") -> dict:
    r = c.post("/api/v1/auth/signup", json={
        "email": email, "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"), "device_id": "d1"})
    code = re.search(r"\b(\d{6})\b", ConsoleEmailSender.OUTBOX[-1].text).group(1)
    resp = c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": r.json()["challenge_id"],
                        "code": code, "device_id": "d1"})
    assert resp.status_code == 200
    return resp.json()


def _forgot(c: TestClient, email: str = "ada@example.com"):
    return c.post("/api/v1/auth/password/forgot",
                  json={"email": email,
                        "form_token": form_token_mod.issue("reset")})


def _link_token() -> str:
    m = re.search(r"reset-password\?token=([A-Za-z0-9_\-]+)",
                  ConsoleEmailSender.OUTBOX[-1].text)
    assert m, ConsoleEmailSender.OUTBOX[-1].text
    return m.group(1)


def test_forgot_returns_202_for_unknown_and_known_alike():
    c = _client()
    _verified_user(c)
    r_unknown = _forgot(c, "ghost@example.com")
    r_known = _forgot(c, "ada@example.com")
    assert r_unknown.status_code == r_known.status_code == 202
    for resp in (r_unknown, r_known):
        body = resp.json()
        assert set(body) == {"ok", "challenge_id", "resend_after_sec",
                             "expires_in_sec"}
        assert body["ok"] is True


def test_forgot_unknown_sends_no_mail():
    c = _client()
    _verified_user(c)
    before = len(ConsoleEmailSender.OUTBOX)
    _forgot(c, "nobody@example.com")
    assert len(ConsoleEmailSender.OUTBOX) == before


def test_link_goes_to_account_address_only_and_has_no_crlf():
    c = _client()
    _verified_user(c)
    _forgot(c, "ada@example.com")
    mail = ConsoleEmailSender.OUTBOX[-1]
    assert mail.to == ["ada@example.com"] or mail.to == "ada@example.com"
    assert "www.datawrap.io/reset-password?token=" in mail.text
    # injection-shaped input is refused before any mail work
    before = len(ConsoleEmailSender.OUTBOX)
    _forgot(c, "ada@example.com\r\nBCC: evil@example.com")
    assert len(ConsoleEmailSender.OUTBOX) == before


def test_token_is_256bit_and_only_hmac_is_stored():
    c = _client()
    _verified_user(c)
    _forgot(c)
    token = _link_token()
    assert len(token) >= 40  # token_urlsafe(32) -> 43 chars
    doc = password_reset_store._load()["tokens"]
    row = next(iter(doc.values()))
    assert row["token_hash"] != token
    assert len(row["token_hash"]) == 64  # sha256 hex
    assert row["expires_at"] and row["used_at"] is None
    assert row["request_ip"] is not None


def test_inspect_then_reset_revokes_sessions_and_notifies():
    c = _client()
    _verified_user(c)
    state = auth_state.get_auth_state()
    assert state.list_sessions("ada@example.com")
    _forgot(c)
    token = _link_token()
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": token})
    assert probe.status_code == 200 and probe.json()["ok"] is True
    resp = c.post("/api/v1/auth/password/reset", json={
        "token": token, "new_password": "an-even-better-password"})
    assert resp.status_code == 200
    creds = [u for u in user_store.credentials_for_auth()
             if u["email"] == "ada@example.com"]
    assert creds[0]["password_algo"] == "argon2id"
    assert state.list_sessions("ada@example.com") == []
    notice = ConsoleEmailSender.OUTBOX[-1]
    assert "password was changed" in notice.subject


def test_reused_link_is_rejected():
    c = _client()
    _verified_user(c)
    _forgot(c)
    token = _link_token()
    ok = c.post("/api/v1/auth/password/reset", json={
        "token": token, "new_password": "an-even-better-password"})
    assert ok.status_code == 200
    again = c.post("/api/v1/auth/password/reset", json={
        "token": token, "new_password": "third-password-here"})
    assert again.status_code == 410
    assert again.json()["error"]["code"] == "link_expired"
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": token})
    assert probe.json()["status"] == "used"


def test_expired_link_is_rejected(monkeypatch):
    monkeypatch.setattr(password_reset_store, "TOKEN_TTL",
                        timedelta(seconds=-1))
    c = _client()
    _verified_user(c)
    _forgot(c)
    token = _link_token()
    resp = c.post("/api/v1/auth/password/reset", json={
        "token": token, "new_password": "an-even-better-password"})
    assert resp.status_code == 410
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": token})
    assert probe.json()["status"] == "expired"


def test_new_request_invalidates_older_link():
    c = _client()
    _verified_user(c)
    _forgot(c)
    old = _link_token()
    _forgot(c)
    new = _link_token()
    assert new != old
    resp = c.post("/api/v1/auth/password/reset", json={
        "token": old, "new_password": "an-even-better-password"})
    assert resp.status_code == 410
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": new})
    assert probe.json()["ok"] is True


def test_concurrent_double_reset_exactly_one_wins():
    c = _client()
    _verified_user(c)
    _forgot(c)
    token = _link_token()
    body = {"token": token, "new_password": "an-even-better-password"}
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(
            lambda _: c.post("/api/v1/auth/password/reset", json=body),
            range(2)))
    codes = sorted(r.status_code for r in results)
    assert codes == [200, 410]


def test_garbage_token_is_410_not_500():
    c = _client()
    resp = c.post("/api/v1/auth/password/reset", json={
        "token": "totally-bogus-token", "new_password": "a-long-enough-password"})
    assert resp.status_code == 410
    assert resp.json()["error"]["code"] == "link_expired"


def test_reset_rejects_short_password_before_consume():
    c = _client()
    _verified_user(c)
    _forgot(c)
    token = _link_token()
    resp = c.post("/api/v1/auth/password/reset", json={
        "token": token, "new_password": "short"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_password"
    # token still usable — the password check ran before consume
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": token})
    assert probe.json()["ok"] is True


def test_raw_token_never_logged(caplog):
    c = _client()
    _verified_user(c)
    import logging
    with caplog.at_level(logging.DEBUG):
        _forgot(c)
        token = _link_token()
        c.post("/api/v1/auth/password/reset", json={
            "token": token, "new_password": "an-even-better-password"})
    for record in caplog.records:
        assert token not in record.getMessage()


def test_email_change_velocity_429s_with_retry_after(monkeypatch):
    """forgot stays uniform-202 by design; the 429 + Retry-After contract
    lives on the authenticated email-change route sharing the helper."""
    monkeypatch.setenv("DATAWRAP_EMCHG_EMAIL_DAILY", "1")
    c = _client()
    _verified_user(c)
    csrf = c.cookies["dw_csrf"]
    last = None
    for _ in range(3):
        last = c.post("/api/v1/auth/email/change",
                      headers={"x-csrf-token": csrf},
                      json={"new_email": "grace@example.com"})
    assert last.status_code == 429
    assert last.json()["error"]["retry_after_sec"] > 0


def test_variant_request_mails_only_the_stored_address():
    """A canonical-equivalent variant (case/dots/+tag) must not route the
    reset link to the typed mailbox — the link goes to the stored,
    verified account address only."""
    c = _client()
    _verified_user(c, "ada.signup@gmail.com")
    _forgot(c, "ADASIGNUP@gmail.com")
    mail = ConsoleEmailSender.OUTBOX[-1]
    assert mail.to == ["ada.signup@gmail.com"] or mail.to == "ada.signup@gmail.com"
    _forgot(c, "ada.signup+alternate@gmail.com")
    mail = ConsoleEmailSender.OUTBOX[-1]
    assert mail.to == ["ada.signup@gmail.com"] or mail.to == "ada.signup@gmail.com"


def test_variant_requests_supersede_one_token_set():
    """Tokens are keyed to the account, not the typed string: a second
    request through a different variant kills the first link."""
    c = _client()
    _verified_user(c, "ada.signup@gmail.com")
    _forgot(c, "ada.signup@gmail.com")
    first = _link_token()
    _forgot(c, "adasignup@gmail.com")
    second = _link_token()
    assert first != second
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": first})
    assert probe.json()["status"] == "used"
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": second})
    assert probe.json()["ok"] is True
