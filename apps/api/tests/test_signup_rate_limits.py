"""P2-6 — signup velocity limits: per-IP, /24|/48 subnet, device, address.

Exhausted bucket -> 429 {"error":{"code":"signup_rate_limited",
"retry_after_sec":N}} + Retry-After; the pending row and the mail still
happen for posts under the caps.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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
    # Two proxies: the edge appends the client, the next hop appends the edge
    # (10.255.0.1), so the client is hops[-2] (ProxyFix semantics).
    monkeypatch.setenv("DATAWRAP_TRUSTED_PROXY_COUNT", "2")
    monkeypatch.setenv("DATAWRAP_SIGNUP_IP_HOURLY", "3")
    monkeypatch.setenv("DATAWRAP_SIGNUP_SUBNET_HOURLY", "5")
    monkeypatch.setenv("DATAWRAP_SIGNUP_DEVICE_DAILY", "50")
    monkeypatch.setenv("DATAWRAP_SIGNUP_EMAIL_DAILY", "2")
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


def _client() -> TestClient:
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _post(c: TestClient, n: int, *, ip: str = "1.2.3.4", dev: str | None = None):
    return c.post(
        "/api/v1/auth/signup",
        json={
            "email": f"user{n}@example.com",
            "password": "a-very-long-password",
            "form_token": form_token_mod.issue("signup"),
            "device_id": dev if dev is not None else f"dev-{n}-{id(c) % 9973}",
        },
        headers={"x-forwarded-for": f"{ip}, 10.255.0.1"},
    )


def test_per_ip_cap_returns_429():
    c = _client()
    for n in range(3):
        assert _post(c, n).status_code == 202
    r = _post(c, 99)
    assert r.status_code == 429
    body = r.json()["error"]
    assert body["code"] == "signup_rate_limited"
    assert body["retry_after_sec"] > 0
    assert r.headers["Retry-After"] == str(body["retry_after_sec"])
    assert signup_store.get_pending_signup(email="user99@example.com") is None


def test_other_ip_unaffected_by_exhausted_ip():
    c = _client()
    for n in range(3):
        assert _post(c, n).status_code == 202
    assert _post(c, 99).status_code == 429
    assert _post(c, 100, ip="9.8.7.6").status_code == 202


def test_shared_subnet_cap():
    c = _client()
    # 5-post /24 cap across three different addresses in the same /24
    for n in range(5):
        assert _post(c, n, ip=f"10.9.8.{n + 1}", dev=f"s{n}").status_code == 202
    r = _post(c, 99, ip="10.9.8.42", dev="s99")
    assert r.status_code == 429
    # a different /24 is unaffected
    assert _post(c, 100, ip="10.9.9.1", dev="s100").status_code == 202


def test_same_address_daily_cap():
    c = _client()
    assert _post(c, 1).status_code == 202
    # EMAIL_DAILY=2: a second post for the same address is allowed…
    # (pending row dedupes; this exercises the counter)
    _post(c, 2)  # different address stays fine
    r = c.post("/api/v1/auth/signup", json={
        "email": "user1@example.com",
        "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"),
        "device_id": "dev-x",
    }, headers={"x-forwarded-for": "1.2.3.4, 10.255.0.1"})
    assert r.status_code == 202
    r = c.post("/api/v1/auth/signup", json={
        "email": "user1@example.com",
        "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"),
        "device_id": "dev-y",
    }, headers={"x-forwarded-for": "5.6.7.8, 10.255.0.1"})
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "signup_rate_limited"


def test_subnet_key_normalization():
    assert signup_service._subnet_key("192.168.3.77") == "192.168.3.0"
    assert signup_service._subnet_key("2001:db8:abcd:1234::1").startswith("2001:db8:abcd:")
    assert signup_service._subnet_key("not-an-ip") == "unknown"
