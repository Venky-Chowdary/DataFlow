"""SEC-7 / REL-1: login lockout per account+address, bounded, shared, with TTLs.

Nine anonymous failures from nine addresses locked the administrator out
everywhere (429 for the right password from a new address); eight failures
from one office address locked every account behind it; the lockout table
grew without bound, per process, and was lost on restart.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from tests.tenancy_world import ADMIN_EMAIL, ADMIN_PASSWORD, EDITOR_A, EDITOR_B, tenancy_world  # noqa: F401

_PASSWORDS = {EDITOR_A: "Editor-A-Password-2026", EDITOR_B: "Editor-B-Password-2026"}


def _login(app, email, password, ip):
    return TestClient(app).post("/api/v1/auth/login", json={"email": email, "password": password},
                                headers={"X-Forwarded-For": ip})


@pytest.fixture
def app(tenancy_world, monkeypatch):  # noqa: F811
    from services import auth_rate_limit, client_ip

    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "100")
    # Every test address is a client address (no proxy hop to strip).
    monkeypatch.setattr(client_ip, "client_ip_from_request",
                        lambda request: request.headers.get("X-Forwarded-For") or "testclient")
    auth_rate_limit.reset_auth_rate_limits()
    yield tenancy_world.app
    auth_rate_limit.reset_auth_rate_limits()


def test_anonymous_failures_from_many_addresses_never_lock_the_account(app):
    # The administrator signs in from their usual address first.
    assert _login(app, ADMIN_EMAIL, ADMIN_PASSWORD, "192.0.2.10").status_code == 200
    for i in range(40):
        _login(app, ADMIN_EMAIL, "Wrong-password-nope-xx", f"198.51.100.{i}")
    usual = _login(app, ADMIN_EMAIL, ADMIN_PASSWORD, "192.0.2.10")
    assert usual.status_code == 200, usual.text
    # From a brand-new address the account is only slowed down: a short wait, never a lockout.
    fresh = _login(app, ADMIN_EMAIL, ADMIN_PASSWORD, "192.0.2.77")
    if fresh.status_code == 429:
        assert int(fresh.headers["Retry-After"]) <= 60, fresh.headers


def test_nine_anonymous_failures_do_not_stop_the_right_password_from_a_new_address(app):
    for i in range(9):
        _login(app, ADMIN_EMAIL, "Wrong-password-nope-xx", f"198.51.100.{i}")
    r = _login(app, ADMIN_EMAIL, ADMIN_PASSWORD, "192.0.2.77")
    assert r.status_code == 200, r.text


def test_one_users_typos_do_not_lock_out_their_office(app):
    for i in range(8):
        _login(app, EDITOR_A, f"Wrong-password-{i}-xx", "203.0.113.9")
    assert _login(app, EDITOR_A, "Wrong-password-x-xx", "203.0.113.9").status_code == 429
    assert _login(app, ADMIN_EMAIL, ADMIN_PASSWORD, "203.0.113.9").status_code == 200


def test_the_pair_lockout_backs_off_and_the_account_slowdown_lets_mfa_through(app, monkeypatch):
    from services import mfa

    for i in range(8):
        _login(app, ADMIN_EMAIL, "Wrong-password-nope-xx", "203.0.113.50")
    locked = _login(app, ADMIN_EMAIL, ADMIN_PASSWORD, "203.0.113.50")
    assert locked.status_code == 429 and int(locked.headers["Retry-After"]) >= 300
    for i in range(30):
        _login(app, ADMIN_EMAIL, "Wrong-password-nope-xx", f"198.51.100.{i}")
    monkeypatch.setattr(mfa, "is_enrolled", lambda email: True)
    monkeypatch.setattr(mfa, "mint_challenge", lambda email: ("w1sec-challenge", 0))
    # A wrong password from a new address is slowed (soft, short), ...
    wrong = _login(app, ADMIN_EMAIL, "Wrong-password-nope-xx", "192.0.2.98")
    assert wrong.status_code == 429 and int(wrong.headers["Retry-After"]) <= 60
    # ... the right one reaches the second factor.
    held = _login(app, ADMIN_EMAIL, ADMIN_PASSWORD, "192.0.2.99")
    assert held.status_code == 202 and held.json()["mfa_required"] is True, held.text


def test_the_lockout_table_is_bounded_and_expires(monkeypatch):
    from services import auth_rate_limit as arl

    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_BACKEND", "memory")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_MAX_KEYS", "500")
    arl.reset_auth_rate_limits()
    for i in range(3000):
        arl.record_login_failure(ip=f"10.{i // 250}.{i % 250}.1", email=f"spray{i}@x.example")
    assert len(arl._LOCKOUTS) <= 500
    clock = [arl.time.time()]
    monkeypatch.setattr(arl.time, "time", lambda: clock[0])
    arl.record_login_failure(ip="10.9.9.9", email="late@x.example")
    clock[0] += 24 * 3600
    assert arl._LOCKOUTS.get("pair:late@x.example|10.9.9.9", now=clock[0]) is None
    arl.reset_auth_rate_limits()


def test_with_redis_configured_lockouts_are_shared_and_survive_a_restart(monkeypatch):
    import os

    import redis

    from services import auth_rate_limit as arl

    url = os.environ.get("REDIS_URL") or "redis://127.0.0.1:6379/0"
    try:
        client = redis.from_url(url)
        client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no Redis: {exc}")
    prefix = f"w1sec:authlock:{uuid.uuid4().hex}:"
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_BACKEND", "redis")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_REDIS_URL", url)
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_PREFIX", prefix)
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "3")
    monkeypatch.setitem(arl._BACKEND, "store", None)
    try:
        for _ in range(3):
            arl.record_login_failure(ip="198.51.100.7", email="victim@x.example")
        keys = list(client.scan_iter(match=prefix + "*"))
        assert keys and all(client.ttl(k) > 0 for k in keys)
        # Another replica (or this one after a restart) sees the same counters.
        arl._LOCKOUTS.clear()
        monkeypatch.setitem(arl._BACKEND, "store", None)
        denied = arl.check_login_rate_limit(ip="198.51.100.7", email="victim@x.example")
        assert denied["allowed"] is False and denied.get("locked") is True, denied
        assert arl.check_login_rate_limit(ip="198.51.100.8", email="victim@x.example")["allowed"] is True
    finally:
        for key in client.scan_iter(match=prefix + "*"):
            client.delete(key)
        monkeypatch.setitem(arl._BACKEND, "store", None)
