"""Login rate limit + lockout (audit §6.2)."""

from __future__ import annotations

from services.auth_rate_limit import (
    check_login_rate_limit,
    record_login_failure,
    reset_auth_rate_limits,
)


def test_login_rate_limit_denies_burst(monkeypatch):
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "3")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "0.01")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "100")
    reset_auth_rate_limits()

    assert check_login_rate_limit(ip="1.2.3.4", email="a@b.com")["allowed"] is True
    assert check_login_rate_limit(ip="1.2.3.4", email="a@b.com")["allowed"] is True
    assert check_login_rate_limit(ip="1.2.3.4", email="a@b.com")["allowed"] is True
    denied = check_login_rate_limit(ip="1.2.3.4", email="a@b.com")
    assert denied["allowed"] is False
    assert float(denied["retry_after_sec"]) > 0


def test_login_lockout_after_failures(monkeypatch):
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "50")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "10")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "3")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_SEC", "120")
    reset_auth_rate_limits()

    for _ in range(3):
        assert check_login_rate_limit(ip="9.9.9.9", email="x@y.com")["allowed"] is True
        record_login_failure(ip="9.9.9.9", email="x@y.com")

    locked = check_login_rate_limit(ip="9.9.9.9", email="x@y.com")
    assert locked["allowed"] is False
    assert locked.get("locked") is True
