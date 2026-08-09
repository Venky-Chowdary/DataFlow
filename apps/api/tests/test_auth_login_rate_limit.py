"""Login rate limit + lockout (audit ITEM 4 / §6.2)."""

from __future__ import annotations

from services.auth_rate_limit import (
    check_login_rate_limit,
    record_login_failure,
    record_login_success,
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


def test_per_ip_throttle_independent_of_email(monkeypatch):
    """One IP spraying many accounts must still exhaust the IP bucket."""
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "2")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "0.01")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "100")
    reset_auth_rate_limits()

    assert check_login_rate_limit(ip="10.0.0.1", email="a@ex.com")["allowed"] is True
    assert check_login_rate_limit(ip="10.0.0.1", email="b@ex.com")["allowed"] is True
    denied = check_login_rate_limit(ip="10.0.0.1", email="c@ex.com")
    assert denied["allowed"] is False
    assert str(denied.get("principal", "")).startswith("ip:")


def test_per_email_throttle_independent_of_ip(monkeypatch):
    """One account attacked from many IPs must still exhaust the email bucket."""
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "2")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "0.01")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "100")
    reset_auth_rate_limits()

    assert check_login_rate_limit(ip="10.0.0.1", email="victim@ex.com")["allowed"] is True
    assert check_login_rate_limit(ip="10.0.0.2", email="victim@ex.com")["allowed"] is True
    denied = check_login_rate_limit(ip="10.0.0.3", email="victim@ex.com")
    assert denied["allowed"] is False
    assert str(denied.get("principal", "")).startswith("email:")


def test_lockout_backoff_is_exponential(monkeypatch):
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "50")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "10")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "2")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_SEC", "30")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_MAX_SEC", "3600")
    reset_auth_rate_limits()

    for _ in range(2):
        assert check_login_rate_limit(ip="1.1.1.1", email="z@ex.com")["allowed"] is True
        record_login_failure(ip="1.1.1.1", email="z@ex.com")
    first = check_login_rate_limit(ip="1.1.1.1", email="z@ex.com")
    assert first["allowed"] is False and first.get("locked") is True
    assert float(first["retry_after_sec"]) == 30.0

    # Expire lockout without waiting wall clock.
    import services.auth_rate_limit as mod

    with mod._LOCK:
        for state in mod._LOCKOUTS.values():
            state.locked_until = 0.0

    for _ in range(2):
        assert check_login_rate_limit(ip="1.1.1.1", email="z@ex.com")["allowed"] is True
        record_login_failure(ip="1.1.1.1", email="z@ex.com")
    second = check_login_rate_limit(ip="1.1.1.1", email="z@ex.com")
    assert second["allowed"] is False and second.get("locked") is True
    assert float(second["retry_after_sec"]) == 60.0

    record_login_success(ip="1.1.1.1", email="z@ex.com")
    assert check_login_rate_limit(ip="1.1.1.1", email="z@ex.com")["allowed"] is True
