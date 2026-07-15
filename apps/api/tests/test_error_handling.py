"""Unit tests for retry classification and RetryBudget env overrides."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.error_handling import RetryBudget, classify_error, with_retry  # noqa: E402


def test_classify_operational_error_as_retriable():
    class FakeOperationalError(Exception):
        pass

    classification = classify_error(FakeOperationalError("server closed the connection unexpectedly"))
    assert classification["retriable"] is True
    assert any("operationalerror" in e for e in classification["evidence"])


def test_classify_unique_violation_as_non_retriable():
    class FakeIntegrityError(Exception):
        pass

    classification = classify_error(FakeIntegrityError("duplicate key value violates unique constraint \"users_pkey\""))
    assert classification["retriable"] is False


def test_classify_connection_refused_as_retriable():
    classification = classify_error(ConnectionRefusedError("Connection refused"))
    assert classification["retriable"] is True


def test_retry_budget_env_overrides(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATAFLOW_RETRY_MAX_ATTEMPTS", "5")
    monkeypatch.setenv("DATAFLOW_RETRY_BASE_DELAY_SECONDS", "0.25")
    monkeypatch.setenv("DATAFLOW_RETRY_MAX_DELAY_SECONDS", "10")
    monkeypatch.setenv("DATAFLOW_RETRY_EXPONENTIAL_BASE", "3")
    monkeypatch.setenv("DATAFLOW_RETRY_JITTER", "false")

    budget = RetryBudget()
    assert budget.max_attempts == 5
    assert budget.base_delay_seconds == 0.25
    assert budget.max_delay_seconds == 10
    assert budget.exponential_base == 3
    assert budget.jitter is False


def test_with_retry_recovers_from_transient_error():
    attempts = []

    def flaky():
        attempts.append(len(attempts))
        if len(attempts) < 2:
            raise ConnectionError("transient")
        return "ok"

    result = with_retry(flaky, budget=RetryBudget(max_attempts=3, base_delay_seconds=0.001))
    assert result == "ok"
    assert len(attempts) == 2


def test_with_retry_gives_up_on_non_retriable():
    calls = []

    def bad():
        calls.append(1)
        raise ValueError("invalid schema")

    with pytest.raises(ValueError):
        with_retry(bad, budget=RetryBudget(max_attempts=3, base_delay_seconds=0.001))
    assert len(calls) == 1
