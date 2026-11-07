from __future__ import annotations

from types import SimpleNamespace

from services import error_handling
from services.error_handling import RetryBudget, retry_after_seconds, with_retry


class _HttpFailure(Exception):
    def __init__(self, status: int, headers: dict[str, str]) -> None:
        super().__init__(f"HTTP {status} Too Many Requests")
        self.response = SimpleNamespace(status_code=status, headers=headers)


def test_rate_limit_reset_is_used_when_retry_after_is_absent(monkeypatch) -> None:
    monkeypatch.setattr(
        error_handling,
        "time",
        SimpleNamespace(time=lambda: 100.0),
    )

    assert retry_after_seconds(
        _HttpFailure(429, {"X-RateLimit-Reset": "150"})
    ) == 50.0


def test_github_reset_is_limited_to_403_with_zero_remaining(monkeypatch) -> None:
    monkeypatch.setattr(
        error_handling,
        "time",
        SimpleNamespace(time=lambda: 100.0),
    )

    assert retry_after_seconds(
        _HttpFailure(
            403,
            {"x-ratelimit-reset": "150", "x-ratelimit-remaining": "0"},
        )
    ) == 50.0
    assert retry_after_seconds(
        _HttpFailure(
            403,
            {"x-ratelimit-reset": "150", "x-ratelimit-remaining": "1"},
        )
    ) is None
    assert retry_after_seconds(
        _HttpFailure(503, {"x-ratelimit-reset": "150"})
    ) is None


def test_retry_after_takes_precedence_over_rate_limit_reset(monkeypatch) -> None:
    monkeypatch.setattr(
        error_handling,
        "time",
        SimpleNamespace(time=lambda: 100.0),
    )

    assert retry_after_seconds(
        _HttpFailure(
            429,
            {"Retry-After": "7", "X-RateLimit-Reset": "150"},
        )
    ) == 7.0


def test_rate_limit_reset_is_capped_and_past_values_retry_immediately(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        error_handling,
        "time",
        SimpleNamespace(time=lambda: 100.0),
    )

    assert retry_after_seconds(
        _HttpFailure(429, {"x-ratelimit-reset": "1000"})
    ) == 300.0
    assert retry_after_seconds(
        _HttpFailure(429, {"x-ratelimit-reset": "99"})
    ) == 0.0


def test_with_retry_uses_injected_sleep() -> None:
    attempts = 0
    delays: list[float] = []

    def request() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _HttpFailure(429, {"Retry-After": "2"})
        return "ok"

    result = with_retry(
        request,
        budget=RetryBudget(
            max_attempts=2,
            base_delay_seconds=0.1,
            max_delay_seconds=1.0,
            jitter=False,
        ),
        sleep=delays.append,
    )

    assert result == "ok"
    assert delays == [2.0]
    assert attempts == 2


def test_rate_limit_reset_requires_a_valid_header_value(monkeypatch) -> None:
    monkeypatch.setattr(
        error_handling,
        "time",
        SimpleNamespace(time=lambda: 100.0),
    )

    assert retry_after_seconds(
        _HttpFailure(429, {"Retry-After": "", "X-RateLimit-Reset": "150"})
    ) is None
    assert retry_after_seconds(
        _HttpFailure(429, {"X-RateLimit-Reset": "not-an-epoch"})
    ) is None
