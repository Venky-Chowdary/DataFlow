"""Throttled SaaS writes must obey server-directed ``Retry-After``.

Blast radius: every connector that goes through ``saas_common.request`` /
``with_retry`` (Salesforce, HubSpot, Graph, Zendesk, vector stores).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest

from services.error_handling import RetryBudget, retry_after_seconds, with_retry


class _Resp:
    def __init__(self, headers: dict[str, str]):
        self.headers = headers
        self.status_code = 429


class _Throttled(Exception):
    def __init__(self, headers: dict[str, str]):
        super().__init__("429 Too Many Requests")
        self.response = _Resp(headers)


def test_delta_seconds_header_is_honoured():
    assert retry_after_seconds(_Throttled({"Retry-After": "30"})) == 30.0


def test_header_is_case_insensitive():
    assert retry_after_seconds(_Throttled({"retry-after": "7"})) == 7.0


def test_http_date_header_is_honoured():
    when = datetime.now(timezone.utc) + timedelta(seconds=45)
    got = retry_after_seconds(_Throttled({"Retry-After": format_datetime(when)}))
    assert got is not None and 30.0 <= got <= 50.0


def test_past_http_date_means_retry_now():
    when = datetime.now(timezone.utc) - timedelta(seconds=45)
    assert retry_after_seconds(_Throttled({"Retry-After": format_datetime(when)})) == 0.0


def test_header_is_capped_so_one_response_cannot_park_a_worker():
    got = retry_after_seconds(_Throttled({"Retry-After": "86400"}))
    assert got is not None and got <= 300.0


def test_missing_or_garbage_header_falls_back_to_our_backoff():
    assert retry_after_seconds(_Throttled({})) is None
    assert retry_after_seconds(_Throttled({"Retry-After": "soon"})) is None
    assert retry_after_seconds(RuntimeError("429")) is None


def test_with_retry_waits_at_least_the_server_hint(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr("services.error_handling.time.sleep", slept.append)

    calls = {"n": 0}

    def _fn():
        calls["n"] += 1
        if calls["n"] == 1:
            raise _Throttled({"Retry-After": "42"})
        return "ok"

    budget = RetryBudget(max_attempts=3, base_delay_seconds=0.5, max_delay_seconds=1.0)
    assert with_retry(_fn, budget=budget) == "ok"
    assert slept and slept[0] == pytest.approx(42.0)


def test_with_retry_keeps_exponential_backoff_without_a_hint(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr("services.error_handling.time.sleep", slept.append)

    calls = {"n": 0}

    def _fn():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("503 Service Unavailable")
        return "ok"

    budget = RetryBudget(
        max_attempts=3, base_delay_seconds=2.0, max_delay_seconds=10.0, jitter=False
    )
    assert with_retry(_fn, budget=budget) == "ok"
    assert slept == [pytest.approx(2.0)]
