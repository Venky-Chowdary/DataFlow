from __future__ import annotations

import logging
import time

import pytest
import requests

from connectors.sdk.declarative.errors import (
    ConnectorAuthError,
    ConnectorRequestError,
    RateLimitExhausted,
    ResponseShapeError,
    TransientExhausted,
)
from connectors.sdk.declarative.requester import HttpRequester
from services.secret_config import redact_url
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer


def _requester(*, sleeps: list[float] | None = None, attempts: int = 3) -> HttpRequester:
    return HttpRequester(
        timeout_s=2,
        max_attempts=attempts,
        base_delay_seconds=0.1,
        max_delay_seconds=1.0,
        sleep=(sleeps if sleeps is not None else []).append,
    )


def test_retry_after_response_is_obeyed_and_request_id_is_sent() -> None:
    sleeps: list[float] = []
    with FixtureServer() as fixture:
        fixture.add_route(
            "/items",
            responses=[
                FixtureResponse(
                    status=429,
                    body={"error": "slow down"},
                    headers={"Retry-After": "2"},
                ),
                FixtureResponse(body={"items": [{"id": 1}]}),
            ],
        )
        requester = _requester(sleeps=sleeps, attempts=2)

        result = requester.request_json(
            "GET",
            f"{fixture.base_url}/items",
            stream="items",
        )

        assert result.payload == {"items": [{"id": 1}]}
        assert sleeps and sleeps[0] >= 2.0
        sent_ids = [
            entry.headers["X-Request-ID"] for entry in fixture.request_log
        ]
        assert sent_ids[0] and len(set(sent_ids)) == 1
        assert result.request_id == sent_ids[0]


def test_server_request_id_is_preserved_on_exhausted_rate_limit() -> None:
    secret = "query-secret"
    with FixtureServer() as fixture:
        fixture.add_route(
            "/items",
            FixtureResponse(
                status=429,
                body={"error": "slow down"},
                headers={"x-amzn-requestid": "upstream-429"},
            ),
        )
        requester = _requester(attempts=1)

        with pytest.raises(RateLimitExhausted) as raised:
            requester.request_json(
                "GET",
                f"{fixture.base_url}/items?client_secret={secret}",
                stream="items",
            )

        assert raised.value.request_id == "upstream-429"
        assert raised.value.stream == "items"
        assert raised.value.status == 429
        assert secret not in str(raised.value)
        assert "client_secret=%2A%2A%2A" in str(raised.value)


def test_server_error_retries_then_returns_exact_json() -> None:
    sleeps: list[float] = []
    with FixtureServer() as fixture:
        fixture.add_route(
            "/records",
            responses=[
                FixtureResponse(status=503, body={"error": "temporary"}),
                FixtureResponse(
                    body='{"records":[{"n":9007199254740993}]}',
                    headers={"Content-Type": "application/json"},
                ),
            ],
        )
        requester = _requester(sleeps=sleeps, attempts=2)

        result = requester.request_json("GET", f"{fixture.base_url}/records")

        assert result.payload["records"][0]["n"] == 9007199254740993
        assert len(fixture.request_log) == 2
        assert len(sleeps) == 1


def test_github_rate_limit_reset_is_obeyed() -> None:
    sleeps: list[float] = []
    with FixtureServer() as fixture:
        fixture.add_route(
            "/github",
            responses=[
                FixtureResponse(
                    status=403,
                    body={"message": "API rate limit exceeded"},
                    headers={
                        "x-ratelimit-remaining": "0",
                        "x-ratelimit-reset": str(time.time() + 5),
                    },
                ),
                FixtureResponse(body={"ok": True}),
            ],
        )
        requester = _requester(sleeps=sleeps, attempts=2)

        result = requester.request_json("GET", f"{fixture.base_url}/github")

        assert result.payload == {"ok": True}
        assert sleeps and sleeps[0] >= 4.0
        assert len(fixture.request_log) == 2


def test_invalid_json_response_is_a_typed_shape_error() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/invalid",
            FixtureResponse(
                body="{",
                headers={"Content-Type": "application/json", "x-request-id": "bad-json"},
            ),
        )
        requester = _requester(attempts=1)

        with pytest.raises(ResponseShapeError) as raised:
            requester.request_json("GET", f"{fixture.base_url}/invalid")

        assert raised.value.request_id == "bad-json"


def test_bad_request_is_typed_and_not_retried() -> None:
    sleeps: list[float] = []
    with FixtureServer() as fixture:
        fixture.add_route(
            "/bad",
            FixtureResponse(status=400, body={"error": "invalid"}),
        )
        requester = _requester(sleeps=sleeps, attempts=3)

        with pytest.raises(ConnectorRequestError) as raised:
            requester.request_json("GET", f"{fixture.base_url}/bad")

        assert raised.value.status == 400
        assert len(fixture.request_log) == 1
        assert sleeps == []


def test_auth_error_refreshes_once_and_second_failure_is_typed() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/auth",
            responses=[
                FixtureResponse(status=401, body={"error": "expired"}),
                FixtureResponse(body={"ok": True}),
            ],
        )
        requester = _requester(attempts=2)
        refreshes: list[int] = []

        def refresh() -> dict[str, str]:
            refreshes.append(1)
            return {"Authorization": "Bearer refreshed"}

        result = requester.request_json(
            "GET",
            f"{fixture.base_url}/auth",
            headers={"Authorization": "Bearer expired"},
            refresh_auth=refresh,
        )

        assert result.payload == {"ok": True}
        assert len(refreshes) == 1
        assert [
            entry.headers["Authorization"] for entry in fixture.request_log
        ] == ["Bearer expired", "Bearer refreshed"]

    with FixtureServer() as fixture:
        fixture.add_route(
            "/auth",
            FixtureResponse(status=401, body={"error": "expired"}),
        )
        requester = _requester(attempts=2)
        refreshes = []

        with pytest.raises(ConnectorAuthError) as raised:
            requester.request_json(
                "GET",
                f"{fixture.base_url}/auth",
                headers={"Authorization": "Bearer expired"},
                refresh_auth=refresh,
                stream="accounts",
            )

        assert len(refreshes) == 1
        assert raised.value.stream == "accounts"
        assert raised.value.status == 401


def test_auth_refresh_error_exposes_safe_cause_without_leaking_secret(caplog) -> None:
    secret = "planted-refresh-response-secret"
    refresh_response = requests.Response()
    refresh_response.status_code = 503
    refresh_response.headers["X-Correlation-ID"] = "refresh-failure-17"
    refresh_error = requests.HTTPError(secret, response=refresh_response)

    def refresh() -> dict[str, str]:
        raise refresh_error

    with FixtureServer() as fixture:
        fixture.add_route("/auth", FixtureResponse(status=401, body={"error": "expired"}))
        requester = _requester(attempts=1)
        with caplog.at_level(logging.DEBUG):
            with pytest.raises(ConnectorAuthError) as raised:
                requester.request_json(
                    "GET",
                    f"{fixture.base_url}/auth?access_token={secret}",
                    refresh_auth=refresh,
                    stream="accounts",
                )

    message = str(raised.value)
    assert "HTTPError" in message
    assert "503" in message
    assert "refresh-failure-17" in message
    assert secret not in message
    assert secret not in caplog.text


def test_connection_failure_exhaustion_is_typed_with_request_id() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/drop",
            FixtureResponse(drop_connection=True),
        )
        requester = _requester(attempts=1)

        with pytest.raises(TransientExhausted) as raised:
            requester.request_json("GET", f"{fixture.base_url}/drop")

        assert raised.value.request_id
        assert raised.value.status is None
        assert len(fixture.request_log) == 1


def test_redaction_scrubs_urls_and_logged_headers(caplog) -> None:
    assert "very-secret" not in redact_url(
        "https://example.test/items?apiKey=very-secret&limit=1"
    )
    with FixtureServer() as fixture:
        fixture.add_route("/safe", FixtureResponse(body={"ok": True}))
        requester = _requester()
        with caplog.at_level(logging.DEBUG):
            requester.request_json(
                "GET",
                f"{fixture.base_url}/safe?access_token=very-secret",
                headers={
                    "Authorization": "Bearer very-secret",
                    "X-Secret-Token": "custom-very-secret",
                },
            )

        assert "very-secret" not in caplog.text
        assert "custom-very-secret" not in caplog.text
        assert "access_token=%2A%2A%2A" in caplog.text
        assert "Authorization" in caplog.text


def test_injected_clock_and_sleep_control_client_token_bucket() -> None:
    now = [10.0]
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    with FixtureServer() as fixture:
        fixture.add_route("/a", FixtureResponse(body={"ok": True}))
        requester = HttpRequester(
            timeout_s=2,
            max_attempts=1,
            rate_limit_per_second=1.0,
            clock=lambda: now[0],
            sleep=sleep,
        )

        requester.request_json("GET", f"{fixture.base_url}/a")
        requester.request_json("GET", f"{fixture.base_url}/a")

        assert sleeps and sleeps[0] >= 1.0
        assert len(fixture.request_log) == 2
