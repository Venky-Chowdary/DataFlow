from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest
import requests

import connectors.saas_common as saas_common
import connectors.sdk.hubspot_cdk as hubspot_module
from connectors.sdk import SingerTapBridge, SingerTapError
from connectors.sdk.declarative.errors import ConnectorAuthError
from connectors.sdk.hubspot_cdk import HubSpotAuthError, HubSpotCDKConnector
from services.error_handling import RetryBudget


def test_saas_request_uses_injected_sleep_for_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = []
    for status, headers in (
        (429, {"Retry-After": "2"}),
        (200, {}),
    ):
        response = requests.Response()
        response.status_code = status
        response.headers.update(headers)
        response.url = "https://api.example.test/items"
        response._content = b"{}"
        responses.append(response)

    calls = []

    def fake_request(**kwargs):
        calls.append(kwargs)
        return responses.pop(0)

    monkeypatch.setattr(saas_common.requests, "request", fake_request)
    slept: list[float] = []
    result = saas_common.request(
        method="GET",
        url="https://api.example.test/items",
        retry_budget=RetryBudget(
            max_attempts=2,
            base_delay_seconds=0,
            max_delay_seconds=5,
            jitter=False,
        ),
        sleep=slept.append,
    )

    assert result.status_code == 200
    assert len(calls) == 2
    assert slept and slept[0] >= 2


@pytest.mark.parametrize(
    ("status", "headers", "auth_error"),
    [
        (401, {}, True),
        (403, {}, True),
        (403, {"X-RateLimit-Remaining": "0"}, False),
    ],
)
def test_hubspot_read_classifies_auth_and_rate_limit_errors(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    status: int,
    headers: dict[str, str],
    auth_error: bool,
) -> None:
    secret = "m3-hubspot-planted-secret"
    response = requests.Response()
    response.status_code = status
    response.headers.update(headers)
    response.url = "https://fixture.example.test/contacts"
    error = requests.HTTPError(f"HTTP {status}", response=response)
    slept: list[float] = []

    def fake_sleep(delay: float) -> None:
        slept.append(delay)

    def fake_request(**kwargs):
        assert kwargs["sleep"] is fake_sleep
        raise error

    monkeypatch.setattr(hubspot_module, "request", fake_request)
    connector = HubSpotCDKConnector({"api_key": secret}, sleep=fake_sleep)

    with caplog.at_level(logging.DEBUG):
        if auth_error:
            ok, message = connector.check()
            assert not ok
            assert secret not in message
            with pytest.raises(HubSpotAuthError) as raised:
                list(connector.read("contacts"))
            assert isinstance(raised.value, ConnectorAuthError)
            assert secret not in str(raised.value)
        else:
            with pytest.raises(requests.HTTPError) as raised:
                list(connector.read("contacts"))
            assert not isinstance(raised.value, HubSpotAuthError)
        assert secret not in caplog.text


def test_singer_nonzero_exit_is_typed_and_redacts_tap_config(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "m3-singer-planted-secret"
    script = tmp_path / "failed_tap.py"
    script.write_text(
        "import json, sys\n"
        "args = sys.argv\n"
        "if '--config' in args:\n"
        "    with open(args[args.index('--config') + 1], encoding='utf-8') as f:\n"
        "        sys.stderr.write(json.load(f)['api_key'])\n"
        "sys.exit(1)\n",
        encoding="utf-8",
    )
    connector = SingerTapBridge(
        {
            "tap_command": [sys.executable, str(script)],
            "tap_config": {"api_key": secret},
        }
    )

    with caplog.at_level(logging.DEBUG):
        ok, message = connector.check()
        assert not ok
        assert secret not in message
        with pytest.raises(SingerTapError) as raised:
            list(connector.read("items"))
        assert secret not in str(raised.value)
        assert secret not in caplog.text
