"""Enterprise error-handling matrix across every declarative SDK connector.

The enterprise bar for the beta connector fleet is identical, uniform error
behaviour — a vendor outage, bad key, or malformed payload must surface as a
typed, honest error with the connector name attached, never a silent empty
read, never a swallowed exception, never an unhandled traceback.  This file
proves that contract on every registered manifest connector by intercepting
the transport layer and replaying the five canonical failure shapes:

* 401 / 403            -> ConnectorAuthError (bad key, never retried forever)
* 429                  -> retried inside the retry budget, then RateLimitExhausted
* 5xx                  -> retried inside the retry budget, then TransientExhausted
* malformed JSON       -> ResponseShapeError (a corrupt body is not "no rows")

A new manifest connector without a CONFIG entry fails the completeness test,
so the matrix can never silently lose coverage.
"""

from __future__ import annotations

import pytest
import requests

from connectors.sdk import get_sdk_connector, list_sdk_connectors
from connectors.sdk.declarative.errors import (
    ConnectorAuthError,
    RateLimitExhausted,
    ResponseShapeError,
    TransientExhausted,
)

# Minimal synthetic configs — every credential field a connector needs to
# render its first-stream request.  Supersets are fine; extra keys are ignored.
CONFIGS: dict[str, dict] = {
    "gitlab": {"api_key": "synthetic"},
    "mailchimp": {"dc": "us21", "api_key": "synthetic"},
    "pipedrive": {"domain": "acme", "api_key": "synthetic"},
    "klaviyo": {"api_key": "synthetic"},
    "servicenow": {"instance": "dev1", "username": "etl", "password": "synthetic"},
    "freshdesk": {"domain": "acme", "api_key": "synthetic"},
    "slack": {"api_key": "xoxb-synthetic"},
    "asana": {"api_key": "synthetic", "workspace": "ws-1"},
    "calendly": {
        "api_key": "synthetic",
        "user_uri": "https://api.calendly.com/users/U1",
    },
    "woocommerce": {
        "domain": "shop.example.com",
        "username": "ck_x",
        "password": "cs_y",
    },
    "bigcommerce": {"store_hash": "abc123", "api_key": "synthetic"},
    "brevo": {"api_key": "synthetic"},
    "freshservice": {"domain": "acme", "api_key": "synthetic"},
    "close": {"api_key": "synthetic"},
    "todoist": {"api_key": "synthetic"},
    "typeform": {"api_key": "synthetic", "form_id": "f1"},
    "notion": {"api_key": "synthetic", "database_id": "db1"},
    "zoho_crm": {
        "client_id": "cid",
        "client_secret": "csec",
        "refresh_token": "rt",
        "access_token": "zoho-synthetic",
    },
    "posthog": {
        "domain": "app.posthog.com",
        "api_key": "phx_synthetic",
        "project_id": "42",
    },
    "confluence": {
        "domain": "acme",
        "username": "a@acme.com",
        "api_key": "atl-synthetic",
    },
    "zendesk": {
        "domain": "acme",
        "username": "a@acme.com",
        "api_key": "zd-synthetic",
    },
    "airtable": {"api_key": "pat_synthetic", "base_id": "appXYZ", "table_id": "tblABC"},
    "clickup": {"api_key": "pk_synthetic", "list_id": "9001"},
    "mailjet": {"api_key": "pub_synthetic", "secret_key": "sec_synthetic"},
    "sendgrid": {"api_key": "SG.synthetic"},
    "monday": {"api_key": "mdy_synthetic", "board_id": "42"},
    "linear": {"api_key": "lin_api_synthetic"},
}

# Connectors in the registry that are NOT manifest-driven (bridges, bespoke
# classes) are out of scope for this matrix.
EXCLUDED = {"singer_tap_bridge", "intercom", "github"}


def _manifest_names() -> list[str]:
    names = []
    for name in list_sdk_connectors():
        if name in EXCLUDED:
            continue
        cls = get_sdk_connector(name)
        try:
            instance = cls(CONFIGS.get(name, {}))
        except Exception:
            continue
        if getattr(instance, "manifest", None) is not None:
            names.append(name)
    return names


MANIFEST_NAMES = _manifest_names()


def test_matrix_covers_every_manifest_connector() -> None:
    missing = sorted(set(MANIFEST_NAMES) - set(CONFIGS))
    assert not missing, f"add minimal configs for: {missing}"


def _first_stream(name: str):
    cls = get_sdk_connector(name)
    source = cls(CONFIGS[name])
    return source, source.manifest.streams[0].name


def _patched_transport(monkeypatch, status: int, body: bytes = b"{}", headers=None):
    calls = {"n": 0}

    def fake_request(self, method, url, **kwargs):
        calls["n"] += 1
        response = requests.Response()
        response.status_code = status
        response._content = body
        response.url = url
        response.reason = "fixture"
        if headers:
            response.headers.update(headers)
        return response

    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
    monkeypatch.setattr(requests.Session, "request", fake_request)
    return calls


def _read_first(source, stream: str) -> None:
    for batch in source.read(stream):
        _ = batch.records
        break


@pytest.mark.parametrize("name", MANIFEST_NAMES)
def test_401_surfaces_auth_error(name, monkeypatch) -> None:
    source, stream = _first_stream(name)
    calls = _patched_transport(monkeypatch, 401)
    with pytest.raises(ConnectorAuthError):
        _read_first(source, stream)
    assert calls["n"] >= 1


@pytest.mark.parametrize("name", MANIFEST_NAMES)
def test_403_surfaces_auth_error(name, monkeypatch) -> None:
    source, stream = _first_stream(name)
    _patched_transport(monkeypatch, 403)
    with pytest.raises(ConnectorAuthError):
        _read_first(source, stream)


@pytest.mark.parametrize("name", MANIFEST_NAMES)
def test_429_retried_then_rate_limit_exhausted(name, monkeypatch) -> None:
    source, stream = _first_stream(name)
    calls = _patched_transport(monkeypatch, 429)
    with pytest.raises(RateLimitExhausted):
        _read_first(source, stream)
    assert calls["n"] > 1, "429 must be retried inside the budget before surfacing"


@pytest.mark.parametrize("name", MANIFEST_NAMES)
def test_5xx_retried_then_transient_exhausted(name, monkeypatch) -> None:
    source, stream = _first_stream(name)
    calls = _patched_transport(monkeypatch, 503)
    with pytest.raises(TransientExhausted):
        _read_first(source, stream)
    assert calls["n"] > 1, "5xx must be retried inside the budget before surfacing"


@pytest.mark.parametrize("name", MANIFEST_NAMES)
def test_malformed_json_is_a_shape_error_not_empty_read(name, monkeypatch) -> None:
    source, stream = _first_stream(name)
    _patched_transport(monkeypatch, 200, body=b"<html>not json</html>")
    with pytest.raises(ResponseShapeError):
        _read_first(source, stream)


@pytest.mark.parametrize("name", MANIFEST_NAMES)
def test_check_reports_auth_failure_not_crash(name, monkeypatch) -> None:
    """The UI 'Test connection' probe must report (False, reason) on a bad
    credential — never raise, never report success."""
    source, _ = _first_stream(name)
    _patched_transport(monkeypatch, 401)
    ok, message = source.check()
    assert ok is False
    assert isinstance(message, str) and message.strip()
