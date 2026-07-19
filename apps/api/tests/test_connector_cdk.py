"""Connector CDK protocol + HubSpot golden + declarative HTTP."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.sdk import (
    BaseConnector,
    RecordBatch,
    SingerTapBridge,
    StreamSchema,
    get_sdk_connector,
    list_sdk_connectors,
)
from connectors.sdk.http_declarative import DeclarativeHttpConnector, parse_declarative_spec
from connectors.sdk.hubspot_cdk import HubSpotCDKConnector
from connectors.sdk.oauth import OAuth2Spec, OAuth2Tokens, apply_tokens_to_config, refresh_oauth2_token


def test_sdk_registry_includes_builtins() -> None:
    names = list_sdk_connectors()
    assert "singer_tap" in names
    assert "declarative_http" in names
    assert "hubspot_cdk" in names
    assert get_sdk_connector("hubspot_cdk") is HubSpotCDKConnector


def test_hubspot_cdk_spec_discover() -> None:
    c = HubSpotCDKConnector({"api_key": "tok"})
    spec = c.spec()
    assert "api_key" in spec["connectionSpecification"]["properties"]
    streams = c.discover()
    assert {s.name for s in streams} >= {"contacts", "companies", "deals"}
    contacts = next(s for s in streams if s.name == "contacts")
    assert contacts.cursor_field
    assert "id" in contacts.primary_key


def test_hubspot_cdk_check_and_read() -> None:
    c = HubSpotCDKConnector({"api_key": "tok"})
    with patch("connectors.sdk.hubspot_cdk.request") as req:
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {
            "results": [{"id": "1", "properties": {"email": "a@b.com", "lastmodifieddate": "2026-01-01"}}],
            "paging": {"next": {"after": "cursor2"}},
        }
        req.return_value = resp
        ok, msg = c.check()
        assert ok is True
        batches = list(c.read("contacts", state=None, limit=10))
        assert len(batches) == 1
        assert batches[0].records[0]["email"] == "a@b.com"
        assert batches[0].state.get("contacts", {}).get("after") == "cursor2"


def test_declarative_http_discover_and_read() -> None:
    spec = {
        "name": "demo",
        "base_url": "https://example.com/api/",
        "streams": [
            {
                "name": "items",
                "path": "items",
                "primary_key": ["id"],
                "records_path": "data",
                "cursor_field": "updated_at",
                "properties": {"id": "string", "updated_at": "string"},
            }
        ],
    }
    parsed = parse_declarative_spec(spec)
    assert parsed.streams[0].name == "items"
    c = DeclarativeHttpConnector({"api_key": "x", "spec": spec})
    assert len(c.discover()) == 1
    with patch("connectors.sdk.http_declarative.requests.get") as get:
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"data": [{"id": "1", "updated_at": "t1"}]}
        get.return_value = resp
        batch = next(c.read("items", state=None, limit=5))
        assert batch.records[0]["id"] == "1"
        assert batch.state["items"]["updated_at"] == "t1"


def test_oauth_apply_and_refresh_mock() -> None:
    tokens = OAuth2Tokens(access_token="new", refresh_token="r2", expires_at=9999999999)
    cfg = apply_tokens_to_config({"api_key": "old"}, tokens)
    assert cfg["api_key"] == "new"
    assert cfg["credentials"]["refresh_token"] == "r2"
    with patch("connectors.sdk.oauth.requests.post") as post:
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.content = b"{}"
        resp.json.return_value = {
            "access_token": "atok",
            "refresh_token": "rtok",
            "expires_in": 3600,
        }
        post.return_value = resp
        out = refresh_oauth2_token(
            OAuth2Spec(
                token_url="https://auth.example/token",
                client_id="cid",
                client_secret="sec",
                refresh_token="r1",
            )
        )
        assert out.access_token == "atok"
        assert out.refresh_token == "rtok"


def test_base_connector_protocol_methods() -> None:
    class Demo(BaseConnector):
        name = "demo_proto"

        def test_connection(self) -> bool:
            return True

        def discover(self) -> list[StreamSchema]:
            return [StreamSchema(name="s", properties={"a": "string"}, primary_key=["a"])]

        def read(self, stream, *, state=None, offset=0, limit=1000):
            yield RecordBatch(stream=stream, records=[{"a": "1"}], state={"s": {"a": "1"}})

    d = Demo({})
    assert d.check()[0] is True
    assert d.discover()[0].name == "s"
    assert next(d.read("s")).state["s"]["a"] == "1"


def test_singer_check_without_binary() -> None:
    bridge = SingerTapBridge({"tap_command": "tap-does-not-exist-xyz"})
    ok, msg = bridge.check()
    assert ok is False
    assert "not found" in msg.lower() or "tap" in msg.lower()
