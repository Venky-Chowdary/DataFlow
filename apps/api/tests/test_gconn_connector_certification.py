from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

from connectors.sdk import (
    ConnectorDescriptor,
    SDK_AUTH_MODES,
    SingerTapBridge,
    get_descriptor,
    list_descriptors,
    list_sdk_connectors,
)
from connectors.sdk.declarative.connector import DeclarativeSource
from connectors.sdk.http_declarative import DeclarativeHttpConnector
from connectors.sdk.hubspot_cdk import HubSpotCDKConnector
from services.connector_auth import _KNOWN_AUTH_MODES
from services.connector_truth_audit import _SECRET_FIELD_KEY
from src.transfer.connector_capabilities import _DRIVER_CAPS
from src.transfer.connector_registry import CONNECTOR_MODULES


def test_sdk_registry_has_a_descriptor_for_every_registered_connector() -> None:
    registered_ids = set(list_sdk_connectors())
    descriptors = {descriptor.id: descriptor for descriptor in list_descriptors()}

    assert registered_ids == set(descriptors)
    assert all(get_descriptor(connector_id) is not None for connector_id in registered_ids)
    assert {
        "singer_tap",
        "hubspot_cdk",
        "declarative_source",
        "declarative_http",
    } <= registered_ids


def test_sdk_descriptors_match_discovered_sync_modes(tmp_path: Path) -> None:
    script = tmp_path / "synthetic_tap.py"
    script.write_text(
        "import json\n"
        "print(json.dumps({'type':'SCHEMA','stream':'items',"
        "'schema':{'type':'object','properties':{'id':{'type':'string'}}},"
        "'key_properties':['id']}))\n",
        encoding="utf-8",
    )
    manifest = {
        "name": "acme",
        "base_url": "http://127.0.0.1:1",
        "auth": {"type": "none"},
        "streams": [
            {
                "name": "items",
                "path": "/items",
                "records_path": "data",
                "primary_key": ["id"],
                "cursor": {"field": "updated_at", "request_param": "since"},
                "json_schema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "updated_at": {"type": "string"},
                    },
                },
            }
        ],
    }
    connectors = [
        HubSpotCDKConnector({"api_key": "synthetic-secret"}),
        SingerTapBridge({"tap_command": [sys.executable, str(script)]}),
        DeclarativeSource({"manifest": manifest}),
        DeclarativeHttpConnector(
            {
                "api_key": "synthetic-secret",
                "spec": {
                    "base_url": "http://127.0.0.1:1",
                    "streams": [
                        {
                            "name": "items",
                            "path": "/items",
                            "records_path": "data",
                            "primary_key": ["id"],
                            "cursor_field": "updated_at",
                            "cursor_param": "since",
                            "properties": {"id": "string", "updated_at": "string"},
                        }
                    ],
                },
            }
        ),
    ]

    for connector in connectors:
        modes = {
            mode
            for stream in connector.discover()
            for mode in stream.supported_sync_modes
        }
        descriptor = get_descriptor(connector.name)
        assert descriptor is not None
        assert set(descriptor.sync_modes) == modes


def test_sdk_descriptor_auth_modes_and_secret_fields_are_honest() -> None:
    allowed_modes = _KNOWN_AUTH_MODES | SDK_AUTH_MODES
    for descriptor in list_descriptors():
        assert set(descriptor.auth_modes) <= allowed_modes
        assert descriptor.evidence in {"synthetic-fixture", "live"}
        for field in descriptor.form_fields:
            if _SECRET_FIELD_KEY.search(str(field.get("name", ""))):
                assert field.get("sensitive") is True


def test_sdk_registry_drift_matches_transfer_handoffs() -> None:
    api_root = Path(__file__).resolve().parents[1]
    list_files = (
        api_root / "src/transfer/connector_capabilities.py",
        api_root / "src/transfer/connector_registry.py",
        api_root / "src/transfer/adapters.py",
        api_root / "src/transfer/batch_readers.py",
        api_root / "data/connector_form_schema.json",
    )
    sdk_ids = set(list_sdk_connectors())
    descriptor_ids = {descriptor.id for descriptor in list_descriptors()}
    mentions: dict[Path, set[str]] = {}
    for path in list_files:
        source = path.read_text(encoding="utf-8")
        mentions[path] = {
            connector_id
            for connector_id in sdk_ids
            if re.search(rf"\b{re.escape(connector_id)}\b", source)
        }
        assert mentions[path] <= descriptor_ids

    capabilities_path, registry_path = list_files[:2]
    assert "singer_tap" in mentions[capabilities_path]
    assert "singer_tap" in mentions[registry_path]
    singer = get_descriptor("singer_tap")
    assert singer is not None
    singer_caps = _DRIVER_CAPS["singer_tap"]
    assert singer_caps["read"] is ("source" in singer.roles)
    assert singer_caps["write"] is ("destination" in singer.roles)
    singer_modules = CONNECTOR_MODULES["singer_tap"]
    assert bool(singer_modules.reader) is ("source" in singer.roles)
    assert (singer_modules.writer_fn != "write_not_supported") is (
        "destination" in singer.roles
    )


def test_connector_descriptor_is_frozen_and_rejects_empty_skip_reasons() -> None:
    descriptor = ConnectorDescriptor(
        id="sample",
        display_name="Sample",
        roles=("source",),
        auth_modes=(),
        sync_modes=("full_refresh",),
        form_fields=(),
        evidence="synthetic-fixture",
    )
    with pytest.raises((AttributeError, TypeError)):
        descriptor.display_name = "Changed"  # type: ignore[misc]
    with pytest.raises(ValueError, match="skip names and reasons"):
        ConnectorDescriptor(
            id="sample",
            display_name="Sample",
            roles=("source",),
            auth_modes=(),
            sync_modes=("full_refresh",),
            form_fields=(),
            evidence="synthetic-fixture",
            certification_skips={"rate_limit": " "},
        )
