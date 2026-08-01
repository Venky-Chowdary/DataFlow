"""HubSpot / Zendesk live Describe → typed quarantine carriers."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.hubspot_writer import (  # noqa: E402
    hubspot_property_to_carrier,
    resolve_hubspot_dest_types,
)
from connectors.writer_common import apply_write_quarantine_matrix  # noqa: E402
from connectors.zendesk_writer import (  # noqa: E402
    resolve_zendesk_dest_types,
    zendesk_field_to_carrier,
)


def test_hubspot_property_to_carrier_platform_limits():
    assert (
        hubspot_property_to_carrier({"type": "string", "fieldType": "text"})
        == "VARCHAR(65536)"
    )
    assert (
        hubspot_property_to_carrier(
            {
                "type": "string",
                "fieldType": "text",
                "validationRules": [{"ruleType": "MAX_LENGTH", "ruleArguments": ["80"]}],
            }
        )
        == "VARCHAR(80)"
    )
    assert (
        hubspot_property_to_carrier(
            {"type": "number", "fieldType": "number", "numberDisplayHint": "currency"}
        )
        == "DECIMAL(38,2)"
    )
    assert (
        hubspot_property_to_carrier({"type": "number", "fieldType": "number"})
        == "DECIMAL(38,10)"
    )
    assert hubspot_property_to_carrier({"type": "bool"}) == "BOOLEAN"
    assert hubspot_property_to_carrier({"type": "datetime"}) == "TIMESTAMPTZ"
    assert (
        hubspot_property_to_carrier({"type": "enumeration", "fieldType": "select"})
        == "VARCHAR(256)"
    )


def test_resolve_hubspot_dest_types_prefers_describe():
    types = resolve_hubspot_dest_types(
        ["email", "amount"],
        [
            {"source": "e", "target": "email", "target_type": "VARCHAR(500)"},
            {"source": "a", "target": "amount", "target_type": "FLOAT"},
        ],
        {"e": "TEXT", "a": "FLOAT"},
        describe_props=[
            {
                "name": "email",
                "type": "string",
                "fieldType": "text",
                "validationRules": [
                    {"ruleType": "MAX_LENGTH", "ruleArguments": ["254"]}
                ],
            },
            {
                "name": "amount",
                "type": "number",
                "fieldType": "number",
                "numberDisplayHint": "currency",
            },
        ],
    )
    assert types["email"] == "VARCHAR(254)"
    assert types["amount"] == "DECIMAL(38,2)"


def test_hubspot_quarantine_holds_string_overflow():
    details: list[dict] = []
    out = apply_write_quarantine_matrix(
        [("A" * 100,), ("ok",)],
        ["note"],
        ["VARCHAR(40)"],
        details,
        policy="quarantine",
        dialect_label="HubSpot",
    )
    assert out == [("ok",)]
    assert any("HubSpot" in d.get("reason", "") for d in details)


def test_zendesk_field_to_carrier_system_and_custom():
    assert zendesk_field_to_carrier({"type": "subject"}) == "VARCHAR(255)"
    assert zendesk_field_to_carrier({"type": "description"}) == "VARCHAR(65535)"
    assert zendesk_field_to_carrier({"type": "text"}) == "VARCHAR(65536)"
    assert zendesk_field_to_carrier({"type": "textarea"}) == "VARCHAR(65536)"
    assert zendesk_field_to_carrier({"type": "checkbox"}) == "BOOLEAN"
    assert zendesk_field_to_carrier({"type": "integer"}) == "INTEGER"
    assert zendesk_field_to_carrier({"type": "decimal"}) == "DECIMAL(38,10)"
    assert zendesk_field_to_carrier({"type": "tagger"}) == "VARCHAR(255)"


def test_resolve_zendesk_dest_types_prefers_describe_and_system_seeds():
    types = resolve_zendesk_dest_types(
        ["subject", "Order Number", "email"],
        [
            {"source": "s", "target": "subject", "target_type": "TEXT"},
            {"source": "o", "target": "Order Number", "target_type": "TEXT"},
            {"source": "e", "target": "email", "target_type": "TEXT"},
        ],
        {},
        describe_fields=[
            {"name": "subject", "title": "Subject", "type": "subject"},
            {"name": "3600123", "title": "Order Number", "type": "text"},
        ],
    )
    assert types["subject"] == "VARCHAR(255)"
    assert types["Order Number"] == "VARCHAR(65536)"
    assert types["email"] == "VARCHAR(255)"


def test_zendesk_quarantine_holds_subject_overflow():
    details: list[dict] = []
    out = apply_write_quarantine_matrix(
        [("S" * 300,), ("ok subject",)],
        ["subject"],
        ["VARCHAR(255)"],
        details,
        policy="quarantine",
        dialect_label="Zendesk",
    )
    assert out == [("ok subject",)]
    assert any("Zendesk" in d.get("reason", "") for d in details)
