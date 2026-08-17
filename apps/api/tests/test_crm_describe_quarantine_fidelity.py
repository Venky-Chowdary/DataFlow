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


def test_hubspot_zendesk_unknown_meta_refuse_varchar_invent():
    """Unknown CRM Meta types must not soft-bind platform VARCHAR caps."""
    from connectors.saas_common import merge_saas_live_types

    assert hubspot_property_to_carrier({"type": "brand_new_hs_type_v99"}) == ""
    assert hubspot_property_to_carrier({"type": "json"}) == "JSON"
    assert zendesk_field_to_carrier({"type": "brand_new_zd_type_v99"}) == ""
    assert zendesk_field_to_carrier({"type": "lookup"}) == "VARCHAR(64)"

    live, err = merge_saas_live_types(
        {
            "email": hubspot_property_to_carrier(
                {"type": "string", "fieldType": "text"}
            ),
            "mystery": hubspot_property_to_carrier({"type": "brand_new_hs_type_v99"}),
        },
        ["email", "mystery"],
        studio_types=None,
        product="HubSpot",
    )
    assert err is not None
    assert "mystery" in err
    live2, err2 = merge_saas_live_types(
        {
            "email": hubspot_property_to_carrier(
                {"type": "string", "fieldType": "text"}
            ),
        },
        ["email", "mystery"],
        studio_types={"mystery": "INTEGER"},
        product="HubSpot",
    )
    assert err2 is None
    assert live2["mystery"] == "INTEGER"


def test_resolve_hubspot_does_not_map_invent_unknown_describe():
    types = resolve_hubspot_dest_types(
        ["email", "mystery"],
        [
            {"source": "e", "target": "email", "target_type": "VARCHAR"},
            {"source": "m", "target": "mystery", "target_type": "VARCHAR"},
        ],
        {},
        describe_props=[
            {"name": "email", "type": "string", "fieldType": "text"},
            {"name": "mystery", "type": "brand_new_hs_type_v99"},
        ],
    )
    assert types["email"].startswith("VARCHAR")
    assert "mystery" not in types


def test_resolve_airtable_notion_sf_no_map_invent_on_describe():
    from connectors.airtable_writer import resolve_airtable_dest_types
    from connectors.notion_writer import resolve_notion_dest_types
    from connectors.salesforce_writer import resolve_salesforce_dest_types

    at = resolve_airtable_dest_types(
        ["Name", "Score"],
        [
            {"source": "n", "target": "Name", "target_type": "VARCHAR"},
            {"source": "s", "target": "Score", "target_type": "VARCHAR"},
        ],
        {},
        meta_fields=[
            {"name": "Name", "type": "singleLineText"},
            {"name": "Score", "type": "formula"},
        ],
    )
    assert at["Name"].startswith("VARCHAR")
    assert "Score" not in at

    no = resolve_notion_dest_types(
        ["Name", "Computed"],
        [
            {"source": "n", "target": "Name", "target_type": "VARCHAR"},
            {"source": "c", "target": "Computed", "target_type": "VARCHAR"},
        ],
        {},
        properties={"Name": "title", "Computed": "formula"},
    )
    assert no["Name"].startswith("VARCHAR")
    assert "Computed" not in no

    sf = resolve_salesforce_dest_types(
        ["Name", "Weird__c"],
        [
            {"source": "n", "target": "Name", "target_type": "VARCHAR"},
            {"source": "w", "target": "Weird__c", "target_type": "VARCHAR"},
        ],
        {},
        describe_fields=[
            {"name": "Name", "type": "string", "length": 80},
            {"name": "Weird__c", "type": "brand_new_soap_type"},
        ],
    )
    assert sf["Name"] == "VARCHAR(80)"
    assert "Weird__c" not in sf


def test_resolve_studio_fills_refused_meta_carrier():
    from connectors.airtable_writer import resolve_airtable_dest_types
    from connectors.notion_writer import resolve_notion_dest_types

    at = resolve_airtable_dest_types(
        ["Name", "Score"],
        [],
        {},
        meta_fields=[
            {"name": "Name", "type": "singleLineText"},
            {"name": "Score", "type": "formula"},
        ],
        studio_types={"Score": "INTEGER"},
    )
    assert at["Score"] == "INTEGER"

    no = resolve_notion_dest_types(
        ["id", "Name"],
        [],
        {},
        properties={"id": "rich_text", "Name": "title"},
    )
    # Live property named id keeps Describe carrier (not page-UUID invent).
    assert no["id"] != "VARCHAR(64)"
    assert no["id"].startswith("VARCHAR")
    assert no["Name"].startswith("VARCHAR")


def test_resolve_partial_studio_without_describe_no_map_invent():
    """Describe unavailable + partial Studio must not Map-fill gaps."""
    from connectors.airtable_writer import resolve_airtable_dest_types
    from connectors.salesforce_writer import resolve_salesforce_dest_types

    hs = resolve_hubspot_dest_types(
        ["email", "mystery"],
        [
            {"source": "e", "target": "email", "target_type": "VARCHAR"},
            {"source": "m", "target": "mystery", "target_type": "VARCHAR"},
        ],
        {},
        describe_props=None,
        studio_types={"email": "VARCHAR(65536)"},
    )
    assert hs["email"].startswith("VARCHAR")
    assert "mystery" not in hs

    at = resolve_airtable_dest_types(
        ["Name", "Score"],
        [
            {"source": "n", "target": "Name", "target_type": "VARCHAR"},
            {"source": "s", "target": "Score", "target_type": "VARCHAR"},
        ],
        {},
        meta_fields=None,
        studio_types={"Name": "VARCHAR(100000)"},
    )
    assert "Name" in at
    assert "Score" not in at

    sf = resolve_salesforce_dest_types(
        ["Name", "Weird__c"],
        [
            {"source": "n", "target": "Name", "target_type": "VARCHAR"},
            {"source": "w", "target": "Weird__c", "target_type": "VARCHAR"},
        ],
        {},
        describe_fields=None,
        studio_types={"Name": "VARCHAR(80)"},
    )
    assert sf["Name"] == "VARCHAR(80)"
    assert "Weird__c" not in sf

    zd = resolve_zendesk_dest_types(
        ["subject", "custom_gap"],
        [
            {"source": "s", "target": "subject", "target_type": "VARCHAR"},
            {"source": "c", "target": "custom_gap", "target_type": "VARCHAR"},
        ],
        {},
        describe_fields=None,
        studio_types={"subject": "VARCHAR(255)"},
    )
    # System seed covers subject even when Studio stamp differs; gap stays refuse.
    assert "subject" in zd
    assert zd["subject"].startswith("VARCHAR")
    assert "custom_gap" not in zd

    zd_seed_only = resolve_zendesk_dest_types(
        ["subject", "custom_gap"],
        [
            {"source": "s", "target": "subject", "target_type": "VARCHAR"},
            {"source": "c", "target": "custom_gap", "target_type": "VARCHAR"},
        ],
        {},
        describe_fields=None,
        studio_types={"custom_gap": "INTEGER"},
    )
    assert "subject" in zd_seed_only
    assert zd_seed_only["custom_gap"] == "INTEGER"

    # No Describe + no Studio: system seeds still bind; custom stays refuse.
    zd_seeds_only = resolve_zendesk_dest_types(
        ["subject", "custom_gap"],
        [
            {"source": "s", "target": "subject", "target_type": "VARCHAR"},
            {"source": "c", "target": "custom_gap", "target_type": "VARCHAR"},
        ],
        {},
        describe_fields=None,
        studio_types=None,
    )
    assert "subject" in zd_seeds_only
    assert "custom_gap" not in zd_seeds_only


def test_overlay_promotes_mysql_enum_set_over_map_varchar():
    from connectors.writer_common import overlay_physical_bind_types

    overlaid = overlay_physical_bind_types(
        ["status", "flags", "note"],
        ["VARCHAR", "VARCHAR", "VARCHAR"],
        {
            "status": "enum('open','closed')",
            "flags": "set('a','b','c')",
            "note": "varchar(40)",
        },
    )
    assert overlaid[0].lower().startswith("enum(")
    assert overlaid[1].lower().startswith("set(")
    # Bounded physical VARCHAR(n) beats Map bare VARCHAR.
    assert overlaid[2].lower() == "varchar(40)"


def test_overlay_promotes_bounded_varchar_over_map_soft_string():
    from connectors.writer_common import overlay_physical_bind_types

    overlaid = overlay_physical_bind_types(
        ["a", "b", "c", "d"],
        ["VARCHAR", "TEXT", "VARCHAR(500)", "INTEGER"],
        {
            "a": "VARCHAR(40)",
            "b": "NVARCHAR(20)",
            "c": "CHAR(8)",
            "d": "VARCHAR(40)",
        },
    )
    assert overlaid[0] == "VARCHAR(40)"
    assert overlaid[1] == "NVARCHAR(20)"
    assert overlaid[2] == "CHAR(8)"
    # Non-string Map stamp must not be rewritten to VARCHAR.
    assert overlaid[3] == "INTEGER"
