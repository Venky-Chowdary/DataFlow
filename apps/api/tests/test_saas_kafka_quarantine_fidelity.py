"""Airtable / Notion / Kafka typed quarantine — no silent overflow on wire."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.airtable_writer import (  # noqa: E402
    airtable_field_to_carrier,
    resolve_airtable_dest_types,
)
from connectors.kafka_writer import _json_schema_property_for_logical  # noqa: E402
from connectors.notion_writer import (  # noqa: E402
    _rich_text_chunks,
    notion_property_to_carrier,
    resolve_notion_dest_types,
)
from connectors.writer_common import (  # noqa: E402
    apply_write_quarantine_matrix,
    resolve_mapping_dest_types,
)


def test_airtable_field_to_carrier_number_precision_and_text():
    assert airtable_field_to_carrier({"type": "singleLineText"}) == "VARCHAR(100000)"
    assert (
        airtable_field_to_carrier(
            {"type": "currency", "options": {"precision": 2, "symbol": "$"}}
        )
        == "DECIMAL(38,2)"
    )
    assert (
        airtable_field_to_carrier({"type": "number", "options": {"precision": 4}})
        == "DECIMAL(38,4)"
    )
    assert airtable_field_to_carrier({"type": "checkbox"}) == "BOOLEAN"
    assert airtable_field_to_carrier({"type": "dateTime"}) == "TIMESTAMPTZ"


def test_resolve_airtable_dest_types_prefers_meta():
    types = resolve_airtable_dest_types(
        ["Name", "Amount"],
        [
            {"source": "name", "target": "Name", "target_type": "VARCHAR(255)"},
            {"source": "amt", "target": "Amount", "target_type": "DECIMAL"},
        ],
        {"name": "TEXT", "amt": "DECIMAL(20,6)"},
        meta_fields=[
            {"name": "Name", "type": "singleLineText"},
            {
                "name": "Amount",
                "type": "currency",
                "options": {"precision": 2, "symbol": "$"},
            },
        ],
    )
    assert types["Name"] == "VARCHAR(100000)"
    assert types["Amount"] == "DECIMAL(38,2)"


def test_airtable_quarantine_holds_oversized_and_bad_decimal():
    details: list[dict] = []
    out = apply_write_quarantine_matrix(
        [
            ("A" * 100_001, "12.345"),
            ("ok", "not-a-number"),
            ("short", "10.00"),
        ],
        ["Name", "Amount"],
        ["VARCHAR(100000)", "DECIMAL(38,2)"],
        details,
        policy="quarantine",
        dialect_label="Airtable",
    )
    assert out == [("short", "10.00")]
    reasons = " ".join(d.get("reason", "") for d in details)
    assert "Airtable" in reasons


def test_notion_property_to_carrier_request_limits():
    assert notion_property_to_carrier("rich_text") == "VARCHAR(200000)"
    assert notion_property_to_carrier("email") == "VARCHAR(200)"
    assert notion_property_to_carrier("url") == "VARCHAR(2000)"
    assert notion_property_to_carrier("phone_number") == "VARCHAR(200)"
    assert notion_property_to_carrier("number") == "FLOAT"
    assert notion_property_to_carrier("checkbox") == "BOOLEAN"
    assert notion_property_to_carrier("date") == "TIMESTAMPTZ"


def test_resolve_notion_dest_types_prefers_live_properties():
    types = resolve_notion_dest_types(
        ["Notes", "Email"],
        [
            {"source": "n", "target": "Notes", "target_type": "TEXT"},
            {"source": "e", "target": "Email", "target_type": "VARCHAR(500)"},
        ],
        {"n": "TEXT", "e": "VARCHAR(500)"},
        properties={"notes": "rich_text", "email": "email"},
    )
    assert types["Notes"] == "VARCHAR(200000)"
    assert types["Email"] == "VARCHAR(200)"


def test_notion_rich_text_chunks_respect_2000_char_elements():
    text = "x" * 4500
    chunks = _rich_text_chunks(text)
    assert len(chunks) == 3
    assert all(len(c["text"]["content"]) <= 2000 for c in chunks)
    assert "".join(c["text"]["content"] for c in chunks) == text


def test_notion_quarantine_holds_email_and_total_rich_text_overflow():
    details: list[dict] = []
    out = apply_write_quarantine_matrix(
        [
            ("a" * 201, "ok"),
            ("good@example.com", "y" * 200_001),
            ("good@example.com", "short"),
        ],
        ["Email", "Body"],
        ["VARCHAR(200)", "VARCHAR(200000)"],
        details,
        policy="quarantine",
        dialect_label="Notion",
    )
    assert out == [("good@example.com", "short")]
    assert any("Notion" in d.get("reason", "") for d in details)


def test_kafka_json_schema_decimal_is_string_not_ieee():
    prop = _json_schema_property_for_logical("DECIMAL(18,2)")
    assert prop.get("type") == ["string", "null"]
    assert "decimal" in str(prop.get("contentMediaType", "")).lower()


def test_resolve_mapping_dest_types_prefers_live_then_map():
    types = resolve_mapping_dest_types(
        ["a", "b"],
        [
            {"source": "sa", "target": "a", "target_type": "VARCHAR(10)"},
            {"source": "sb", "target": "b", "target_type": "BOOLEAN"},
        ],
        {"sa": "TEXT", "sb": "BOOLEAN"},
        logical_types=["VARCHAR(10)", "BOOLEAN"],
        live_types={"a": "VARCHAR(80)"},
        default="VARCHAR",
    )
    assert types["a"] == "VARCHAR(80)"
    assert types["b"] == "BOOLEAN"


def test_crm_writers_quarantine_dialect_labels_surface():
    """Zendesk/Stripe/Shopify share Map-typed quarantine (same SSOT matrix)."""
    for label in ("Zendesk", "Stripe", "Shopify", "HubSpot"):
        details: list[dict] = []
        out = apply_write_quarantine_matrix(
            [("toolong",), ("ok",)],
            ["code"],
            ["VARCHAR(2)"],
            details,
            policy="quarantine",
            dialect_label=label,
        )
        assert out == [("ok",)]
        assert any(label in d.get("reason", "") for d in details)


def test_kafka_quarantine_holds_unfit_boolean_and_varchar():
    details: list[dict] = []
    out = apply_write_quarantine_matrix(
        [
            ("maybe", "toolong"),
            ("true", "ok"),
            ("true", "toolong"),
        ],
        ["active", "code"],
        ["BOOLEAN", "VARCHAR(2)"],
        details,
        policy="quarantine",
        dialect_label="Kafka",
    )
    assert out == [("true", "ok")]
    reasons = " ".join(d.get("reason", "") for d in details)
    assert "non-canonical boolean" in reasons.lower()
    assert "Kafka VARCHAR" in reasons
