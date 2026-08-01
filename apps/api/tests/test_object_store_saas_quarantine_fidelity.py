"""Object-store + Salesforce typed quarantine — no silent truncate/invent."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.salesforce_writer import (  # noqa: E402
    resolve_salesforce_dest_types,
    salesforce_field_to_carrier,
)
from connectors.writer_common import apply_write_quarantine_matrix  # noqa: E402


def test_salesforce_field_to_carrier_preserves_length_and_decimal():
    assert (
        salesforce_field_to_carrier({"type": "string", "length": 80}) == "VARCHAR(80)"
    )
    assert (
        salesforce_field_to_carrier(
            {"type": "currency", "precision": 18, "scale": 2}
        )
        == "DECIMAL(18,2)"
    )
    assert salesforce_field_to_carrier({"type": "boolean"}) == "BOOLEAN"
    assert salesforce_field_to_carrier({"type": "base64"}) == "BINARY"


def test_resolve_salesforce_dest_types_prefers_describe():
    types = resolve_salesforce_dest_types(
        ["Name", "Amount__c"],
        [
            {"source": "name", "target": "Name", "target_type": "VARCHAR(255)"},
            {"source": "amt", "target": "Amount__c", "target_type": "DECIMAL"},
        ],
        {"name": "TEXT", "amt": "DECIMAL(20,6)"},
        describe_fields=[
            {"name": "Name", "type": "string", "length": 80},
            {"name": "Amount__c", "type": "currency", "precision": 16, "scale": 2},
        ],
    )
    assert types["Name"] == "VARCHAR(80)"
    assert types["Amount__c"] == "DECIMAL(16,2)"


def test_object_store_quarantine_holds_oversized_string_and_bad_binary():
    details: list[dict] = []
    out = apply_write_quarantine_matrix(
        [
            ("toolongvalue", "AQID"),
            ("ok", "not-valid-base64!!!"),
            ("short", "AQID"),
        ],
        ["name", "blob"],
        ["VARCHAR(5)", "BINARY(16)"],
        details,
        policy="quarantine",
        dialect_label="S3",
    )
    assert out == [("short", "AQID")]
    reasons = " ".join(d.get("reason", "") for d in details)
    assert "S3 VARCHAR" in reasons or "exceeds" in reasons.lower()
    assert "base64" in reasons.lower() or "BINARY" in reasons


def test_salesforce_quarantine_holds_string_too_long():
    details: list[dict] = []
    out = apply_write_quarantine_matrix(
        [("A" * 100,), ("ok",)],
        ["Name"],
        ["VARCHAR(40)"],
        details,
        policy="quarantine",
        dialect_label="Salesforce",
    )
    assert out == [("ok",)]
    assert details and "Salesforce VARCHAR" in details[0]["reason"]
