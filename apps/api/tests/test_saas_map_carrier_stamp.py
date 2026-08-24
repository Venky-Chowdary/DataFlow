"""Map introspect stamps writer SSOT carriers (VARCHAR(n) / DECIMAL) — not bare TEXT."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.schema_introspect import (  # noqa: E402
    _introspect_hubspot,
    _introspect_salesforce,
    _introspect_shopify,
    _introspect_thin_saas,
    _introspect_zendesk,
    _stamp_thin_saas_write_carriers,
)


def test_salesforce_introspect_stamps_varchar_length():
    fields = [
        {"name": "Name", "type": "string", "length": 80, "nillable": False, "label": "Name"},
        {
            "name": "Amount__c",
            "type": "currency",
            "precision": 16,
            "scale": 2,
            "nillable": True,
            "label": "Amount",
        },
    ]
    with (
        patch("connectors.salesforce.list_sobjects", return_value=["Account"]),
        patch("connectors.salesforce.describe_sobject", return_value=fields),
    ):
        out = _introspect_salesforce(table="Account", api_key="x")
    by_name = {c["name"]: c["inferred_type"] for c in out["columns"]}
    assert by_name["Name"] == "VARCHAR(80)"
    assert by_name["Amount__c"] == "DECIMAL(16,2)"


def test_hubspot_introspect_stamps_string_and_currency():
    props = [
        {
            "name": "email",
            "type": "string",
            "fieldType": "text",
            "validationRules": [{"ruleType": "MAX_LENGTH", "ruleArguments": ["254"]}],
        },
        {
            "name": "amount",
            "type": "number",
            "fieldType": "number",
            "numberDisplayHint": "currency",
        },
    ]
    with (
        patch("connectors.hubspot.list_object_types", return_value=["contacts"]),
        patch("connectors.hubspot.describe_properties", return_value=props),
    ):
        out = _introspect_hubspot(table="contacts", api_key="x")
    by_name = {c["name"]: c["inferred_type"] for c in out["columns"]}
    assert by_name["email"] == "VARCHAR(254)"
    assert by_name["amount"] == "DECIMAL(38,2)"


def test_stripe_stamp_overlay_uses_documented_limits():
    cols = [
        {"name": "email", "inferred_type": "TEXT", "nullable": True},
        {"name": "phone", "inferred_type": "TEXT", "nullable": True},
        {"name": "name", "inferred_type": "TEXT", "nullable": True},
    ]
    stamped = _stamp_thin_saas_write_carriers("stripe", "customers", cols, {})
    by_name = {c["name"]: c["inferred_type"] for c in stamped}
    assert by_name["email"] == "VARCHAR(512)"
    assert by_name["phone"] == "VARCHAR(20)"
    assert by_name["name"] == "VARCHAR(256)"


def test_shopify_introspect_stamps_core_note_and_email():
    with patch(
        "connectors.shopify.describe_metafield_definitions",
        return_value=[],
    ):
        out = _introspect_shopify(table="customers", host="x.myshopify.com", api_key="t")
    by_name = {c["name"]: c["inferred_type"] for c in out["columns"]}
    assert by_name["note"] == "VARCHAR(5000)"
    assert by_name["email"] == "VARCHAR(255)"


def test_zendesk_introspect_fail_closed_when_describe_empty():
    with patch("connectors.zendesk.describe_fields", return_value=[]):
        out = _introspect_zendesk(table="tickets", host="x.zendesk.com", api_key="t")
    assert out.get("ok") is False
    assert not out.get("columns")
    assert "refuse" in str(out.get("error") or "").lower() or "no fields" in str(
        out.get("error") or ""
    ).lower()


def test_thin_saas_stripe_path_stamps_after_sample_read():
    class _Batch:
        headers = ["email", "phone"]
        rows = [["a@b.com", "1"]]
        meta = {"native_types": {"email": "string", "phone": "string"}, "saas_typed": True}

    with patch("connectors.stripe.read_object", return_value=_Batch()):
        out = _introspect_thin_saas(
            "stripe", table="customers", api_key="sk_test", database="customers"
        )
    by_name = {c["name"]: c["inferred_type"] for c in out["columns"]}
    assert by_name["email"] == "VARCHAR(512)"
    assert by_name["phone"] == "VARCHAR(20)"
