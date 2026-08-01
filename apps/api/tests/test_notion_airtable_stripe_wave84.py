"""Wave 84: Notion/Airtable select domains + Stripe closed enums + reverse-ETL plans.

Research anchors
----------------
- Notion property schema: select/status/multi_select ``options[].name`` —
  writes reference names; unknown names must quarantine (not invent schema).
- Airtable Meta: singleSelect/multipleSelects ``options.choices[].name``.
- Stripe Customer.tax_exempt ∈ {none, exempt, reverse}; Price billing_scheme /
  tax_behavior; Coupon duration — official API enums.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_notion_select_status_multi_select_domains():
    from connectors.notion_writer import (
        _notion_option_names,
        notion_property_to_carrier,
    )

    assert notion_property_to_carrier("select") == "VARCHAR(100)"
    select = notion_property_to_carrier(
        "select", option_names=["To do", "Doing", "Done"]
    )
    assert select == "ENUM('To do','Doing','Done')"

    status = notion_property_to_carrier(
        "status", option_names=["Not started", "In progress", "Done"]
    )
    assert "Not started" in status and status.startswith("ENUM(")

    multi = notion_property_to_carrier(
        "multi_select", option_names=["A", "B"]
    )
    assert multi == "SET('A','B')"

    names = _notion_option_names(
        {
            "type": "select",
            "select": {
                "options": [
                    {"name": "Red", "color": "red"},
                    {"name": "Blue,bad", "color": "blue"},  # commas invalid
                    {"name": "Green"},
                ]
            },
        }
    )
    assert names == ["Red", "Green"]


def test_airtable_single_and_multiple_select_domains():
    from connectors.airtable_writer import airtable_field_to_carrier

    single = airtable_field_to_carrier(
        {
            "type": "singleSelect",
            "options": {
                "choices": [
                    {"name": "Todo"},
                    {"name": "Done"},
                ]
            },
        }
    )
    assert single == "ENUM('Todo','Done')"

    multi = airtable_field_to_carrier(
        {
            "type": "multipleSelects",
            "options": {"choices": [{"name": "a"}, {"name": "b"}]},
        }
    )
    assert multi.startswith("SET(")
    assert "a" in multi and "b" in multi


def test_stripe_closed_enums():
    from connectors.saas_write_carriers import stripe_field_carriers

    cust = stripe_field_carriers("customers")
    assert cust["tax_exempt"] == "ENUM('none','exempt','reverse')"

    prices = stripe_field_carriers("prices")
    assert prices["billing_scheme"].startswith("ENUM(")
    assert "per_unit" in prices["billing_scheme"]
    assert prices["tax_behavior"].startswith("ENUM(")

    coupons = stripe_field_carriers("coupons")
    assert coupons["duration"] == "ENUM('forever','once','repeating')"


def test_reverse_etl_notion_airtable_stripe_plans():
    from services.reverse_etl import plan_activation, supported_activation_kinds

    for kind in ("notion", "airtable", "stripe"):
        assert kind in supported_activation_kinds()

    assert plan_activation(
        destination_kind="notion", object_name="Tasks", primary_key="id"
    ).batch_size == 25
    assert plan_activation(
        destination_kind="airtable", object_name="Grid", primary_key="id"
    ).batch_size == 10
    assert any(
        "tax_exempt" in n.lower() or "enum" in n.lower()
        for n in plan_activation(
            destination_kind="stripe",
            object_name="customers",
            primary_key="id",
        ).notes
    )
