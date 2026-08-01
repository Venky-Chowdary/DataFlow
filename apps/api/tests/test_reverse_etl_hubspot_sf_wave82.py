"""Wave 82: Reverse-ETL HubSpot enumeration + Salesforce CASESAFEID fidelity.

Research anchors
----------------
- Hightouch / Census: HubSpot enumeration (select/radio/checkbox) only accepts
  *internal* ``options[].value`` — labels invent INVALID_OPTION.
- Checkbox multi-select is semicolon-delimited (HubSpot CRM Properties guide).
- Salesforce CASESAFEID: 15-char case-sensitive Id → 18-char checksum suffix
  (Data Loader / Informatica class reverse-ETL).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_hubspot_enumeration_enum_and_checkbox_set():
    from connectors.hubspot_writer import hubspot_property_to_carrier
    from connectors.sql_bind import coerce_set_wire

    select = hubspot_property_to_carrier(
        {
            "type": "enumeration",
            "fieldType": "select",
            "options": [
                {"label": "1 Star", "value": "bucket_1"},
                {"label": "2 Stars", "value": "bucket_2", "hidden": False},
                {"label": "Gone", "value": "gone", "hidden": True},
            ],
        }
    )
    assert select == "ENUM('bucket_1','bucket_2')"
    # Labels must not invent wire — domain is internal values only.
    assert "1 Star" not in select

    checkbox = hubspot_property_to_carrier(
        {
            "type": "enumeration",
            "fieldType": "checkbox",
            "options": [
                {"value": "DECISION_MAKER"},
                {"value": "BUDGET_HOLDER"},
            ],
        }
    )
    assert checkbox.startswith("SET(")
    assert "DECISION_MAKER" in checkbox
    # Semicolon joiner for HubSpot CRM batch upsert.
    wire = coerce_set_wire(
        "BUDGET_HOLDER;DECISION_MAKER", ddl_type=checkbox, joiner=";"
    )
    assert wire == "DECISION_MAKER;BUDGET_HOLDER"  # definition order


def test_salesforce_casesafeid_15_to_18():
    from connectors.salesforce_writer import coerce_salesforce_id_wire

    # StackExchange reference sample.
    assert coerce_salesforce_id_wire("001A000010khO8J") == "001A000010khO8JIAU"
    assert coerce_salesforce_id_wire("001A000010khO8JIAU") == "001A000010khO8JIAU"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_salesforce_id_wire("001A000010khO8JXXX")  # bad checksum
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_salesforce_id_wire("too-short")
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_salesforce_id_wire(12345)


def test_saas_set_joiner_accepts_comma_or_semicolon_input():
    from connectors.sql_bind import coerce_set_wire

    ddl = "SET('a','b','c')"
    assert coerce_set_wire("a;c", ddl_type=ddl, joiner=";") == "a;c"
    assert coerce_set_wire("a,c", ddl_type=ddl, joiner=";") == "a;c"
    assert coerce_set_wire("a;c", ddl_type=ddl, joiner=",") == "a,c"
    with pytest.raises(ValueError, match="refuse invent"):
        coerce_set_wire("a;z", ddl_type=ddl, joiner=";")


def test_reverse_etl_plans_keep_quarantine_notes():
    from services.reverse_etl import plan_activation

    sf = plan_activation(
        destination_kind="salesforce",
        object_name="Account",
        primary_key="External_Id__c",
    )
    assert sf.batch_size == 200
    assert any("quarantine" in n.lower() for n in sf.notes)

    hs = plan_activation(
        destination_kind="hubspot",
        object_name="contacts",
        primary_key="email",
    )
    assert hs.batch_size == 100
    assert any("quarantine" in n.lower() for n in hs.notes)
