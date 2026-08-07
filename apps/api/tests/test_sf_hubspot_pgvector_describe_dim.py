"""Salesforce/HubSpot Describe fail-closed + pgvector live vector(n) typmod."""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch


def test_salesforce_describe_failure_refuses_map_only():
    from connectors.salesforce_writer import write_mapped_rows

    with patch(
        "connectors.salesforce.describe_sobject",
        side_effect=RuntimeError("timeout"),
    ):
        result = write_mapped_rows(
            host="https://example.my.salesforce.com",
            table_name="Account",
            api_key="tok",
            headers=["Name"],
            data_rows=[["Acme"]],
            mappings=[{"source": "Name", "target": "Name", "target_type": "VARCHAR"}],
            column_types={"Name": "VARCHAR"},
            write_mode="insert",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
        )
    assert result.ok is False
    assert "describe unavailable" in (result.error or "").lower()


def test_salesforce_describe_auth_fail_closed():
    from connectors.salesforce_writer import write_mapped_rows

    with patch(
        "connectors.salesforce.describe_sobject",
        side_effect=Exception("401 Unauthorized"),
    ):
        result = write_mapped_rows(
            host="https://example.my.salesforce.com",
            table_name="Account",
            api_key="tok",
            headers=["Name"],
            data_rows=[["Acme"]],
            mappings=[{"source": "Name", "target": "Name", "target_type": "VARCHAR"}],
            column_types={"Name": "VARCHAR"},
            destination_column_types={"Name": "VARCHAR"},
            write_mode="insert",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
        )
    assert result.ok is False
    assert "auth" in (result.error or "").lower()


def test_salesforce_empty_describe_refuses_map_only():
    from connectors.salesforce_writer import write_mapped_rows

    with patch("connectors.salesforce.describe_sobject", return_value=[]):
        result = write_mapped_rows(
            host="https://example.my.salesforce.com",
            table_name="Account",
            api_key="tok",
            headers=["Name"],
            data_rows=[["Acme"]],
            mappings=[{"source": "Name", "target": "Name", "target_type": "VARCHAR"}],
            column_types={"Name": "VARCHAR"},
            write_mode="insert",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
        )
    assert result.ok is False
    assert "no fields" in (result.error or "").lower()


def test_hubspot_describe_properties_preserves_enumeration_options():
    """Properties API options must survive Describe → ENUM carrier (not VARCHAR)."""
    from connectors.hubspot import describe_properties
    from connectors.hubspot_writer import hubspot_property_to_carrier
    from unittest.mock import MagicMock, patch

    payload = {
        "results": [
            {
                "name": "lifecyclestage",
                "type": "enumeration",
                "fieldType": "select",
                "label": "Lifecycle Stage",
                "options": [
                    {"label": "Lead", "value": "lead", "hidden": False},
                    {"label": "Customer", "value": "customer", "hidden": False},
                ],
            }
        ]
    }
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()

    with patch("connectors.hubspot.request", return_value=resp):
        props = describe_properties(
            {"api_key": "pat-xxx", "table": "contacts"},
            "contacts",
        )
    assert len(props) == 1
    assert isinstance(props[0].get("options"), list)
    assert len(props[0]["options"]) == 2
    carrier = hubspot_property_to_carrier(props[0])
    assert carrier.startswith("ENUM(")
    assert "lead" in carrier and "customer" in carrier
    assert "VARCHAR" not in carrier


def test_pgvector_live_embedding_dim_parses_format_type():
    from connectors.pgvector_writer import _pgvector_live_embedding_dim

    cur = MagicMock()
    cur.fetchone.return_value = ("vector(768)",)
    assert _pgvector_live_embedding_dim(cur, "public", "chunks") == 768
    cur.fetchone.return_value = ("vector",)
    assert _pgvector_live_embedding_dim(cur, "public", "chunks") is None
    cur.fetchone.return_value = None
    assert _pgvector_live_embedding_dim(cur, "public", "chunks") is None


def test_pgvector_format_type_regex():
    assert re.search(r"vector\((\d+)\)", "vector(384)").group(1) == "384"


def test_sf_field_api_name_preserves_custom_suffix():
    from connectors.salesforce_writer import _sf_field_api_name

    assert _sf_field_api_name("External_Id__c") == "External_Id__c"
    assert _sf_field_api_name("Account__r") == "Account__r"
    assert "__c" in _sf_field_api_name("My_Field__c")
