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


def test_hubspot_empty_describe_refuses_map_only():
    from connectors.hubspot_writer import write_mapped_rows

    with patch("connectors.hubspot.describe_properties", return_value=[]):
        result = write_mapped_rows(
            host="api.hubapi.com",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="contacts",
            headers=["email"],
            data_rows=[["a@b.com"]],
            mappings=[
                {"source": "email", "target": "email", "target_type": "VARCHAR"}
            ],
            column_types={"email": "VARCHAR"},
            api_key="pat-xxx",
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "no properties" in (result.error or "").lower()


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
