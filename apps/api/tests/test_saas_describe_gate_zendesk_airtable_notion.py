"""Zendesk/Airtable/Notion/Shopify Describe fail-closed + shared SaaS gate."""

from __future__ import annotations

from unittest.mock import patch


def test_gate_saas_describe_auth_fail_closed():
    from connectors.saas_common import gate_saas_describe

    gate = gate_saas_describe(
        product="Zendesk",
        object_name="tickets",
        fields=None,
        exc=Exception("401 Unauthorized"),
        target_cols=["subject"],
        studio_types={"subject": "VARCHAR"},
    )
    assert gate.ok is False
    assert "auth" in gate.error.lower()


def test_gate_saas_describe_empty_without_studio_refuses():
    from connectors.saas_common import gate_saas_describe

    gate = gate_saas_describe(
        product="Airtable",
        object_name="Contacts",
        fields=[],
        exc=None,
        target_cols=["Name"],
        studio_types=None,
    )
    assert gate.ok is False
    assert "no fields" in gate.error.lower()


def test_gate_saas_describe_empty_with_studio_allows_fallback():
    from connectors.saas_common import gate_saas_describe

    gate = gate_saas_describe(
        product="Airtable",
        object_name="Contacts",
        fields=[],
        exc=None,
        target_cols=["Name"],
        studio_types={"Name": "VARCHAR(128)"},
    )
    assert gate.ok is True
    assert gate.fields is None
    assert "Studio" in gate.warning


def test_zendesk_describe_failure_refuses_map_only():
    from connectors.zendesk_writer import write_mapped_rows

    with patch(
        "connectors.zendesk.describe_fields",
        side_effect=RuntimeError("timeout"),
    ):
        result = write_mapped_rows(
            host="https://acme.zendesk.com",
            table_name="tickets",
            api_key="tok",
            headers=["subject"],
            data_rows=[["help"]],
            mappings=[
                {"source": "subject", "target": "subject", "target_type": "VARCHAR"}
            ],
            column_types={"subject": "VARCHAR"},
            write_mode="insert",
            port=443,
            database="",
            username="user@ex.com",
            password="",
            schema="",
            connection_string="",
            ssl=True,
        )
    assert result.ok is False
    assert "describe unavailable" in (result.error or "").lower() or "refuse" in (
        result.error or ""
    ).lower()


def test_zendesk_empty_describe_refuses_map_only():
    from connectors.zendesk_writer import write_mapped_rows

    with patch("connectors.zendesk.describe_fields", return_value=[]):
        result = write_mapped_rows(
            host="https://acme.zendesk.com",
            table_name="tickets",
            api_key="tok",
            headers=["subject"],
            data_rows=[["help"]],
            mappings=[
                {"source": "subject", "target": "subject", "target_type": "VARCHAR"}
            ],
            column_types={"subject": "VARCHAR"},
            write_mode="insert",
            port=443,
            database="",
            username="user@ex.com",
            password="",
            schema="",
            connection_string="",
            ssl=True,
        )
    assert result.ok is False
    assert "no fields" in (result.error or "").lower()


def test_airtable_describe_auth_fail_closed():
    from connectors.airtable_writer import write_mapped_rows

    with patch(
        "connectors.airtable_writer._fetch_table_fields",
        return_value=(None, Exception("401 Unauthorized")),
    ):
        result = write_mapped_rows(
            host="api.airtable.com",
            port=443,
            database="appXXX",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="Contacts",
            headers=["Name"],
            data_rows=[["Ada"]],
            mappings=[{"source": "Name", "target": "Name", "target_type": "VARCHAR"}],
            column_types={"Name": "VARCHAR"},
            api_key="patXXX",
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "auth" in (result.error or "").lower()


def test_airtable_empty_meta_refuses_map_only():
    from connectors.airtable_writer import write_mapped_rows

    with patch(
        "connectors.airtable_writer._fetch_table_fields",
        return_value=([], None),
    ):
        result = write_mapped_rows(
            host="api.airtable.com",
            port=443,
            database="appXXX",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="Contacts",
            headers=["Name"],
            data_rows=[["Ada"]],
            mappings=[{"source": "Name", "target": "Name", "target_type": "VARCHAR"}],
            column_types={"Name": "VARCHAR"},
            api_key="patXXX",
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "no fields" in (result.error or "").lower()


def test_notion_empty_properties_refuses_map_only():
    from connectors.notion_writer import write_mapped_rows

    with patch(
        "connectors.notion_writer._fetch_database_properties",
        return_value=({}, {}),
    ):
        result = write_mapped_rows(
            host="api.notion.com",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="11111111-1111-1111-1111-111111111111",
            headers=["Name"],
            data_rows=[["Ada"]],
            mappings=[{"source": "Name", "target": "Name", "target_type": "VARCHAR"}],
            column_types={"Name": "VARCHAR"},
            api_key="secret_xxx",
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "no fields" in (result.error or "").lower() or "refuse" in (
        result.error or ""
    ).lower()


def test_notion_describe_failure_refuses_map_only():
    from connectors.notion_writer import write_mapped_rows

    with patch(
        "connectors.notion_writer._fetch_database_properties",
        side_effect=RuntimeError("timeout"),
    ):
        result = write_mapped_rows(
            host="api.notion.com",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="11111111-1111-1111-1111-111111111111",
            headers=["Name"],
            data_rows=[["Ada"]],
            mappings=[{"source": "Name", "target": "Name", "target_type": "VARCHAR"}],
            column_types={"Name": "VARCHAR"},
            api_key="secret_xxx",
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "unavailable" in (result.error or "").lower() or "refuse" in (
        result.error or ""
    ).lower()


def test_shopify_metafield_auth_fail_closed():
    from connectors.shopify_writer import write_mapped_rows

    with patch(
        "connectors.shopify.describe_metafield_definitions",
        side_effect=Exception("401 Unauthorized"),
    ):
        result = write_mapped_rows(
            host="https://acme.myshopify.com",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="customers",
            headers=["email"],
            data_rows=[["a@b.com"]],
            mappings=[
                {"source": "email", "target": "email", "target_type": "VARCHAR"}
            ],
            column_types={"email": "VARCHAR"},
            api_key="shpat_xxx",
            error_policy="quarantine",
            write_mode="insert",
        )
    assert result.ok is False
    assert "auth" in (result.error or "").lower()


def test_shopify_metafield_graphql_scope_fail_closed():
    from connectors.shopify_writer import write_mapped_rows

    with patch(
        "connectors.shopify.describe_metafield_definitions",
        side_effect=RuntimeError(
            "Shopify metafield Describe auth/scope failed: ACCESS_DENIED"
        ),
    ):
        result = write_mapped_rows(
            host="https://acme.myshopify.com",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="customers",
            headers=["email"],
            data_rows=[["a@b.com"]],
            mappings=[
                {"source": "email", "target": "email", "target_type": "VARCHAR"}
            ],
            column_types={"email": "VARCHAR"},
            api_key="shpat_xxx",
            error_policy="quarantine",
            write_mode="insert",
        )
    assert result.ok is False
    assert "auth" in (result.error or "").lower()


def test_shopify_describe_metafields_propagates_http_auth():
    """Probe must not swallow 401 into soft-empty (writer auth fail-closed)."""
    from connectors.shopify import describe_metafield_definitions

    with patch(
        "connectors.shopify.request",
        side_effect=Exception("401 Unauthorized"),
    ):
        try:
            describe_metafield_definitions(
                {
                    "host": "acme.myshopify.com",
                    "api_key": "shpat_xxx",
                    "table": "customers",
                },
                "customers",
            )
            raised = False
        except Exception as exc:
            raised = True
            assert "401" in str(exc) or "unauthorized" in str(exc).lower()
    assert raised is True


def test_notion_stamp_passes_select_option_names():
    from connectors.notion_writer import notion_property_to_carrier

    carrier = notion_property_to_carrier(
        "select", option_names=["Open", "Closed"]
    )
    assert "Open" in carrier and "Closed" in carrier
    assert notion_property_to_carrier("select").startswith("VARCHAR")
