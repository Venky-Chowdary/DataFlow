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


def test_merge_saas_live_types_partial_describe_refuses_missing():
    from connectors.saas_common import merge_saas_live_types

    merged, err = merge_saas_live_types(
        {"Name": "VARCHAR(80)"},
        ["Name", "Amount"],
        studio_types=None,
        product="Salesforce",
    )
    assert err is not None
    assert "Amount" in err
    assert "Name" in merged
    assert "Amount" not in merged


def test_crm_unknown_meta_types_refuse_varchar_invent():
    """Airtable/Notion/Salesforce unknown Meta types must not soft-bind VARCHAR."""
    from connectors.airtable_writer import airtable_field_to_carrier
    from connectors.notion_writer import notion_property_to_carrier
    from connectors.salesforce_writer import salesforce_field_to_carrier
    from connectors.saas_common import merge_saas_live_types

    assert airtable_field_to_carrier({"type": "formula"}) == ""
    assert airtable_field_to_carrier({"type": "brandNewAirtableType"}) == ""
    assert airtable_field_to_carrier({"type": "multipleAttachments"}) == "JSON"
    assert notion_property_to_carrier("formula") == ""
    assert notion_property_to_carrier("people") == "JSON"
    assert salesforce_field_to_carrier({"type": "anytype"}) == "JSON"
    assert salesforce_field_to_carrier({"type": "brand_new_soap_type"}) == ""

    live, err = merge_saas_live_types(
        {
            "Name": airtable_field_to_carrier({"type": "singleLineText"}),
            "Score": airtable_field_to_carrier({"type": "formula"}),
        },
        ["Name", "Score"],
        studio_types=None,
        product="Airtable",
    )
    assert err is not None
    assert "Score" in err
    live2, err2 = merge_saas_live_types(
        {"Name": airtable_field_to_carrier({"type": "singleLineText"})},
        ["Name", "Score"],
        studio_types={"Score": "INTEGER"},
        product="Airtable",
    )
    assert err2 is None
    assert live2["Score"] == "INTEGER"


def test_bigquery_repeated_physical_carrier_and_overlay():
    from types import SimpleNamespace

    from connectors.bigquery_writer import (
        _bigquery_physical_field_carrier,
        resolve_bigquery_decimal_target_types,
    )
    from connectors.writer_common import overlay_physical_bind_types

    tags = SimpleNamespace(
        name="tags", field_type="STRING", mode="REPEATED", max_length=None
    )
    nums = SimpleNamespace(
        name="nums",
        field_type="INTEGER",
        mode="REPEATED",
        precision=None,
        scale=None,
    )
    assert _bigquery_physical_field_carrier(tags) == "ARRAY<STRING>"
    assert _bigquery_physical_field_carrier(nums) == "ARRAY<INTEGER>"
    assert _bigquery_physical_field_carrier(
        SimpleNamespace(name="x", field_type="", mode="NULLABLE")
    ) == ""

    types = resolve_bigquery_decimal_target_types(
        ["tags", "nums"],
        ["VARCHAR", "VARCHAR"],
        [tags, nums],
    )
    assert types == ["ARRAY<STRING>", "ARRAY<INTEGER>"]

    overlaid = overlay_physical_bind_types(
        ["tags"],
        ["VARCHAR"],
        {"tags": "ARRAY<STRING>"},
    )
    assert overlaid == ["ARRAY<STRING>"]


def test_bigquery_records_use_array_carrier_not_map_scalar():
    """REPEATED live carriers must reach records_for_bigquery as ARRAY<T>."""
    from connectors.warehouse_temporal import (
        bigquery_repeated_element,
        records_for_bigquery,
    )

    assert bigquery_repeated_element("ARRAY<STRING>") == "STRING"
    assert bigquery_repeated_element("STRING") is None
    rec = records_for_bigquery(
        [(["a", "b"],)],
        ["tags"],
        ["ARRAY<STRING>"],
    )[0]
    assert isinstance(rec["tags"], list)
    assert rec["tags"] == ["a", "b"]


def test_pg_fetch_array_udt_not_scalar_int():
    """``_INT4`` information_schema udt must become INTEGER[] not INTEGER."""
    from connectors.postgresql_writer import _fetch_pg_column_types
    from connectors.writer_common import overlay_physical_bind_types

    class _Cur:
        def execute(self, *_a, **_k):
            return None

        def fetchall(self):
            return [
                ("ids", "ARRAY", "_int4", None, None, None),
                ("label", "character varying", "varchar", 40, None, None),
            ]

    physical = _fetch_pg_column_types(_Cur(), "public", "t")
    assert physical["ids"] == "INTEGER[]"
    assert physical["label"] == "VARCHAR(40)"
    overlaid = overlay_physical_bind_types(
        ["ids", "label"],
        ["VARCHAR", "TEXT"],
        physical,
    )
    assert overlaid[0] == "INTEGER[]"
    # Bounded physical VARCHAR(n) beats Map TEXT soft invent.
    assert overlaid[1] == "VARCHAR(40)"


def test_merge_saas_live_types_studio_fills_partial_gap():
    from connectors.saas_common import merge_saas_live_types

    merged, err = merge_saas_live_types(
        {"Name": "VARCHAR(80)"},
        ["Name", "Amount"],
        studio_types={"Amount": "DECIMAL(18,2)"},
        product="Salesforce",
    )
    assert err is None
    assert merged["Name"] == "VARCHAR(80)"
    assert merged["Amount"] == "DECIMAL(18,2)"


def test_hubspot_partial_describe_refuses_unmapped_invent():
    from connectors.hubspot_writer import write_mapped_rows

    with patch(
        "connectors.hubspot.describe_properties",
        return_value=[{"name": "email", "type": "string", "fieldType": "text"}],
    ):
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
            headers=["email", "custom_prop"],
            data_rows=[["a@b.com", "x"]],
            mappings=[
                {"source": "email", "target": "email", "target_type": "VARCHAR"},
                {
                    "source": "custom_prop",
                    "target": "custom_prop",
                    "target_type": "VARCHAR",
                },
            ],
            column_types={"email": "VARCHAR", "custom_prop": "VARCHAR"},
            api_key="pat-xxx",
            error_policy="quarantine",
            write_mode="insert",
        )
    assert result.ok is False
    assert "custom_prop" in (result.error or "")
    assert "refuse" in (result.error or "").lower()


def test_hubspot_partial_describe_studio_gap_allows():
    from connectors.hubspot_writer import write_mapped_rows

    with (
        patch(
            "connectors.hubspot.describe_properties",
            return_value=[{"name": "email", "type": "string", "fieldType": "text"}],
        ),
        patch(
            "connectors.hubspot_writer.request",
            side_effect=RuntimeError("stop-after-bind"),
        ),
    ):
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
            headers=["email", "custom_prop"],
            data_rows=[["a@b.com", "x"]],
            mappings=[
                {"source": "email", "target": "email", "target_type": "VARCHAR"},
                {
                    "source": "custom_prop",
                    "target": "custom_prop",
                    "target_type": "VARCHAR",
                },
            ],
            column_types={"email": "VARCHAR", "custom_prop": "VARCHAR"},
            api_key="pat-xxx",
            error_policy="quarantine",
            write_mode="insert",
            destination_column_types={
                "email": "VARCHAR(65536)",
                "custom_prop": "VARCHAR(256)",
            },
        )
    # Coverage gate passed; write fails later on mocked HTTP — not invent refuse.
    assert "refuse Map VARCHAR invent" not in (result.error or "")
    assert "missing mapped field" not in (result.error or "").lower()


def test_airtable_partial_meta_refuses_missing_field():
    from connectors.airtable_writer import write_mapped_rows

    with patch(
        "connectors.airtable_writer._fetch_table_fields",
        return_value=([{"name": "Name", "type": "singleLineText"}], None),
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
            headers=["Name", "Email"],
            data_rows=[["Ada", "a@b.com"]],
            mappings=[
                {"source": "Name", "target": "Name", "target_type": "VARCHAR"},
                {"source": "Email", "target": "Email", "target_type": "VARCHAR"},
            ],
            column_types={"Name": "VARCHAR", "Email": "VARCHAR"},
            api_key="patXXX",
            error_policy="quarantine",
        )
    assert result.ok is False
    assert "Email" in (result.error or "")
    assert "refuse" in (result.error or "").lower()


def test_salesforce_partial_describe_refuses_missing_field():
    from connectors.salesforce_writer import write_mapped_rows

    with patch(
        "connectors.salesforce.describe_sobject",
        return_value=[
            {
                "name": "Name",
                "type": "string",
                "length": 80,
                "createable": True,
                "updateable": True,
            }
        ],
    ):
        result = write_mapped_rows(
            host="https://example.my.salesforce.com",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="Account",
            headers=["Name", "Amount"],
            data_rows=[["Acme", "10"]],
            mappings=[
                {"source": "Name", "target": "Name", "target_type": "VARCHAR"},
                {"source": "Amount", "target": "Amount", "target_type": "VARCHAR"},
            ],
            column_types={"Name": "VARCHAR", "Amount": "VARCHAR"},
            api_key="sess",
            error_policy="quarantine",
            write_mode="insert",
        )
    assert result.ok is False
    assert "Amount" in (result.error or "")
    assert "refuse" in (result.error or "").lower()


def test_notion_partial_properties_refuse_missing():
    from connectors.notion_writer import write_mapped_rows

    with patch(
        "connectors.notion_writer._fetch_database_properties",
        return_value=({"Title": "title"}, {}),
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
            table_name="0123456789abcdef0123456789abcdef",
            headers=["Title", "Status"],
            data_rows=[["Hello", "Open"]],
            mappings=[
                {"source": "Title", "target": "Title", "target_type": "VARCHAR"},
                {"source": "Status", "target": "Status", "target_type": "VARCHAR"},
            ],
            column_types={"Title": "VARCHAR", "Status": "VARCHAR"},
            api_key="secret_xxx",
            error_policy="quarantine",
            write_mode="insert",
        )
    assert result.ok is False
    assert "Status" in (result.error or "")
    assert "refuse" in (result.error or "").lower()


def test_zendesk_studio_overrides_system_seed_carriers():
    """Describe-omitted subject must bind Studio length, not hardcoded seed."""
    from connectors.saas_common import merge_saas_live_types
    from connectors.zendesk_writer import _zendesk_system_seed_carriers

    seeds = _zendesk_system_seed_carriers(["subject", "custom_field"])
    assert "subject" in seeds
    fallback = dict(seeds)
    fallback["subject"] = "VARCHAR(120)"
    merged, err = merge_saas_live_types(
        {},
        ["subject"],
        studio_types=fallback,
        product="Zendesk",
    )
    assert err is None
    assert merged["subject"] == "VARCHAR(120)"
