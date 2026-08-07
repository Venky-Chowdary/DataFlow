"""Stripe catalog∩Studio gate + object-store Studio live type overlay."""

from __future__ import annotations

from unittest.mock import patch


def test_merge_stripe_catalog_refuses_uncatalogued_without_studio():
    from connectors.saas_write_carriers import merge_stripe_catalog_types

    live, err = merge_stripe_catalog_types(
        "customers",
        ["email", "invented_field"],
        studio_types=None,
    )
    assert err is not None
    assert "invented_field" in err
    assert "email" in live


def test_merge_stripe_catalog_allows_studio_typed_custom():
    from connectors.saas_write_carriers import merge_stripe_catalog_types

    live, err = merge_stripe_catalog_types(
        "customers",
        ["email", "custom_score"],
        studio_types={"custom_score": "INTEGER"},
    )
    assert err is None
    assert live["email"].startswith("VARCHAR")
    assert live["custom_score"] == "INTEGER"


def test_stripe_writer_refuses_uncatalogued_map_varchar():
    from connectors.stripe_writer import write_mapped_rows

    with patch("connectors.stripe_writer.request") as req:
        result = write_mapped_rows(
            host="api.stripe.com",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="customers",
            headers=["weird_col"],
            data_rows=[["x"]],
            mappings=[
                {
                    "source": "weird_col",
                    "target": "weird_col",
                    "target_type": "VARCHAR",
                }
            ],
            column_types={"weird_col": "VARCHAR"},
            api_key="sk_test_xxx",
            error_policy="quarantine",
            write_mode="insert",
        )
    assert result.ok is False
    assert "catalog" in (result.error or "").lower() or "refuse" in (
        result.error or ""
    ).lower()
    req.assert_not_called()


def test_stripe_writer_studio_typed_custom_proceeds_past_catalog_gate():
    from unittest.mock import MagicMock

    from connectors.stripe_writer import write_mapped_rows

    with patch("connectors.stripe_writer.request") as req:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"id": "cus_1"}
        mock_resp.raise_for_status = MagicMock()
        req.return_value = mock_resp
        result = write_mapped_rows(
            host="api.stripe.com",
            port=443,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=True,
            table_name="customers",
            headers=["email", "loyalty_tier"],
            data_rows=[["a@b.com", "gold"]],
            mappings=[
                {"source": "email", "target": "email", "target_type": "VARCHAR"},
                {
                    "source": "loyalty_tier",
                    "target": "loyalty_tier",
                    "target_type": "VARCHAR",
                },
            ],
            column_types={"email": "VARCHAR", "loyalty_tier": "VARCHAR"},
            destination_column_types={
                "email": "VARCHAR(512)",
                "loyalty_tier": "VARCHAR(64)",
            },
            api_key="sk_test_xxx",
            error_policy="quarantine",
            write_mode="insert",
        )
    assert result.ok is True
    assert result.rows_written == 1
    req.assert_called()


def test_object_store_dest_types_prefer_studio_decimal():
    from connectors.object_store_common import resolve_object_store_write_dest_types

    dest = resolve_object_store_write_dest_types(
        ["amount"],
        [{"source": "amount", "target": "amount", "target_type": "VARCHAR"}],
        {"amount": "VARCHAR"},
        logical_types=["VARCHAR"],
        destination_column_types={"amount": "DECIMAL(18,2)"},
    )
    assert "DECIMAL" in str(dest.get("amount") or "").upper()


def test_object_store_dest_types_fall_back_to_map_without_studio():
    from connectors.object_store_common import resolve_object_store_write_dest_types

    dest = resolve_object_store_write_dest_types(
        ["note"],
        [{"source": "note", "target": "note", "target_type": "VARCHAR(100)"}],
        {"note": "VARCHAR(100)"},
        logical_types=["VARCHAR(100)"],
        destination_column_types=None,
    )
    assert "VARCHAR" in str(dest.get("note") or "").upper()


def test_merge_shopify_catalog_refuses_uncatalogued_without_studio():
    from connectors.saas_write_carriers import merge_shopify_catalog_types

    _live, err = merge_shopify_catalog_types(
        "customers",
        ["email", "invented_metafield"],
        metafield_defs=[],
        studio_types=None,
    )
    assert err is not None
    assert "invented_metafield" in err


def test_merge_shopify_catalog_allows_studio_typed_custom():
    from connectors.saas_write_carriers import merge_shopify_catalog_types

    live, err = merge_shopify_catalog_types(
        "customers",
        ["email", "custom_score"],
        metafield_defs=[],
        studio_types={"custom_score": "INTEGER"},
    )
    assert err is None
    assert live["custom_score"] == "INTEGER"
