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


def test_stripe_address_leaf_typed_not_soft_varchar():
    """Documented address leaves get typed carriers; unknown leaves refuse."""
    from connectors.saas_write_carriers import (
        address_leaf_carrier,
        merge_stripe_catalog_types,
        stripe_live_types_for_columns,
    )

    assert address_leaf_carrier("address.country") == "VARCHAR(2)"
    assert address_leaf_carrier("address_postal_code") == "VARCHAR(20)"
    assert address_leaf_carrier("address.city") == "VARCHAR(255)"
    assert address_leaf_carrier("address.line1") == "VARCHAR(255)"
    assert address_leaf_carrier("address.line2") == "VARCHAR(255)"
    assert address_leaf_carrier("billing_details.address.line1") == "VARCHAR(255)"
    assert address_leaf_carrier("address.invented_leaf") is None

    live = stripe_live_types_for_columns(
        "customers",
        ["email", "address.line1", "address.city", "address.country", "address.weird"],
    )
    assert live["address.line1"] == "VARCHAR(255)"
    assert live["address.city"] == "VARCHAR(255)"
    assert live["address.country"] == "VARCHAR(2)"
    assert "address.weird" not in live

    _merged, err = merge_stripe_catalog_types(
        "customers",
        ["email", "address.line1", "address.weird"],
        studio_types=None,
    )
    assert err is not None
    assert "address.weird" in err


def test_shopify_default_address_leaves_catalogued():
    """Shopify default_address.* must not Map-invent; typed leaves pass gate."""
    from connectors.saas_write_carriers import merge_shopify_catalog_types

    live, err = merge_shopify_catalog_types(
        "customers",
        ["email", "default_address.city", "default_address.country", "default_address.bogus"],
        studio_types=None,
    )
    assert live.get("default_address.city") == "VARCHAR(255)"
    assert live.get("default_address.country") == "VARCHAR(2)"
    assert err is not None
    assert "default_address.bogus" in err

    live2, err2 = merge_shopify_catalog_types(
        "customers",
        ["email", "billing_address.zip", "shipping_address.province_code"],
        studio_types=None,
    )
    assert err2 is None
    assert live2["billing_address.zip"] == "VARCHAR(20)"
    assert live2["shipping_address.province_code"] == "VARCHAR(16)"


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

    dest, err = resolve_object_store_write_dest_types(
        ["amount"],
        [{"source": "amount", "target": "amount", "target_type": "VARCHAR"}],
        {"amount": "VARCHAR"},
        logical_types=["VARCHAR"],
        destination_column_types={"amount": "DECIMAL(18,2)"},
    )
    assert err is None
    assert "DECIMAL" in str(dest.get("amount") or "").upper()


def test_object_store_dest_types_fall_back_to_map_without_studio():
    from connectors.object_store_common import resolve_object_store_write_dest_types

    dest, err = resolve_object_store_write_dest_types(
        ["note"],
        [{"source": "note", "target": "note", "target_type": "VARCHAR(100)"}],
        {"note": "VARCHAR(100)"},
        logical_types=["VARCHAR(100)"],
        destination_column_types=None,
    )
    assert err is None
    assert "VARCHAR" in str(dest.get("note") or "").upper()


def test_object_store_partial_studio_refuses_map_invent():
    from connectors.object_store_common import resolve_object_store_write_dest_types

    dest, err = resolve_object_store_write_dest_types(
        ["amount", "note"],
        [
            {"source": "amount", "target": "amount", "target_type": "VARCHAR"},
            {"source": "note", "target": "note", "target_type": "VARCHAR"},
        ],
        {"amount": "VARCHAR", "note": "VARCHAR"},
        logical_types=["VARCHAR", "VARCHAR"],
        destination_column_types={"amount": "DECIMAL(18,2)"},
    )
    assert err is not None
    assert "note" in err.lower()
    assert "amount" in dest
    assert "refuse" in err.lower()


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


def test_shopify_unknown_metafield_type_refuses_varchar_invent():
    """Unknown Admin metafield tokens must not soft-bind VARCHAR(2048)."""
    from connectors.saas_write_carriers import (
        merge_shopify_catalog_types,
        shopify_metafield_type_to_carrier,
    )

    assert shopify_metafield_type_to_carrier("brand_new_shopify_type_v99") == ""
    assert shopify_metafield_type_to_carrier("list.brand_new_shopify_type_v99") == ""
    # Documented types still resolve.
    assert shopify_metafield_type_to_carrier("url") == "VARCHAR(2048)"

    _live, err = merge_shopify_catalog_types(
        "customers",
        ["email", "custom.mystery"],
        metafield_defs=[
            {
                "namespace": "custom",
                "key": "mystery",
                "type": "brand_new_shopify_type_v99",
            }
        ],
        studio_types=None,
    )
    assert err is not None
    assert "custom.mystery" in err or "mystery" in err

    live2, err2 = merge_shopify_catalog_types(
        "customers",
        ["email", "custom.mystery"],
        metafield_defs=[
            {
                "namespace": "custom",
                "key": "mystery",
                "type": "brand_new_shopify_type_v99",
            }
        ],
        studio_types={"custom.mystery": "JSON"},
    )
    assert err2 is None
    assert live2["custom.mystery"] == "JSON"


def test_email_partial_studio_refuses_map_invent():
    """Email attachment serialize shares object-store Studio coverage gate."""
    from connectors.email import write_mapped_rows

    result = write_mapped_rows(
        connection_string="smtp://localhost:25?to=ops@example.com&from=df@example.com",
        headers=["id", "amt"],
        data_rows=[["1", "9.99"]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "amt", "target": "amt", "target_type": "VARCHAR"},
        ],
        column_types={"id": "VARCHAR", "amt": "VARCHAR"},
        destination_column_types={"id": "INTEGER"},  # partial Studio
    )
    assert result.ok is False
    assert result.error
    assert "Object-store" in (result.error or "") or "Studio" in (result.error or "") or "amt" in (
        result.error or ""
    )
