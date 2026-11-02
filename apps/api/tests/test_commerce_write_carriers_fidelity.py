"""Stripe / Shopify documented write carriers — no silent commerce overflow."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.saas_write_carriers import (  # noqa: E402
    shopify_core_field_carriers,
    shopify_live_types_for_columns,
    shopify_metafield_type_to_carrier,
    stripe_field_carriers,
    stripe_live_types_for_columns,
)
from connectors.shopify_writer import resolve_shopify_dest_types  # noqa: E402
from connectors.stripe_writer import resolve_stripe_dest_types  # noqa: E402
from connectors.writer_common import apply_write_quarantine_matrix  # noqa: E402


def test_stripe_customer_documented_limits():
    carriers = stripe_field_carriers("customers")
    assert carriers["email"] == "VARCHAR(512)"
    assert carriers["name"] == "VARCHAR(256)"
    assert carriers["phone"] == "VARCHAR(20)"
    assert carriers["business_name"] == "VARCHAR(150)"
    assert carriers["amount"] == "INTEGER"


def test_stripe_subscription_description_bound_and_aliases():
    sub = stripe_field_carriers("subscriptions")
    assert sub["description"] == "VARCHAR(500)"
    assert sub["cancel_at_period_end"] == "BOOLEAN"
    assert stripe_field_carriers("subscription")["description"] == "VARCHAR(500)"
    refund = stripe_field_carriers("refunds")
    assert refund["reason"] == "VARCHAR(64)"
    coupon = stripe_field_carriers("coupons")
    assert coupon["name"] == "VARCHAR(40)"


def test_stripe_live_types_metadata_and_address():
    live = stripe_live_types_for_columns(
        "customers",
        ["email", "metadata.order_id", "metadata_cms_id", "address.country"],
    )
    assert live["email"] == "VARCHAR(512)"
    assert live["metadata.order_id"] == "VARCHAR(500)"
    assert live["metadata_cms_id"] == "VARCHAR(500)"
    assert live["address.country"] == "VARCHAR(2)"


def test_resolve_stripe_dest_types_prefers_catalog():
    types = resolve_stripe_dest_types(
        ["email", "phone"],
        [
            {"source": "e", "target": "email", "target_type": "VARCHAR(2000)"},
            {"source": "p", "target": "phone", "target_type": "TEXT"},
        ],
        {},
        object_type="customers",
    )
    assert types["email"] == "VARCHAR(512)"
    assert types["phone"] == "VARCHAR(20)"


def test_resolve_stripe_shopify_omit_uncatalogued_without_map_invent():
    from connectors.saas_write_carriers import merge_stripe_catalog_types

    types = resolve_stripe_dest_types(
        ["email", "invented_field"],
        [
            {"source": "e", "target": "email", "target_type": "VARCHAR"},
            {"source": "i", "target": "invented_field", "target_type": "VARCHAR"},
        ],
        {},
        object_type="customers",
    )
    assert "email" in types
    assert "invented_field" not in types
    # Studio still fills gaps when supplied to merge via resolve.
    types2 = resolve_stripe_dest_types(
        ["email", "invented_field"],
        [],
        {},
        object_type="customers",
        studio_types={"invented_field": "INTEGER"},
    )
    assert types2["invented_field"] == "INTEGER"

    shop = resolve_shopify_dest_types(
        ["email", "bogus_meta"],
        [],
        {},
        object_type="customers",
    )
    assert "email" in shop
    assert "bogus_meta" not in shop
    # Pre-merged live_types from write path are returned as-is.
    _live, err = merge_stripe_catalog_types(
        "customers", ["email"], studio_types=None
    )
    assert err is None
    assert resolve_stripe_dest_types(
        ["email"], [], {}, live_types=_live
    )["email"].startswith("VARCHAR")


def test_stripe_quarantine_holds_email_and_phone_overflow():
    details: list[dict] = []
    out = apply_write_quarantine_matrix(
        [
            ("a" * 513, "123"),
            ("ok@example.com", "1" * 21),
            ("ok@example.com", "555-0100"),
        ],
        ["email", "phone"],
        ["VARCHAR(512)", "VARCHAR(20)"],
        details,
        policy="quarantine",
        dialect_label="Stripe",
    )
    assert out == [("ok@example.com", "555-0100")]
    assert any("Stripe" in d.get("reason", "") for d in details)


def test_shopify_core_and_metafield_carriers():
    core = shopify_core_field_carriers("customers")
    assert core["note"] == "VARCHAR(5000)"
    assert core["email"] == "VARCHAR(255)"
    assert shopify_metafield_type_to_carrier("number_decimal") == "DECIMAL(22,9)"
    assert shopify_metafield_type_to_carrier("number_integer") == "BIGINT"
    assert (
        shopify_metafield_type_to_carrier(
            "single_line_text_field", max_validation=80
        )
        == "VARCHAR(80)"
    )
    assert shopify_metafield_type_to_carrier("boolean") == "BOOLEAN"
    assert shopify_metafield_type_to_carrier("date_time") == "TIMESTAMPTZ"
    drafts = shopify_core_field_carriers("draft_orders")
    assert drafts["invoice_sent_at"] == "TIMESTAMPTZ"
    assert drafts["completed_at"] == "TIMESTAMPTZ"


def test_shopify_draft_orders_and_collections_catalog():
    drafts = shopify_core_field_carriers("draft_orders")
    assert drafts["note"] == "VARCHAR(5000)"
    assert drafts["email"] == "VARCHAR(255)"
    assert shopify_core_field_carriers("draft_order")["note"] == "VARCHAR(5000)"
    cols = shopify_core_field_carriers("collections")
    assert cols["title"] == "VARCHAR(255)"
    assert cols["body_html"] == "VARCHAR(65535)"


def test_shopify_live_types_merge_metafield_defs():
    live = shopify_live_types_for_columns(
        "customers",
        ["email", "note", "custom.vip", "vip"],
        metafield_defs=[
            {
                "namespace": "custom",
                "key": "vip",
                "type": "boolean",
                "validations": [],
            },
            {
                "namespace": "custom",
                "key": "sku",
                "type": "single_line_text_field",
                "validations": [{"name": "max", "value": "40"}],
            },
        ],
    )
    assert live["email"] == "VARCHAR(255)"
    assert live["note"] == "VARCHAR(5000)"
    assert live["custom.vip"] == "BOOLEAN"
    assert live["vip"] == "BOOLEAN"
    assert live["custom.sku"] == "VARCHAR(40)"


def test_resolve_shopify_dest_types_prefers_core_catalog():
    types = resolve_shopify_dest_types(
        ["note", "email"],
        [
            {"source": "n", "target": "note", "target_type": "TEXT"},
            {"source": "e", "target": "email", "target_type": "TEXT"},
        ],
        {},
        object_type="customers",
    )
    assert types["note"] == "VARCHAR(5000)"
    assert types["email"] == "VARCHAR(255)"


def test_shopify_quarantine_holds_note_overflow():
    details: list[dict] = []
    out = apply_write_quarantine_matrix(
        [("N" * 5001,), ("short note",)],
        ["note"],
        ["VARCHAR(5000)"],
        details,
        policy="quarantine",
        dialect_label="Shopify",
    )
    assert out == [("short note",)]
    assert any("Shopify" in d.get("reason", "") for d in details)
