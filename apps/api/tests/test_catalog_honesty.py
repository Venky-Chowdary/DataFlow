"""Catalog honesty: REST brand stubs must not appear as live/certified."""

from __future__ import annotations

from src.transfer.connector_capabilities import (
    assert_transfer_endpoint_honesty,
    certification_tier,
    endpoint_allowed_for_role,
    enrich_catalog_entry,
    resolve_driver_type,
)
from src.transfer.registry import validate_transfer


def test_rest_api_brand_stubs_are_planned() -> None:
    """Catalog brand IDs with only a generic rest_api driver are Planned."""
    for brand in ("netsuite", "servicenow"):
        driver = resolve_driver_type(brand)
        assert driver == "rest_api", brand
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live", "description": ""}
        )
        assert row["transfer_ready"] is False, brand
        assert row["effective_status"] == "planned", brand
        assert row["certification_tier"] == "planned", brand
        assert row["capability_label"] == "Planned", brand


def test_dedicated_saas_source_only_drivers() -> None:
    """No brand-specific SaaS driver should remain source-only once a reverse-ETL writer ships."""
    # Zendesk and Notion now have real reverse-ETL writers; any new source-only SaaS
    # must be added here with a documented reason.
    for brand in ():
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live", "description": ""}
        )
        assert row["transfer_ready"] is False, brand
        assert row["effective_status"] == "live", brand
        assert row["certification_tier"] == "source_only", brand
        assert "Source" in row["capability_label"], brand


def test_dedicated_saas_transfer_ready_drivers() -> None:
    """Shopify/Airtable stay Planned until PRODUCTION_SKU. Stripe earned incremental SKU."""
    planned = ("shopify", "airtable", "zendesk", "notion")
    for brand in planned:
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live", "description": ""}
        )
        assert row["transfer_ready"] is False, brand
        assert row["effective_status"] == "planned", brand
        assert row["certification_tier"] == "planned", brand
        assert row["capability_label"] == "Planned", brand


def test_dedicated_saas_drivers_activation_and_source_only() -> None:
    """HubSpot/Salesforce/Stripe are certified; Shopify/Airtable Planned."""
    certified = ("hubspot", "salesforce", "stripe")
    planned = ("airtable", "shopify", "zendesk", "notion")
    for brand in certified:
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live", "description": ""}
        )
        assert row["transfer_ready"] is True, brand
        assert row["certification_tier"] == "certified", brand
        assert row["effective_status"] == "live", brand
    for brand in planned:
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live", "description": ""}
        )
        assert row["transfer_ready"] is False, brand
        assert row["certification_tier"] == "planned", brand
        assert row["effective_status"] == "planned", brand

def test_first_class_rest_api_is_source_only() -> None:
    row = enrich_catalog_entry(
        {"id": "rest_api", "name": "REST API", "category": "api", "status": "live", "description": ""}
    )
    assert row["transfer_ready"] is False
    assert row["effective_status"] == "live"
    assert row["certification_tier"] == "source_only"
    assert "Source" in row["capability_label"]


def test_postgresql_is_certified_transfer_ready() -> None:
    row = enrich_catalog_entry(
        {"id": "postgresql", "name": "PostgreSQL", "category": "database", "status": "live", "description": ""}
    )
    assert row["transfer_ready"] is True
    assert row["effective_status"] == "live"
    assert row["certification_tier"] == "certified"
    assert certification_tier("postgresql", "postgresql", row["capabilities"], transfer_ready_flag=True) == "certified"
    assert row.get("is_hosted_alias") is False
    assert row.get("alias_of") is None


def test_snowflake_cloud_and_edition_tiles_are_aliases() -> None:
    """AWS / Azure / GCP / Standard / Enterprise are the same Snowflake login."""
    for catalog_id in (
        "snowflake_aws",
        "snowflake_azure",
        "snowflake_gcp",
        "snowflake_standard",
        "snowflake_enterprise",
    ):
        row = enrich_catalog_entry(
            {
                "id": catalog_id,
                "name": catalog_id,
                "category": "warehouse",
                "status": "live",
                "description": "",
            }
        )
        assert row["driver_type"] == "snowflake", catalog_id
        assert row["is_hosted_alias"] is True, catalog_id
        assert row["alias_of"] == "snowflake", catalog_id


def test_e1_hosted_twin_is_alias_not_extra_engine() -> None:
    """Phase E1 — postgresql_rds shares the postgresql driver; not a second engine."""
    row = enrich_catalog_entry(
        {
            "id": "postgresql_rds",
            "name": "PostgreSQL (RDS)",
            "category": "database",
            "status": "live",
            "description": "",
        }
    )
    assert row["driver_type"] == "postgresql"
    assert row["is_hosted_alias"] is True
    assert row["alias_of"] == "postgresql"


def test_e1_catalog_summary_prefers_unique_drivers() -> None:
    from services.catalog_service import catalog_summary

    summary = catalog_summary()
    assert summary["unique_drivers"] == summary["transfer_live"] == summary["certified"]
    assert summary["unique_drivers"] <= summary.get("catalog_tile_total", summary["total"])
    assert "alias_tiles" in summary
    assert "honesty_note" in summary
    assert summary["catalog_tiles_are_not_transfer_live"] is True
    assert summary["customer_tenant_warehouse_sku_claimed"] is False
    assert summary["unique_drivers"] < summary.get("catalog_tile_total", summary["total"])


def test_catalog_tiles_are_not_transfer_live() -> None:
    """Named honesty: tile count is not TRANSFER_READY / unique_drivers."""
    from services.catalog_service import catalog_summary

    summary = catalog_summary()
    assert summary["catalog_tiles_are_not_transfer_live"] is True
    tiles = int(summary.get("catalog_tile_total") or 0)
    live = int(summary.get("unique_drivers") or 0)
    assert tiles > live
    assert live == int(summary.get("transfer_live") or 0)
    assert summary.get("catalog_tiles_are_not_transfer_live") is True
    assert "not transfer-live" in str(summary.get("honesty_note") or "")


def test_redshift_is_planned_until_production_sku() -> None:
    """Redshift RW exists but is not SKU-proven — never pitch as Full transfer."""
    row = enrich_catalog_entry(
        {"id": "redshift", "name": "Amazon Redshift", "category": "warehouse", "status": "live", "description": ""}
    )
    assert row["transfer_ready"] is False
    assert row["certification_tier"] == "planned"
    assert row["effective_status"] == "planned"


def test_uncertified_saas_not_in_transfer_ready_catalog_ids() -> None:
    from src.transfer.connector_capabilities import TRANSFER_READY_CATALOG_IDS

    for brand in ("shopify", "airtable", "zendesk", "notion", "redshift"):
        assert brand not in TRANSFER_READY_CATALOG_IDS, brand


def test_uncertified_generic_sql_brands_are_planned() -> None:
    # db2/teradata always Planned; oracle/sql_server only when DBAPI missing.
    for brand in ("db2", "teradata"):
        row = enrich_catalog_entry(
            {"id": brand, "name": brand, "category": "database", "status": "live", "description": ""}
        )
        assert row["transfer_ready"] is False, brand
        assert row["effective_status"] == "planned", brand
        assert row["certification_tier"] == "planned", brand


def test_planned_brand_blocked_as_transfer_endpoint() -> None:
    ok, msg = endpoint_allowed_for_role("db2", "source")
    assert ok is False
    assert "Planned" in msg

    ok, msg = endpoint_allowed_for_role("hubspot", "destination")
    assert ok is True, msg

    ok, msg = endpoint_allowed_for_role("hubspot", "source")
    assert ok is True

    ok, msg = endpoint_allowed_for_role("postgresql", "destination")
    assert ok is True

    honest, _ = assert_transfer_endpoint_honesty("database", "db2", "database", "postgresql")
    assert honest is False
    route_ok, route_msg = validate_transfer("database", "db2", "database", "postgresql")
    assert route_ok is False
    assert "Planned" in route_msg


def test_catalog_search_live_is_certified_only() -> None:
    from services.catalog_service import _enriched_connectors, search_catalog

    _enriched_connectors.cache_clear()
    try:
        data = search_catalog(status="live", limit=500)
        ids = {c["id"] for c in data["connectors"]}
        assert all(c.get("transfer_ready") for c in data["connectors"])
        # Reverse-ETL SaaS (hubspot/salesforce/stripe/shopify/airtable/zendesk/notion)
        # may appear as certified destinations.  REST brand stubs must not.
        for brand in ("netsuite", "servicenow"):
            assert brand not in ids
        assert data.get("transfer_live", 0) < 200  # not hundreds of greenwashed stubs
        assert data.get("certified", 0) > 0
    finally:
        _enriched_connectors.cache_clear()
