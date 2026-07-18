"""Catalog honesty: REST brand stubs must not appear as live/certified."""

from __future__ import annotations

from src.transfer.connector_capabilities import (
    certification_tier,
    enrich_catalog_entry,
    resolve_driver_type,
)


def test_rest_api_brand_alias_is_planned_not_live() -> None:
    """Catalog brand IDs that only route to generic rest_api are Planned."""
    for brand in ("zendesk", "shopify", "netsuite", "servicenow"):
        driver = resolve_driver_type(brand)
        assert driver == "rest_api", brand
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live", "description": ""}
        )
        assert row["transfer_ready"] is False, brand
        assert row["effective_status"] == "planned", brand
        assert row["certification_tier"] == "planned", brand
        assert row["capability_label"] == "Planned", brand


def test_dedicated_saas_drivers_are_source_only_not_certified() -> None:
    """HubSpot/Salesforce/Stripe have real modules but are source-only (no write)."""
    for brand in ("hubspot", "salesforce", "stripe"):
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live", "description": ""}
        )
        assert row["transfer_ready"] is False, brand
        assert row["certification_tier"] == "source_only", brand
        assert row["effective_status"] == "live", brand


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


def test_uncertified_generic_sql_brands_are_planned() -> None:
    # db2/teradata always Planned; oracle/sql_server only when DBAPI missing.
    for brand in ("db2", "teradata"):
        row = enrich_catalog_entry(
            {"id": brand, "name": brand, "category": "database", "status": "live", "description": ""}
        )
        assert row["transfer_ready"] is False, brand
        assert row["effective_status"] == "planned", brand
        assert row["certification_tier"] == "planned", brand


def test_catalog_search_live_is_certified_only() -> None:
    from services.catalog_service import _enriched_connectors, search_catalog

    _enriched_connectors.cache_clear()
    try:
        data = search_catalog(status="live", limit=500)
        ids = {c["id"] for c in data["connectors"]}
        assert all(c.get("transfer_ready") for c in data["connectors"])
        # Source-only SaaS and REST brand stubs must not appear under Certified.
        for brand in ("hubspot", "salesforce", "stripe", "zendesk", "shopify"):
            assert brand not in ids
        assert data.get("transfer_live", 0) < 200  # not hundreds of greenwashed stubs
        assert data.get("certified", 0) > 0
    finally:
        _enriched_connectors.cache_clear()
