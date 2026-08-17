"""Smoke: unlocked enterprise drivers are package-available and transfer-ready."""

from __future__ import annotations

from src.transfer.connector_capabilities import (
    driver_available,
    get_capabilities,
    transfer_live_driver_types,
    transfer_ready,
)


def test_unlocked_enterprise_drivers_available_when_packages_present():
    live = set(transfer_live_driver_types())
    for driver in ("sqlserver", "oracle", "sftp", "adls", "pgvector", "qdrant", "weaviate", "pinecone", "milvus", "rest_api"):
        if not driver_available(driver):
            continue
        caps = get_capabilities(driver)
        if caps.get("source_only"):
            # Validate-class preflight gates destinations, so a read-only driver
            # is live on the source path without declaring one.
            assert caps.get("read"), driver
            assert driver in live, f"{driver} should be in package-aware live set"
            continue
        if caps.get("preflight") is False:
            # Installing the package does not unlock a destination that Validate
            # cannot gate — it must stay out of the live set.
            assert not transfer_ready(caps), driver
            assert driver not in live, f"{driver} has no preflight and must not be live"
            continue
        assert transfer_ready(caps), driver
        assert driver in live, f"{driver} should be in package-aware live set"


def test_catalog_has_first_class_ids_for_vector_and_rest():
    from services.catalog_service import _enriched_connectors, get_connector_by_id, load_catalog

    load_catalog.cache_clear()
    _enriched_connectors.cache_clear()

    for cid in ("pgvector", "qdrant", "weaviate", "pinecone", "milvus"):
        row = get_connector_by_id(cid)
        assert row is not None, cid
        assert row.get("id") == cid
        assert row.get("status") == "live"
