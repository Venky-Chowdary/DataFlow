from __future__ import annotations

from connectors.sdk import list_descriptors
from src.transfer.connector_capabilities import (
    enrich_catalog_entry,
    get_capabilities,
    resolve_driver_type,
)
from src.transfer.connector_dispatch import has_reader
from src.transfer.connector_registry import assert_registry_matches_capabilities


def test_sdk_source_descriptors_drive_full_refresh_only_transfer_routes() -> None:
    descriptors = {descriptor.id: descriptor for descriptor in list_descriptors()}
    expected = {"github", "jira", "intercom"}
    assert {key for key in expected if key in descriptors} == expected

    for catalog_id in expected:
        descriptor = descriptors[catalog_id]
        assert descriptor.catalog_ids == (catalog_id,)
        assert "incremental" in descriptor.sync_modes
        assert resolve_driver_type(catalog_id) == descriptor.id
        assert has_reader(descriptor.id)

        caps = get_capabilities(descriptor.id, catalog_id)
        assert caps["test"] is True
        assert caps["read"] is True
        assert caps["write"] is False
        assert caps["source_only"] is True
        assert caps["incremental"] is False
        assert caps["evidence"] == descriptor.evidence == "synthetic-fixture"

        enriched = enrich_catalog_entry({"id": catalog_id, "status": "planned"})
        assert enriched["source_ready"] is True
        assert enriched["dest_ready"] is False
        assert enriched["transfer_ready"] is False
        assert enriched["effective_status"] == "beta"
        assert enriched["certification_tier"] == "source_only"

    assert_registry_matches_capabilities()
