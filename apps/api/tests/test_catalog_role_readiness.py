"""A picker must only offer the side a connector can actually take.

``role`` used only to re-sort the catalog, so the source picker and the
destination picker were handed the same list. That offered Pinecone, Weaviate,
Milvus, Qdrant and pgvector as *sources* — none of which can be read — and an
operator who picked one got a route that failed at Execute, after choosing it,
naming it and mapping against it.

The counts had the same shape of problem: one "transfer-live drivers" number
answered "how many can I use as a source?" and "…as a destination?" identically,
while the true answers differ, because a vector store is a destination only and
a REST feed is a source only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.catalog_service import catalog_summary, search_catalog  # noqa: E402
from src.transfer.connector_capabilities import (  # noqa: E402
    dest_live_driver_types,
    enrich_catalog_entry,
    source_live_driver_types,
)


def _offered(role: str) -> list[dict]:
    return search_catalog(role=role, transfer_only=True, limit=1000)["connectors"]


def test_source_picker_never_offers_a_write_only_store():
    """A vector store is a destination. It cannot be read, so it is not a source."""
    source_drivers = set(source_live_driver_types())
    unusable = [
        c["id"]
        for c in _offered("source")
        if (c.get("driver_type") or "") not in source_drivers
    ]
    assert unusable == [], f"offered as sources but cannot be read: {unusable}"


def test_destination_picker_never_offers_a_read_only_feed():
    dest_drivers = set(dest_live_driver_types())
    unusable = [
        c["id"]
        for c in _offered("destination")
        if (c.get("driver_type") or "") not in dest_drivers
    ]
    assert unusable == [], f"offered as destinations but cannot be written: {unusable}"


@pytest.mark.parametrize("connector_id", ["pinecone", "weaviate", "milvus", "qdrant"])
def test_vector_stores_are_destinations_only(connector_id: str):
    """The specific tiles the source picker used to offer."""
    row = enrich_catalog_entry({"id": connector_id, "status": "live"})
    assert row["dest_ready"] is True
    assert row["source_ready"] is False
    assert connector_id not in {c["id"] for c in _offered("source")}
    assert connector_id in {c["id"] for c in _offered("destination")}


def test_a_duplex_engine_is_offered_on_both_sides():
    for role in ("source", "destination"):
        assert "postgresql" in {c["id"] for c in _offered(role)}


def test_suggestions_obey_the_same_role_rule():
    """A suggestion is still an offer, so it cannot skip the check."""
    source_drivers = set(source_live_driver_types())
    dest_drivers = set(dest_live_driver_types())
    for role, allowed in (("source", source_drivers), ("destination", dest_drivers)):
        suggested = search_catalog(role=role, transfer_only=True, limit=1000)["suggested"]
        assert suggested, f"{role} should suggest something"
        bad = [s["id"] for s in suggested if (s.get("driver_type") or "") not in allowed]
        assert bad == [], f"{role} suggestions unusable in that role: {bad}"


def test_per_side_counts_describe_the_list_they_label():
    """The number shown next to a role-scoped list must count that list."""
    for role, key in (("source", "source_live"), ("destination", "dest_live")):
        result = search_catalog(role=role, transfer_only=True, limit=1000)
        offered_drivers = {
            c.get("driver_type") for c in result["connectors"] if c.get("driver_type")
        }
        assert result["role_live"] == result[key]
        assert result["role_live"] == len(offered_drivers), (
            f"{role}: labelled {result['role_live']} but the list holds "
            f"{len(offered_drivers)} distinct engines"
        )


def test_side_counts_differ_from_the_transfer_total():
    """One number cannot answer both questions; the catalog now says so.

    ``transfer_live`` counts engines usable in *a* transfer — the union of both
    sides — so it necessarily exceeds at least one of them whenever any
    connector is single-sided.
    """
    summary = catalog_summary()
    assert summary["source_live"] > 0
    assert summary["dest_live"] > 0
    assert summary["source_live"] != summary["dest_live"], (
        "single-sided connectors exist, so the two sides cannot have equal counts"
    )
    unscoped = search_catalog(role="all", transfer_only=True, limit=1000)
    assert unscoped["role_live"] == unscoped["transfer_live"]


@pytest.mark.parametrize("connector_id", ["couchbase", "neo4j", "influxdb"])
def test_transfer_ready_filter_keeps_single_sided_sources(connector_id: str):
    """"Transfer ready" on a source list must mean ready *as a source*.

    It meant duplex, so ticking that filter on the source picker hid exactly the
    connectors that can only be sources — an operator searching for Couchbase or
    Neo4j found nothing.
    """
    offered = {
        c["id"]
        for c in search_catalog(role="source", status="live", limit=1000)["connectors"]
    }
    assert connector_id in offered

    # …and it must still exclude them from the destination list.
    dest = {
        c["id"]
        for c in search_catalog(role="destination", status="live", limit=1000)["connectors"]
    }
    assert connector_id not in dest


def test_transfer_ready_filter_without_a_role_still_means_duplex():
    """Unscoped, "transfer ready" keeps its stricter read-and-write meaning."""
    offered = {c["id"] for c in search_catalog(status="live", limit=1000)["connectors"]}
    assert "postgresql" in offered
    assert "couchbase" not in offered


def test_transfer_capabilities_splits_the_two_sides():
    """Transfer Studio builds its pickers from this, so it has the same duty.

    It asked the catalog for ``status="live"``, which is the *duplex* filter, so
    a source-only connector landed in neither list and could not be chosen at
    all — Couchbase, Neo4j and InfluxDB were unusable in Transfer Studio despite
    being read-capable sources.
    """
    from src.transfer.registry import get_capabilities

    caps = get_capabilities()
    sources = set(caps["source_databases"])
    destinations = set(caps["destination_databases"])

    # Write-only stores are destinations, never sources.
    for write_only in ("pinecone", "weaviate", "qdrant", "milvus", "pgvector"):
        assert write_only not in sources, f"{write_only} cannot be read"
        assert write_only in destinations, f"{write_only} should be offered as a destination"

    # Read-only connectors are sources, never destinations.
    for read_only in ("couchbase", "neo4j", "influxdb"):
        assert read_only in sources, f"{read_only} is a readable source"
        assert read_only not in destinations, f"{read_only} cannot be written to"

    # A duplex engine belongs on both sides.
    assert "postgresql" in sources and "postgresql" in destinations


def test_planned_tiles_are_ready_for_neither_side():
    """A roadmap entry must not be offered on either side of a transfer."""
    for role in ("source", "destination"):
        planned = [
            c["id"]
            for c in _offered(role)
            if c.get("certification_tier") == "planned"
            or c.get("effective_status") == "planned"
        ]
        assert planned == [], f"{role} offers planned tiles: {planned[:5]}"
