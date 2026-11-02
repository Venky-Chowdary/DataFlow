"""CDC lease force-release + holder parsing proofs."""

from __future__ import annotations

from services.cdc_lease import (
    acquire_lease,
    configure_store,
    force_release_lease,
    get_lease,
    parse_holder_job_id,
    renew_lease,
)
from services.cdc_lease_store import MemoryLeaseStore
from services.error_handling import humanize_transfer_failure
from services.cdc_lease import CdcLeaseConflict


def test_parse_holder_job_id() -> None:
    assert parse_holder_job_id("host:jobabc123:deadbeef01") == "jobabc123"
    assert parse_holder_job_id("host:job:abc") is None
    assert parse_holder_job_id("short") is None


def test_force_release_lease_memory() -> None:
    store = configure_store(backend="memory")
    assert isinstance(store, MemoryLeaseStore)
    store.clear()
    lease = acquire_lease("ck-force", resource="res-force", holder_id="holder-a", ttl_sec=60.0)
    assert lease.generation == 1
    assert get_lease("ck-force") is not None

    miss = force_release_lease("ck-force", expected_generation=99)
    assert miss["released"] is False
    assert miss["reason"] == "generation_mismatch"
    assert get_lease("ck-force") is not None

    ok = force_release_lease("ck-force", expected_generation=1, reason="test", actor="pytest")
    assert ok["released"] is True
    assert ok["reason"] == "ok"
    assert get_lease("ck-force") is None

    gone = force_release_lease("ck-force")
    assert gone["released"] is False
    assert gone["reason"] == "not_found"


def test_force_release_fences_zombie_renew() -> None:
    store = configure_store(backend="memory")
    store.clear()
    lease = acquire_lease("ck-zombie", resource="res-z", holder_id="old", ttl_sec=60.0)
    assert force_release_lease("ck-zombie")["released"] is True
    # Prior holder cannot renew after break.
    assert renew_lease("ck-zombie", holder_id="old", generation=lease.generation) is None


def test_list_lease_views_gets_from_a_conflict_to_its_cursor_key() -> None:
    """The conflict names a resource; breaking it needs the key behind it."""
    from services.cdc_lease import list_lease_views

    store = configure_store(backend="memory")
    store.clear()
    acquire_lease(
        "cdc:mine→dst",
        resource="mysql_server_id:90269",
        holder_id="host:jobmine01:aaaaaaaaaa",
        ttl_sec=60.0,
    )
    acquire_lease(
        "cdc:other→dst",
        resource="pg_slot:other",
        holder_id="host:jobother1:bbbbbbbbbb",
        ttl_sec=60.0,
    )

    everything = list_lease_views()
    assert {v["cursor_key"] for v in everything} == {"cdc:mine→dst", "cdc:other→dst"}

    by_resource = list_lease_views(resource="mysql_server_id:90269")
    assert [v["cursor_key"] for v in by_resource] == ["cdc:mine→dst"]
    assert by_resource[0]["holder_job_id"] == "jobmine01"
    assert by_resource[0]["stale"] is False

    assert [v["cursor_key"] for v in list_lease_views(job_id="jobother1")] == [
        "cdc:other→dst"
    ]
    assert list_lease_views(stale_only=True) == []

    store.debug_set_heartbeat("cdc:mine→dst", 0.0)
    assert [v["cursor_key"] for v in list_lease_views(stale_only=True)] == [
        "cdc:mine→dst"
    ]

    assert force_release_lease(by_resource[0]["cursor_key"])["released"] is True
    assert [v["cursor_key"] for v in list_lease_views()] == ["cdc:other→dst"]


def test_humanize_cdc_lease_conflict() -> None:
    exc = CdcLeaseConflict(
        "held",
        holder_id="h1",
        resource="pg_slot:x",
        cursor_key="ck-1",
    )
    h = humanize_transfer_failure(exc)
    assert h["code"] == "cdc_lease_conflict"
    assert h["confidence"] == "high"
    assert h["retriable"] is False
    assert "Force-release" in h["fix"]
