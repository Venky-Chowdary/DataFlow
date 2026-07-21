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
