"""Distributed CDC lease store — memory / file / Redis proofs.

Covers fencing, resource cross-key conflict, concurrent acquire races, and
(when Redis is up) multi-client Redis atomicity. Fail-closed Redis errors are
asserted without falling back to file.
"""

from __future__ import annotations

import socket
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from services.cdc_lease import (
    CdcLeaseConflict,
    CdcLeaseGuard,
    LeaseStoreError,
    acquire_lease,
    configure_store,
    get_lease,
    get_store,
    release_lease,
    renew_lease,
)
from services.cdc_lease_store import FileLeaseStore, MemoryLeaseStore, RedisLeaseStore


def _redis_ready(url: str = "redis://127.0.0.1:6379/15") -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 6379), timeout=0.5):
            pass
    except OSError:
        return False
    try:
        import redis

        client = redis.from_url(url, socket_connect_timeout=1.0, socket_timeout=1.0)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


@pytest.fixture
def memory_store():
    store = configure_store(backend="memory")
    assert isinstance(store, MemoryLeaseStore)
    store.clear()
    yield store
    store.clear()


def test_memory_fence_blocks_zombie_renew(memory_store) -> None:
    a = acquire_lease("ck-f", resource="res-f", holder_id="old", ttl_sec=2.0)
    assert a.generation == 1
    memory_store.debug_set_heartbeat("ck-f", 0.0)
    b = acquire_lease("ck-f", resource="res-f", holder_id="new", ttl_sec=2.0)
    assert b.generation == 2
    # Zombie renew with old generation must fail.
    assert renew_lease("ck-f", holder_id="old", generation=1) is None
    assert renew_lease("ck-f", holder_id="new", generation=2) is not None
    assert release_lease("ck-f", holder_id="old", generation=1) is False
    assert release_lease("ck-f", holder_id="new", generation=2) is True


def test_memory_concurrent_acquire_single_winner(memory_store) -> None:
    resource = f"race-res-{uuid.uuid4().hex[:8]}"
    winners: list[tuple[str, str]] = []
    errors: list[BaseException] = []

    def _try(i: int) -> tuple[str, str] | None:
        try:
            lease = acquire_lease(
                f"ck-race-{i}",
                resource=resource,
                holder_id=f"w-{i}",
                ttl_sec=30.0,
            )
            return lease.holder_id, lease.cursor_key
        except CdcLeaseConflict as exc:
            errors.append(exc)
            return None

    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = [pool.submit(_try, i) for i in range(12)]
        for fut in as_completed(futs):
            got = fut.result()
            if got:
                winners.append(got)

    assert len(winners) == 1, winners
    assert len(errors) == 11
    holder, cursor = winners[0]
    release_lease(cursor, holder_id=holder)


def test_memory_concurrent_same_cursor_single_winner(memory_store) -> None:
    resource = f"same-ck-{uuid.uuid4().hex[:8]}"
    cursor = f"ck-{resource}"
    winners: list[str] = []

    def _try(i: int) -> str | None:
        try:
            return acquire_lease(
                cursor, resource=resource, holder_id=f"h-{i}", ttl_sec=30.0
            ).holder_id
        except CdcLeaseConflict:
            return None

    with ThreadPoolExecutor(max_workers=16) as pool:
        for fut in as_completed([pool.submit(_try, i) for i in range(16)]):
            hid = fut.result()
            if hid:
                winners.append(hid)

    assert len(set(winners)) == 1
    assert len(winners) == 1
    release_lease(cursor, holder_id=winners[0])


def test_guard_fence_loss_on_steal(memory_store) -> None:
    g1 = CdcLeaseGuard(cursor_key="ck-g", resource="res-g", holder_id="a", ttl_sec=2.0)
    g1.ensure()
    gen1 = g1.generation
    memory_store.debug_set_heartbeat("ck-g", 0.0)
    g2 = CdcLeaseGuard(cursor_key="ck-g", resource="res-g", holder_id="b", ttl_sec=2.0)
    g2.ensure()
    assert g2.generation == gen1 + 1
    assert g1.renew() is None
    assert g1.acquired is False
    g2.release()


def test_file_backend_roundtrip(tmp_path) -> None:
    path = str(tmp_path / "leases.json")
    store = FileLeaseStore(path=path, data_dir=str(tmp_path))
    from services import cdc_lease

    cdc_lease.reset_store(store)
    assert get_store().name == "file"
    a = acquire_lease("ck-file", resource="res-file", holder_id="fa", ttl_sec=1.0)
    assert a.generation == 1
    with pytest.raises(CdcLeaseConflict):
        acquire_lease("ck-file-2", resource="res-file", holder_id="fb", ttl_sec=1.0)
    store.debug_set_heartbeat("ck-file", 0.0)
    stolen = acquire_lease("ck-file", resource="res-file", holder_id="fb", ttl_sec=1.0)
    assert stolen.holder_id == "fb"
    assert stolen.generation >= 2
    assert release_lease("ck-file", holder_id="fb", generation=stolen.generation) is True


def test_redis_fail_closed_without_url(monkeypatch) -> None:
    monkeypatch.delenv("DATAFLOW_CDC_LEASE_REDIS_URL", raising=False)
    monkeypatch.delenv("DATAFLOW_REDIS_URL", raising=False)
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_BACKEND", "redis")
    with pytest.raises(LeaseStoreError):
        configure_store(backend="redis", url="")


def test_redis_fail_closed_on_unreachable() -> None:
    """Explicit redis backend must not silently fall back to file."""
    store = RedisLeaseStore("redis://127.0.0.1:6399/0")  # nothing listening
    from services import cdc_lease

    cdc_lease.reset_store(store)
    with pytest.raises(LeaseStoreError):
        acquire_lease("ck-down", resource="res-down", holder_id="x")


@pytest.mark.skipif(not _redis_ready(), reason="Redis not reachable on 127.0.0.1:6379")
def test_redis_backend_conflict_renew_fence_and_race() -> None:
    url = "redis://127.0.0.1:6379/15"
    prefix = f"df-test-{uuid.uuid4().hex[:8]}:"
    store = RedisLeaseStore(url, key_prefix=prefix)
    from services import cdc_lease

    cdc_lease.reset_store(store)

    suffix = uuid.uuid4().hex[:8]
    ck = f"ck-redis-{suffix}"
    res = f"res-redis-{suffix}"

    a = acquire_lease(ck, resource=res, holder_id="ra", ttl_sec=30.0)
    assert a.generation == 1
    with pytest.raises(CdcLeaseConflict):
        acquire_lease(ck, resource=res, holder_id="rb", ttl_sec=30.0)
    with pytest.raises(CdcLeaseConflict):
        acquire_lease(f"{ck}-other", resource=res, holder_id="rb", ttl_sec=30.0)

    assert renew_lease(ck, holder_id="ra", generation=1) is not None
    store.debug_set_heartbeat(ck, 0.0)
    b = acquire_lease(ck, resource=res, holder_id="rb", ttl_sec=1.0)
    assert b.holder_id == "rb"
    assert b.generation == 2
    assert renew_lease(ck, holder_id="ra", generation=1) is None

    # Concurrent race on a fresh resource
    race_res = f"race-redis-{suffix}"
    winners: list[tuple[str, str]] = []

    def _try(i: int) -> tuple[str, str] | None:
        try:
            lease = acquire_lease(
                f"ck-r-{suffix}-{i}",
                resource=race_res,
                holder_id=f"rw-{i}",
                ttl_sec=30.0,
            )
            return lease.holder_id, lease.cursor_key
        except CdcLeaseConflict:
            return None

    with ThreadPoolExecutor(max_workers=10) as pool:
        for fut in as_completed([pool.submit(_try, i) for i in range(10)]):
            got = fut.result()
            if got:
                winners.append(got)
    assert len(winners) == 1, winners

    release_lease(ck, holder_id="rb", generation=2)
    holder, cursor = winners[0]
    release_lease(cursor, holder_id=holder)


def test_backend_name_in_theater(memory_store) -> None:
    g = CdcLeaseGuard(cursor_key="ck-th", resource="res-th", holder_id="t1")
    g.ensure()
    fields = g.theater_fields()
    assert fields["cdc_lease_backend"] == "memory"
    assert fields["cdc_lease_generation"] == 1
    g.release()
