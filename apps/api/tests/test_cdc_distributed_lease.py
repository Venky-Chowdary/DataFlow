"""Distributed CDC lease store — memory / file / Redis proofs.

Covers fencing, resource cross-key conflict, concurrent acquire races, and
(when Redis is up) multi-client Redis atomicity. Fail-closed Redis errors are
asserted without falling back to file.
"""

from __future__ import annotations

import json
import socket
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from services.cdc_lease import (
    CdcLeaseConflict,
    CdcLeaseGuard,
    LeaseStoreError,
    acquire_lease,
    configure_store,
    get_store,
    release_lease,
    renew_lease,
)
from services.cdc_lease_store import (
    FileLeaseStore,
    LeaseStoreError,
    MemoryLeaseStore,
    RedisLeaseStore,
    build_store,
)


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


def test_zombie_apply_refused_after_lease_steal(memory_store) -> None:
    """Stolen holder cannot apply after fence — still at-least-once, not EO."""
    from src.transfer.cdc_transfer import _assert_cdc_lease_before_apply

    sink: list[str] = []

    def apply_if_holder(guard: CdcLeaseGuard, row: str) -> None:
        _assert_cdc_lease_before_apply(type("Cdc", (), {"_lease": guard})())
        sink.append(row)

    a = CdcLeaseGuard(cursor_key="ck-zw", resource="res-zw", holder_id="A", ttl_sec=2.0)
    a.ensure()
    apply_if_holder(a, "v1")
    memory_store.debug_set_heartbeat("ck-zw", 0.0)
    b = CdcLeaseGuard(cursor_key="ck-zw", resource="res-zw", holder_id="B", ttl_sec=2.0)
    b.ensure()
    assert a.renew() is None
    with pytest.raises(CdcLeaseConflict, match="fenced|not held"):
        apply_if_holder(a, "v_zombie")
    apply_if_holder(b, "v2")
    assert sink == ["v1", "v2"]
    b.release()


def test_assert_holder_raises_when_never_acquired(memory_store) -> None:
    g = CdcLeaseGuard(cursor_key="ck-na", resource="res-na", holder_id="x")
    with pytest.raises(CdcLeaseConflict, match="not held"):
        g.assert_holder()


@pytest.fixture(params=("memory", "file", "redis"))
def generation_store(request, tmp_path):
    backend = request.param
    if backend == "memory":
        store = MemoryLeaseStore()
    elif backend == "file":
        path = str(tmp_path / "generation-leases.json")
        store = FileLeaseStore(path=path, data_dir=str(tmp_path))
    else:
        if not _redis_ready():
            pytest.skip("Redis unavailable; start df-redis on 127.0.0.1:6379")
        prefix = f"df-test-lease-gen-{uuid.uuid4().hex}:"
        store = RedisLeaseStore("redis://127.0.0.1:6379/15", key_prefix=prefix)
    yield store
    if backend == "redis":
        client = store._connect()
        keys = list(client.scan_iter(match=f"{store._prefix}*"))
        if keys:
            client.delete(*keys)
        client.close()


def test_release_does_not_reset_generation(generation_store) -> None:
    store = generation_store
    generations = []
    for i in range(6):
        lease = store.acquire(
            cursor_key="ck-cycles",
            resource="res-cycles",
            holder_id=f"cycle-{i}",
            ttl_sec=20,
            meta={},
            now=100 + i,
        )
        generations.append(lease["generation"])
        if i < 5:
            assert store.release(
                cursor_key="ck-cycles",
                holder_id=f"cycle-{i}",
                generation=lease["generation"],
            )
    assert generations == [1, 2, 3, 4, 5, 6]
    assert all(a < b for a, b in zip(generations, generations[1:]))


def test_release_takeover_generation_is_accepted_by_sqlite_eos(
    generation_store, tmp_path
) -> None:
    from connectors.cdc_eos_sql import apply_change_batch_exactly_once
    from services.cdc_engine import ChangeBatch

    store = generation_store
    cursor_key = "ck-eos-generation"
    resource = "res-eos-generation"
    a = store.acquire(
        cursor_key=cursor_key,
        resource=resource,
        holder_id="holder-a",
        ttl_sec=10,
        meta={},
        now=100,
    )
    assert a["generation"] == 1
    store.debug_set_heartbeat(cursor_key, 0)
    b = store.acquire(
        cursor_key=cursor_key,
        resource=resource,
        holder_id="holder-b",
        ttl_sec=10,
        meta={},
        now=100,
    )
    assert b["generation"] == 2

    dest_cfg = {"database": str(tmp_path / "lease-fence.db")}
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "v", "target": "v", "confidence": 1.0},
    ]
    types = {"id": "string", "v": "string"}

    def apply(lsn: str, row: dict[str, str], fence: int) -> int:
        rows, _checksum, _summary, _deleted = apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="orders",
            change=ChangeBatch(inserts=[row], resume_token={"lsn": lsn}),
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="eos-generation|orders",
            writer_fence=fence,
        )
        return rows

    assert apply("0/1", {"id": "1", "v": "b"}, b["generation"]) == 1
    assert store.release(
        cursor_key=cursor_key,
        holder_id="holder-b",
        generation=b["generation"],
    )
    c = store.acquire(
        cursor_key=cursor_key,
        resource=resource,
        holder_id="holder-c",
        ttl_sec=10,
        meta={},
        now=101,
    )
    assert apply("0/2", {"id": "2", "v": "c"}, c["generation"]) == 1
    assert c["generation"] == 3


def test_concurrent_acquires_do_not_duplicate_generations(
    generation_store, tmp_path
) -> None:
    store = generation_store
    stores = [store, store]
    if isinstance(store, FileLeaseStore):
        stores = [
            FileLeaseStore(path=store.path, data_dir=store.data_dir),
            FileLeaseStore(path=store.path, data_dir=store.data_dir),
        ]
    elif isinstance(store, RedisLeaseStore):
        stores = [
            store,
            RedisLeaseStore(store.url, key_prefix=store._prefix),
        ]
    barrier = threading.Barrier(2)

    def acquire(i: int):
        barrier.wait()
        try:
            return stores[i].acquire(
                cursor_key="ck-concurrent-generation",
                resource="res-concurrent-generation",
                holder_id=f"concurrent-{i}",
                ttl_sec=30,
                meta={},
                now=100,
            )
        except CdcLeaseConflict:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(acquire, range(2)))
    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    assert winners[0]["generation"] == 1


def test_old_lease_without_generation_counters_continues_its_generation(
    generation_store,
) -> None:
    store = generation_store
    cursor_key = "ck-old-generation"
    resource = "res-old-generation"
    old_lease = {
        "cursor_key": cursor_key,
        "resource": resource,
        "holder_id": "old-holder",
        "acquired_at": 1,
        "heartbeat_at": 0,
        "ttl_sec": 10,
        "generation": 7,
        "meta": {},
    }
    if isinstance(store, MemoryLeaseStore):
        store._leases[cursor_key] = old_lease
        store._by_resource[resource] = cursor_key
    elif isinstance(store, FileLeaseStore):
        with open(store.path, "w", encoding="utf-8") as f:
            json.dump({"leases": {cursor_key: old_lease}}, f)
    else:
        client = store._connect()
        client.set(store._lease_key(cursor_key), json.dumps(old_lease))
        client.set(store._res_key(resource), cursor_key)

    acquired = store.acquire(
        cursor_key=cursor_key,
        resource=resource,
        holder_id="new-holder",
        ttl_sec=10,
        meta={},
        now=100,
    )
    assert acquired["generation"] == 8


def test_resource_handover_after_release_increments_generation(
    generation_store,
) -> None:
    store = generation_store
    first = store.acquire(
        cursor_key="ck-handover-a",
        resource="res-handover",
        holder_id="handover-a",
        ttl_sec=20,
        meta={},
        now=100,
    )
    assert first["generation"] == 1
    assert store.release(
        cursor_key="ck-handover-a",
        holder_id="handover-a",
        generation=first["generation"],
    )
    second = store.acquire(
        cursor_key="ck-handover-b",
        resource="res-handover",
        holder_id="handover-b",
        ttl_sec=20,
        meta={},
        now=101,
    )
    assert second["generation"] == 2


def test_file_store_uses_platform_data_dir_when_unconfigured(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("DATAFLOW_DATA_DIR", raising=False)
    monkeypatch.delenv("DATAFLOW_CDC_LEASE_PATH", raising=False)
    monkeypatch.delenv("DATAFLOW_CDC_LEASE_DIR", raising=False)
    store = FileLeaseStore()
    assert store.path == str(tmp_path / "cdc_leases.json")
    store.acquire(
        cursor_key="ck-data-dir",
        resource="res-data-dir",
        holder_id="holder",
        ttl_sec=20,
        meta={},
    )
    assert (tmp_path / "cdc_leases.json").exists()


def test_file_store_explicit_path_and_dir_precedence(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    explicit_path = tmp_path / "explicit.json"
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_PATH", str(explicit_path))
    assert build_store("file").path == str(explicit_path)
    monkeypatch.delenv("DATAFLOW_CDC_LEASE_PATH")
    lease_dir = tmp_path / "lease-dir"
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_DIR", str(lease_dir))
    assert build_store("file").path == str(lease_dir / "cdc_leases.json")


@pytest.mark.parametrize("backend", ("memory", "file", "redis"))
def test_min_generation_seeds_above_destination_fence(
    backend, tmp_path, caplog
):
    from connectors.cdc_eos_sql import apply_change_batch_exactly_once
    from services.cdc_exactly_once import ExactlyOnceRouteError
    from services.cdc_engine import ChangeBatch
    from services.cdc_lease import reset_store

    if backend == "memory":
        store = MemoryLeaseStore()
    elif backend == "file":
        path = tmp_path / "leases.json"
        store = FileLeaseStore(path=str(path), data_dir=str(tmp_path))
    else:
        if not _redis_ready():
            pytest.skip("Redis unavailable; start df-redis on 127.0.0.1:6379")
        store = RedisLeaseStore(
            "redis://127.0.0.1:6379/15",
            key_prefix=f"df-test-seed-{uuid.uuid4().hex}:",
        )
    reset_store(store)

    dest_cfg = {"database": str(tmp_path / f"lease-seed-{backend}.db")}
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "v", "target": "v", "confidence": 1.0},
    ]
    types = {"id": "string", "v": "string"}

    def apply(lsn: str, row: dict[str, str], fence: int) -> int:
        rows, _checksum, _summary, _deleted = apply_change_batch_exactly_once(
            dest_type="sqlite",
            dest_cfg=dest_cfg,
            dest_table="orders",
            change=ChangeBatch(inserts=[row], resume_token={"lsn": lsn}),
            mappings=mappings,
            column_types=types,
            headers=["id", "v"],
            pk_target_cols=["id"],
            cursor_key="eos-min-generation|orders",
            writer_fence=fence,
        )
        return rows

    assert apply("0/1", {"id": "seed", "v": "before-reset"}, 5) == 1
    store.acquire(
        cursor_key="ck-before-reset",
        resource="res-before-reset",
        holder_id="old-holder",
        ttl_sec=20,
        meta={},
        now=99,
    )
    if backend == "memory":
        store = MemoryLeaseStore()
    elif backend == "file":
        path.unlink()
        store = FileLeaseStore(path=str(path), data_dir=str(tmp_path))
    else:
        client = store._connect()
        client.delete(*list(client.scan_iter(match=f"{store._prefix}*")))
        client.close()
    reset_store(store)

    caplog.set_level("WARNING")
    guard = CdcLeaseGuard(
        cursor_key="ck-seed",
        resource="res-seed",
        holder_id="seed-holder",
        min_generation=5,
    )
    seeded = guard.ensure()
    assert seeded.generation == 6
    assert "cursor_key=ck-seed" in caplog.text
    assert "resource=res-seed" in caplog.text
    assert "store_generation=0" in caplog.text
    assert "dest_fence=5" in caplog.text
    assert f"backend={backend}" in caplog.text
    assert apply("0/2", {"id": "new", "v": "accepted"}, 6) == 1
    with pytest.raises(ExactlyOnceRouteError):
        apply("0/3", {"id": "zombie", "v": "rejected"}, 5)
    guard.release()


@pytest.mark.parametrize("backend", ("memory", "file", "redis"))
def test_min_generation_does_not_lower_existing_floor(backend, tmp_path, caplog):
    if backend == "memory":
        store = MemoryLeaseStore()
    elif backend == "file":
        store = FileLeaseStore(path=str(tmp_path / "floor.json"), data_dir=str(tmp_path))
    else:
        if not _redis_ready():
            pytest.skip("Redis unavailable; start df-redis on 127.0.0.1:6379")
        store = RedisLeaseStore(
            "redis://127.0.0.1:6379/15",
            key_prefix=f"df-test-floor-{uuid.uuid4().hex}:",
        )
    for i in range(7):
        lease = store.acquire(
            cursor_key="ck-floor",
            resource="res-floor",
            holder_id=f"h-{i}",
            ttl_sec=20,
            meta={},
            now=100 + i,
        )
        if i < 6:
            store.release(
                cursor_key="ck-floor",
                holder_id=f"h-{i}",
                generation=lease["generation"],
            )
    caplog.set_level("WARNING")
    lease = store.acquire(
        cursor_key="ck-floor",
        resource="res-floor",
        holder_id="h-final",
        ttl_sec=20,
        meta={},
        min_generation=2,
        now=200,
    )
    assert lease["generation"] == 8
    assert "generation seeded" not in caplog.text


def test_transfer_fence_seed_helper_is_monotonic():
    from src.transfer.cdc_transfer import _seed_cdc_lease_from_dest_fence

    guard = CdcLeaseGuard(cursor_key="ck", resource="res", min_generation=7)
    cdc = type("Reader", (), {"_lease": guard})()
    _seed_cdc_lease_from_dest_fence(cdc, 5)
    assert guard.min_generation == 7
    _seed_cdc_lease_from_dest_fence(cdc, 9)
    assert guard.min_generation == 9
    _seed_cdc_lease_from_dest_fence(object(), 12)


@pytest.mark.parametrize("backend", ("file", "memory"))
def test_multi_replica_local_backend_fails_closed(monkeypatch, backend):
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    monkeypatch.delenv("DATAWRAP_MULTI_REPLICA", raising=False)
    with pytest.raises(LeaseStoreError, match="DATAFLOW_CDC_LEASE_REDIS_URL"):
        build_store(backend)


def test_multi_replica_redis_backend_allowed(monkeypatch):
    if not _redis_ready():
        pytest.skip("Redis unavailable; start df-redis on 127.0.0.1:6379")
    monkeypatch.setenv("DATAFLOW_MULTI_REPLICA", "1")
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_BACKEND", "redis")
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_REDIS_URL", "redis://127.0.0.1:6379/15")
    assert build_store().name == "redis"


def test_single_host_warning_is_emitted_once(monkeypatch, caplog):
    import services.cdc_lease_store as lease_store_module

    monkeypatch.delenv("DATAFLOW_MULTI_REPLICA", raising=False)
    monkeypatch.delenv("DATAWRAP_MULTI_REPLICA", raising=False)
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_BACKEND", "memory")
    monkeypatch.setattr(lease_store_module, "_STORE", None)
    monkeypatch.setattr(lease_store_module, "_SINGLE_HOST_WARNING_EMITTED", False)
    caplog.set_level("WARNING")
    get_store()
    get_store()
    assert caplog.text.count("is single-host") == 1


def test_health_reports_lease_backend_without_building_store(monkeypatch, tmp_path):
    import services.connector_store as connector_store
    import services.cdc_lease_store as lease_store_module
    import services.health_service as health_service

    data = tmp_path / "data"
    uploads = tmp_path / "uploads"
    data.mkdir()
    uploads.mkdir()
    monkeypatch.setattr(health_service, "data_dir", lambda: data)
    monkeypatch.setattr(health_service, "upload_dir", lambda: uploads)
    monkeypatch.setattr(
        connector_store,
        "connector_persistence_status",
        lambda: {"ok": True, "backend": "file"},
    )
    monkeypatch.setattr(
        lease_store_module,
        "build_store",
        lambda *args, **kwargs: pytest.fail("health check must not build a lease store"),
    )
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_BACKEND", "file")
    monkeypatch.delenv("DATAFLOW_CDC_LEASE_REDIS_URL", raising=False)
    assert health_service.check_storage()["cdc_lease_backend"] == "file"
    assert health_service.check_storage()["cdc_lease_distributed"] is False
    monkeypatch.setenv("DATAFLOW_CDC_LEASE_BACKEND", "redis")
    assert health_service.check_storage()["cdc_lease_backend"] == "redis"
    assert health_service.check_storage()["cdc_lease_distributed"] is True
