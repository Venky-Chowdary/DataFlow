"""Load-modify-write JSON stores without a lock of their own must not lose updates.

PS-24 moved the fsyncs out of atomic_file's process-wide lock, which widened
an existing window: two threads that both loaded the file, appended, and
renamed could each keep only their own change. ``locked_update(path)`` makes
the whole load-modify-write one step per path.
"""

from __future__ import annotations

import json
import threading
import time

from services import atomic_file, file_load_ledger, usage_metering


def _together(n, fn):
    barrier = threading.Barrier(n)
    errors: list[BaseException] = []

    def run(i):
        barrier.wait()
        try:
            fn(i)
        except BaseException as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not errors, errors


def test_concurrent_usage_events_are_all_kept(tmp_path, monkeypatch):
    path = tmp_path / "usage_metering.json"
    monkeypatch.setattr(usage_metering, "STORE_PATH", path)
    monkeypatch.setattr(usage_metering, "_mongo", lambda: None)
    real_write = atomic_file.write_json_atomic

    def slow_write(target, data, **kwargs):
        time.sleep(0.01)  # widen the load->rename window, as a slow disk would
        return real_write(target, data, **kwargs)

    monkeypatch.setattr(usage_metering, "write_json_atomic", slow_write)
    _together(12, lambda i: usage_metering.record_transfer_usage(job_id=f"job-{i}", rows_written=i))
    events = json.loads(path.read_text())["events"]
    assert sorted(e["job_id"] for e in events) == sorted(f"job-{i}" for i in range(12))


def test_concurrent_file_ledger_updates_are_all_kept(tmp_path, monkeypatch):
    path = tmp_path / "file_load_ledger.json"
    monkeypatch.setattr(file_load_ledger, "STORE_PATH", path)
    monkeypatch.setattr(file_load_ledger, "_mongo_ledger", lambda: None)
    real_write = atomic_file.write_json_atomic

    def slow_write(target, data, **kwargs):
        time.sleep(0.01)
        return real_write(target, data, **kwargs)

    monkeypatch.setattr(file_load_ledger, "write_json_atomic", slow_write)
    _together(10, lambda i: (
        file_load_ledger.record_file_loaded(f"route-{i}", f"digest-{i}")
        if i % 2 else
        file_load_ledger.record_manifest_file(f"route-{i}", name=f"f{i}.csv", checksum=f"c{i}")
    ))
    keys = {entry["key"] for entry in json.loads(path.read_text())["files"]}
    assert keys == {f"route-{i}" for i in range(10)}


def test_locked_update_is_one_reentrant_lock_per_path(tmp_path):
    a, b = tmp_path / "a.json", tmp_path / "b.json"
    with atomic_file.locked_update(a):
        with atomic_file.locked_update(a):  # re-entrant in one thread
            pass
    assert atomic_file.path_update_lock(a) is atomic_file.path_update_lock(tmp_path / "." / "a.json")
    assert atomic_file.path_update_lock(a) is not atomic_file.path_update_lock(b)


def _slow_writes(monkeypatch, module):
    real_write = atomic_file.write_json_atomic

    def slow_write(target, data, **kwargs):
        time.sleep(0.01)
        return real_write(target, data, **kwargs)

    monkeypatch.setattr(module, "write_json_atomic", slow_write)


def test_concurrent_watermark_writes_are_all_kept(tmp_path, monkeypatch):
    from services import sync_cursor

    monkeypatch.setattr(sync_cursor, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sync_cursor, "_mongo_cursors", lambda: None)
    _slow_writes(monkeypatch, sync_cursor)
    _together(12, lambda i: sync_cursor.set_watermark(f"route-{i}", str(100 + i)))
    keys = {e["key"] for e in json.loads((tmp_path / "sync_cursors.json").read_text())["cursors"]}
    assert keys == {f"route-{i}" for i in range(12)}

    # Clears and job forgets racing new watermarks lose nothing either.
    sync_cursor.set_watermark("gone-a", "1", metadata={"job_id": "job-x"})
    sync_cursor.set_watermark("gone-b", "1")
    _together(6, lambda i: (
        sync_cursor.clear_watermark("gone-b") if i == 0
        else sync_cursor.forget_job_watermarks({"job-x"}) if i == 1
        else sync_cursor.set_watermark(f"late-{i}", "1")
    ))
    keys = {e["key"] for e in json.loads((tmp_path / "sync_cursors.json").read_text())["cursors"]}
    assert {f"late-{i}" for i in range(2, 6)} <= keys
    assert {f"route-{i}" for i in range(12)} <= keys
    assert not {"gone-a", "gone-b"} & keys


def test_concurrent_schema_registrations_are_all_kept_with_unique_versions(tmp_path, monkeypatch):
    from services import schema_registry

    monkeypatch.setattr(schema_registry, "_SCHEMAS_PATH", tmp_path / "schemas.json")
    monkeypatch.setattr(schema_registry, "_schemas_coll", lambda: None)
    _slow_writes(monkeypatch, schema_registry)
    _together(8, lambda i: schema_registry.register_schema(
        [{"name": "id", "type": "INTEGER"}], connector_type="pg", connector_id=f"c{i}", object_name="t",
    ))
    stored = json.loads((tmp_path / "schemas.json").read_text())
    assert set(stored) == {f"pg:c{i}:t" for i in range(8)}

    # Same object, different column sets at once: versions 2..7, none reused.
    _together(6, lambda i: schema_registry.register_schema(
        [{"name": f"col{i}", "type": "TEXT"}], connector_type="pg", connector_id="c0", object_name="t",
    ))
    versions = [v["version"] for v in json.loads((tmp_path / "schemas.json").read_text())["pg:c0:t"]]
    assert versions == list(range(1, 8)), versions


class _FakeSchemasColl:
    """Enough of a Mongo collection for schema_registry, with a hook in reads."""

    def __init__(self, on_read=None):
        self.docs: dict[str, dict] = {}
        self.on_read = on_read or (lambda key: None)

    def find_one(self, query):
        self.on_read(query["_id"])
        doc = self.docs.get(query["_id"])
        return json.loads(json.dumps(doc)) if doc else None

    def replace_one(self, query, doc, upsert=False):
        self.docs[query["_id"]] = json.loads(json.dumps(doc))


def test_a_slow_mongo_registration_holds_neither_the_file_lock_nor_other_objects(tmp_path, monkeypatch):
    from services import schema_registry

    entered, release = threading.Event(), threading.Event()
    slow_key = "pg:slow:t"

    def on_read(key):
        if key == slow_key:
            entered.set()
            release.wait(10)

    coll = _FakeSchemasColl(on_read)
    monkeypatch.setattr(schema_registry, "_SCHEMAS_PATH", tmp_path / "schemas.json")
    monkeypatch.setattr(schema_registry, "_schemas_coll", lambda: coll)
    cols = [{"name": "id", "type": "INTEGER"}]
    slow = threading.Thread(target=lambda: schema_registry.register_schema(
        cols, connector_type="pg", connector_id="slow", object_name="t",
    ))
    slow.start()
    try:
        assert entered.wait(5)
        # The file store's path lock is free while Mongo answers slowly.
        lock = atomic_file.path_update_lock(tmp_path / "schemas.json")
        assert lock.acquire(timeout=2), "register_schema held the file lock across a Mongo read"
        lock.release()
        # Another object (on another lock stripe) registers meanwhile.
        other = next(
            f"fast{i}" for i in range(1000)
            if schema_registry._registration_lock(f"pg:fast{i}:t")
            is not schema_registry._registration_lock(slow_key)
        )
        fast = threading.Thread(target=lambda: schema_registry.register_schema(
            cols, connector_type="pg", connector_id=other, object_name="t",
        ))
        fast.start()
        fast.join(timeout=3)
        assert not fast.is_alive(), "one object's slow Mongo read blocked another object"
    finally:
        release.set()
        slow.join(timeout=10)
    assert set(coll.docs) == {slow_key, f"pg:{other}:t"}


def test_concurrent_mongo_registrations_of_one_object_take_unique_versions(monkeypatch):
    from services import schema_registry

    coll = _FakeSchemasColl(lambda key: time.sleep(0.02))
    monkeypatch.setattr(schema_registry, "_schemas_coll", lambda: coll)
    _together(6, lambda i: schema_registry.register_schema(
        [{"name": f"col{i}", "type": "TEXT"}], connector_type="pg", connector_id="m", object_name="t",
    ))
    versions = [v["version"] for v in coll.docs["pg:m:t"]["versions"]]
    assert versions == list(range(1, 7)), versions


def test_concurrent_lineage_appends_are_all_kept(tmp_path, monkeypatch):
    from services import schema_registry

    monkeypatch.setattr(schema_registry, "_LINEAGE_PATH", tmp_path / "lineage.json")
    monkeypatch.setattr(schema_registry, "_lineage_coll", lambda: None)
    _slow_writes(monkeypatch, schema_registry)
    _together(10, lambda i: schema_registry.record_lineage(
        {"connector_id": "s", "connector_type": "pg", "object_name": "a"},
        {"connector_id": "d", "connector_type": "pg", "object_name": "b"},
        [{"source": f"c{i}", "target": f"c{i}"}],
        job_id=f"job-{i}",
    ))
    edges = json.loads((tmp_path / "lineage.json").read_text())["edges"]
    assert sorted(e["job_id"] for e in edges) == sorted(f"job-{i}" for i in range(10))


def test_concurrent_retired_slot_records_are_all_kept(tmp_path, monkeypatch):
    from services import cdc_slot_resume

    monkeypatch.setattr(cdc_slot_resume, "STORE_PATH", tmp_path / "retired.json")
    monkeypatch.setattr(cdc_slot_resume, "_mongo_retired", lambda: None)
    _slow_writes(monkeypatch, cdc_slot_resume)
    _together(10, lambda i: cdc_slot_resume.retire_slot_lsn(f"0/{i:X}", cursor_key=f"k{i}", slot_name="s"))
    rows = json.loads((tmp_path / "retired.json").read_text())["resumes"]
    assert sorted(r["cursor_key"] for r in rows) == sorted(f"k{i}" for i in range(10))


_DQ_SRC = {"kind": "database", "type": "postgresql", "table": "a"}
_DQ_DST = {"kind": "database", "type": "postgresql", "table": "b"}


def _quality_store(tmp_path, monkeypatch):
    from services import data_quality_history as dq

    monkeypatch.setattr(dq, "quality_profiles_dir", lambda: tmp_path)
    monkeypatch.setattr(dq, "_profile_collection", lambda: None)
    return dq, {"id": dq.profile_column([1, 2, 3], "id", "integer")}


def test_concurrent_quality_profiles_are_all_kept(tmp_path, monkeypatch):
    dq, profile = _quality_store(tmp_path, monkeypatch)
    src, dst = _DQ_SRC, _DQ_DST
    _slow_writes(monkeypatch, dq)
    _together(8, lambda i: dq.save_profile(src, dst, profile, job_id=f"job-{i}", pii_hash_key_version=""))
    runs = dq.load_run_history(src, dst)
    assert sorted(r["job_id"] for r in runs) == sorted(f"job-{i}" for i in range(8))


def test_concurrent_parity_records_are_all_kept(tmp_path, monkeypatch):
    dq, profile = _quality_store(tmp_path, monkeypatch)
    src, dst = _DQ_SRC, _DQ_DST
    for i in range(8):  # the runs exist first, one at a time
        dq.save_profile(src, dst, profile, job_id=f"job-{i}", pii_hash_key_version="")
    _slow_writes(monkeypatch, dq)
    _together(8, lambda i: dq.record_run_parity(src, dst, job_id=f"job-{i}", parity={"ok": i}))
    parity = {r["job_id"]: r.get("parity") for r in dq.load_run_history(src, dst)}
    assert parity == {f"job-{i}": {"ok": i} for i in range(8)}


def test_concurrent_pending_release_ledger_updates_are_all_kept(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from services import cdc_capture_release as ccr

    monkeypatch.setattr(ccr, "_ledger_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(ccr, "_ledger_coll", lambda: None)
    real_write = atomic_file.write_json_atomic

    def slow_write(target, data, **kwargs):
        time.sleep(0.01)
        return real_write(target, data, **kwargs)

    monkeypatch.setattr(atomic_file, "write_json_atomic", slow_write)  # imported per call there

    def sched(i):
        return SimpleNamespace(id=f"s{i}", name=f"s{i}", workspace_id="", source_connector_id="c")

    seeded = [ccr._record_pending(sched(i), {}, slots=[])["pending_id"] for i in range(4)]

    def step(i):
        if i < 4:
            ccr._record_pending(sched(10 + i), {}, slots=[])
        elif i < 6:
            ccr._drop_pending(seeded[i - 4])
        else:
            ccr._bump_pending({"id": seeded[i - 4], "attempts": 0}, "busy")

    _together(8, step)
    entries = {e["schedule_id"]: e for e in ccr.list_pending_capture_releases()}
    assert set(entries) == {"s2", "s3", "s10", "s11", "s12", "s13"}
    assert entries["s2"]["attempts"] == 1 and entries["s3"]["attempts"] == 1


def test_concurrent_ddl_records_take_unique_versions(tmp_path, monkeypatch):
    from services import cdc_schema_history as sh

    monkeypatch.setattr(sh, "STORE_DIR", tmp_path)
    monkeypatch.setattr(sh, "_mongo", lambda: None)
    _slow_writes(monkeypatch, sh)
    _together(8, lambda i: sh.record_ddl("pg:src", "orders", f"ALTER TABLE orders ADD c{i} INT", i, {"c": i}))
    history = sh.list_history("pg:src", "orders")
    assert sorted(e["version"] for e in history) == list(range(1, 9))
    assert sorted(e["ddl"] for e in history) == sorted(f"ALTER TABLE orders ADD c{i} INT" for i in range(8))


def test_usage_backfill_keeps_events_recorded_while_it_runs(tmp_path, monkeypatch):
    path = tmp_path / "usage_metering.json"
    monkeypatch.setattr(usage_metering, "STORE_PATH", path)
    monkeypatch.setattr(usage_metering, "_mongo", lambda: None)
    _slow_writes(monkeypatch, usage_metering)
    path.write_text(json.dumps({"events": [
        {"id": f"old-{i}", "job_id": f"old-{i}", "workspace_id": "", "rows_written": 1} for i in range(3)
    ]}))

    def placed(_event):
        time.sleep(0.02)  # a slow owner lookup keeps the backfill's load->write window open
        return "ws-1"

    _together(7, lambda i: (
        usage_metering.assign_unscoped_workspaces(placed, apply=True)
        if i == 0 else
        usage_metering.record_transfer_usage(job_id=f"new-{i}", rows_written=i)
    ))
    events = json.loads(path.read_text())["events"]
    jobs = {e["job_id"] for e in events}
    assert {f"new-{i}" for i in range(1, 7)} <= jobs, sorted(jobs)
    assert all(e["workspace_id"] == "ws-1" for e in events if e["job_id"].startswith("old-"))
