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
