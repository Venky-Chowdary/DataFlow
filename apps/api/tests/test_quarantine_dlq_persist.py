"""Quarantine DLQ must persist rejected rows (retry, no silent drop)."""

from __future__ import annotations

import json
import multiprocessing
import os
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest


def _hold_jsonl_lock(target, locked, release, results, job_id=None):
    try:
        import services.quarantine_dlq as dlq

        with patch.object(dlq, "_dlq_coll", return_value=None):
            if job_id is not None:
                with patch.object(os, "fsync", wraps=os.fsync) as synced:
                    event = dlq.append_dlq_event(
                        job_id=job_id, action="quarantine", path=target,
                    )
                    assert synced.call_count > 0
                results.put(("appended", event))
            with dlq._jsonl_lock(target):
                locked.set()
                if not release.wait(30):
                    raise TimeoutError("lock release was not signaled")
        results.put(("released", None))
    except BaseException as exc:
        results.put(("error", type(exc).__name__))


def _spawn_append_dlq(target, attempting, acknowledged, results, job_id):
    try:
        import services.quarantine_dlq as dlq

        real_lock = dlq._jsonl_lock

        @contextmanager
        def observed_lock(path):
            attempting.set()
            with real_lock(path):
                yield

        with patch.object(dlq, "_dlq_coll", return_value=None):
            with patch.object(dlq, "_jsonl_lock", observed_lock):
                event = dlq.append_dlq_event(
                    job_id=job_id, action="quarantine", path=target,
                )
        results.put(("appended", event))
        acknowledged.set()
    except BaseException as exc:
        results.put(("error", type(exc).__name__))


def _cleanup_dlq_workers(workers, queues):
    try:
        all_stopped = True
        for worker in workers:
            if worker.pid is not None and worker.is_alive():
                worker.terminate()
        for worker in workers:
            if worker.pid is not None:
                worker.join(5)
                if worker.is_alive():
                    worker.kill()
                    worker.join(5)
                if worker.is_alive():
                    all_stopped = False
                else:
                    worker.close()
        assert all_stopped, "DLQ worker did not stop"
    finally:
        for results in queues:
            results.cancel_join_thread()
            results.close()


def test_persist_rejected_rows_writes_dlq(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "quarantine_dlq.jsonl")
    ev = dlq.persist_rejected_rows(
        job_id="job_x",
        rejected_details=[{"row": 1, "reason": "bad_int", "values": {"id": "x"}}],
        workspace_id="ws1",
    )
    assert ev is not None
    assert ev["action"] == "quarantine"
    assert ev["rows"] == 1
    listed = dlq.list_dlq_events(job_id="job_x")
    assert len(listed) == 1
    assert listed[0]["details"]["rejected_details"][0]["reason"] == "bad_int"


def test_append_dlq_retries_then_raises(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "readonly" / "q.jsonl")
    # Make parent unwritable by pointing at a file-as-directory conflict
    blocker = tmp_path / "readonly"
    blocker.write_text("not-a-dir", encoding="utf-8")
    with pytest.raises(OSError):
        dlq.append_dlq_event(job_id="j", action="quarantine", rows=1)


def test_append_dlq_syncs_before_acknowledging(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    synced = []
    real_fsync = os.fsync

    def record_sync(descriptor):
        real_fsync(descriptor)
        synced.append(descriptor)

    monkeypatch.setattr(os, "fsync", record_sync)
    event = dlq.append_dlq_event(
        job_id="durable", action="quarantine", path=tmp_path / "q.jsonl",
    )

    assert event["job_id"] == "durable"
    assert synced


def test_append_dlq_recovers_partial_write_before_retry(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    target = tmp_path / "q.jsonl"
    original = dlq.append_dlq_event(job_id="original", action="quarantine", path=target)
    real_open = Path.open
    attempts = []

    class PartialWriter:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            self.handle.__enter__()
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def __getattr__(self, name):
            return getattr(self.handle, name)

        def write(self, content):
            attempts.append(content)
            if len(attempts) == 1:
                self.handle.write(content[:len(content) // 2])
                self.handle.flush()
                raise OSError("injected partial append")
            return self.handle.write(content)

    def partial_open(path, mode="r", *args, **kwargs):
        handle = real_open(path, mode, *args, **kwargs)
        return PartialWriter(handle) if path == target and "a" in mode else handle

    monkeypatch.setattr(Path, "open", partial_open)
    recovered = dlq.append_dlq_event(job_id="recovered", action="quarantine", path=target)

    events = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
    assert events == [original, recovered]
    assert len(attempts) == 2


def test_append_dlq_refuses_success_when_sync_fails(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    def failed_sync(descriptor):
        raise OSError("injected sync failure")

    monkeypatch.setattr(os, "fsync", failed_sync)
    with pytest.raises((OSError, RuntimeError), match="sync|recover"):
        dlq.append_dlq_event(job_id="undurable", action="quarantine", path=tmp_path / "q.jsonl")


def test_append_dlq_waits_for_other_process_lock(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setenv("PYTHONPATH", "apps/api")
    monkeypatch.setattr(dlq, "_dlq_coll", lambda: None)
    target = tmp_path / "q.jsonl"
    original = [
        dlq.append_dlq_event(job_id=job_id, action="quarantine", path=target)
        for job_id in ("original_a", "original_b")
    ]
    context = multiprocessing.get_context("spawn")
    locked, release = context.Event(), context.Event()
    attempting, acknowledged = context.Event(), context.Event()
    holder_results, append_results = context.Queue(), context.Queue()
    holder = context.Process(
        target=_hold_jsonl_lock,
        args=(target, locked, release, holder_results),
    )
    appender = context.Process(
        target=_spawn_append_dlq,
        args=(target, attempting, acknowledged, append_results, "contender"),
    )
    try:
        holder.start()
        assert locked.wait(20), "holder did not acquire lock"
        appender.start()
        assert attempting.wait(20), "appender did not reach lock acquisition"
        assert not acknowledged.wait(1), "append acknowledged while lock was held"
        assert holder.is_alive(), "holder exited before release"
        assert [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()] == original

        release.set()
        assert acknowledged.wait(20), "append did not acknowledge after release"
        status, appended = append_results.get(timeout=5)
        assert status == "appended", (status, appended)
        assert holder_results.get(timeout=5) == ("released", None)
        holder.join(5)
        appender.join(5)
        assert holder.exitcode == 0
        assert appender.exitcode == 0
        events = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
        assert events == [*original, appended]
        assert appended["job_id"] == "contender"
    finally:
        release.set()
        _cleanup_dlq_workers((holder, appender), (holder_results, append_results))


def test_append_dlq_reacquires_after_synced_holder_termination(tmp_path, monkeypatch):
    """Process termination only: this does not prove OS/power-loss durability."""
    monkeypatch.setenv("PYTHONPATH", "apps/api")
    target = tmp_path / "q.jsonl"
    context = multiprocessing.get_context("spawn")
    locked, release = context.Event(), context.Event()
    attempting, acknowledged = context.Event(), context.Event()
    holder_results, append_results = context.Queue(), context.Queue()
    holder = context.Process(
        target=_hold_jsonl_lock,
        args=(target, locked, release, holder_results, "synced_before_termination"),
    )
    appender = context.Process(
        target=_spawn_append_dlq,
        args=(target, attempting, acknowledged, append_results, "after_termination"),
    )
    try:
        holder.start()
        assert locked.wait(20), "synced holder did not acquire lock"
        status, original = holder_results.get(timeout=5)
        assert status == "appended", (status, original)
        assert [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()] == [original]
        assert holder.is_alive()
        holder.terminate()
        holder.join(5)
        assert not holder.is_alive(), "terminated holder did not exit"
        assert holder.exitcode is not None and holder.exitcode != 0

        appender.start()
        assert attempting.wait(20), "next appender did not reach lock acquisition"
        assert acknowledged.wait(20), "next append could not reacquire abandoned lock"
        status, appended = append_results.get(timeout=5)
        assert status == "appended", (status, appended)
        appender.join(5)
        assert appender.exitcode == 0
        events = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
        assert events == [original, appended]
        assert original["job_id"] == "synced_before_termination"
        assert appended["job_id"] == "after_termination"
    finally:
        _cleanup_dlq_workers((holder, appender), (holder_results, append_results))
