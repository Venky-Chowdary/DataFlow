"""D168 — resume must not revive the worker it was meant to replace.

A job keeps one id across cancel -> resume, and resume clears the cancel flag.
A previous worker that had not yet noticed the cancel then saw "not cancelled"
again and carried on writing next to the new attempt (the destination kept
growing while the job read ``cancelled``). Every execution now stamps a
``run_attempt`` on the job; a worker whose attempt is no longer the current one
stops, and writes nothing to the job record or its checkpoint.
"""

from __future__ import annotations

import contextvars
import os
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.cancel_check as cc  # noqa: E402
from services.checkpoint_service import Checkpoint, CheckpointService  # noqa: E402
from services.error_handling import TransferCancelled  # noqa: E402
from services.mongodb_service import MemoryMongoDBService  # noqa: E402


def _job(store: MemoryMongoDBService) -> str:
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "running", transfer_request={})
    return job_id


def _new_attempt_in_other_context(store, job_id: str) -> str | None:
    """What a resumed ``execute_tracked`` does, in its own context."""
    return contextvars.Context().run(cc.begin_run_attempt, store, job_id)


def test_old_attempt_stops_after_resume_cleared_the_cancel_flag():
    store = MemoryMongoDBService()
    job_id = _job(store)
    assert cc.begin_run_attempt(store, job_id)
    cc.raise_if_job_cancelled(store, job_id)  # current attempt: carries on

    store.request_job_cancel(job_id)
    store.update_job_status(job_id, "cancelled", operator_command=True)
    store.clear_job_cancel(job_id)
    store.update_job_status(job_id, "pending", allow_terminal_exit=True)
    assert _new_attempt_in_other_context(store, job_id)

    try:
        with pytest.raises(TransferCancelled) as stopped:
            cc.raise_if_job_cancelled(store, job_id, rows_written=7)
        assert stopped.value.superseded is True
    finally:
        cc.end_run_attempt()


def test_new_attempt_is_not_stopped_by_its_own_stamp():
    store = MemoryMongoDBService()
    job_id = _job(store)
    assert cc.begin_run_attempt(store, job_id)
    try:
        cc.raise_if_job_cancelled(store, job_id)
        assert cc.attempt_superseded(store, job_id) is False
    finally:
        cc.end_run_attempt()


def test_superseded_attempt_cannot_overwrite_the_new_attempts_checkpoint(monkeypatch):
    store = MemoryMongoDBService()
    job_id = _job(store)
    assert cc.begin_run_attempt(store, job_id)
    assert _new_attempt_in_other_context(store, job_id)
    service = CheckpointService()
    monkeypatch.setattr(service, "_mongo", lambda: store)
    stale = Checkpoint(job_id=job_id, chunk_index=9, rows_processed=900, offset=900)
    try:
        with pytest.raises(TransferCancelled) as stopped:
            service.save(stale)
        assert stopped.value.superseded is True
    finally:
        cc.end_run_attempt()
    assert not (store.get_job(job_id) or {}).get("checkpoint")


def test_engine_run_stops_when_a_newer_attempt_takes_over(tmp_path: Path, monkeypatch):
    """Real engine: a newer attempt stamps the job mid-run; the old run stops."""
    import services.mongodb_service as mongo_mod
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    import src.transfer.stream as stream_mod
    from src.transfer.engine import UniversalTransferEngine
    from tests.test_d124_watermark_tie_group_e2e import _backend
    from tests.test_d134_cancel_resume_e2e import _insert, _request

    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 5)

    src, dst = _backend("sqlite", tmp_path, "src"), _backend("sqlite", tmp_path, "dst")
    suffix = uuid.uuid4().hex[:8]
    st, dt = f"d168fs_{suffix}", f"d168fd_{suffix}"
    src.execute(f'CREATE TABLE "{st}" (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)')
    _insert(src, st, range(1, 61))

    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})

    real_check = cc.raise_if_job_cancelled
    calls = {"n": 0}

    def takeover_after_a_few_batches(mongo, jid, **kw):
        calls["n"] += 1
        if calls["n"] == 4:
            # Resume: a newer attempt stamps the job (cancel flag already clear).
            store.update_job_fields(jid, {"run_attempt": "newer-attempt"})
        return real_check(mongo, jid, **kw)

    monkeypatch.setattr(cc, "raise_if_job_cancelled", takeover_after_a_few_batches)
    monkeypatch.setattr(
        "services.cancel_check.raise_if_job_cancelled", takeover_after_a_few_batches
    )
    request = _request(src, dst, st, dt, "incremental_deduped")
    result = UniversalTransferEngine().execute_tracked(request, job_id)

    assert not result.success
    landed = len(dst.rows(dt))
    assert landed < 60, "the superseded attempt kept writing"
    job = store.get_job(job_id)
    assert str(job.get("status")) not in {"failed", "cancelled", "completed"}, job.get("status")
    assert job.get("run_attempt") == "newer-attempt"
