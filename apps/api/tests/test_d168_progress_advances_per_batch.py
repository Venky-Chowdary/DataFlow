"""D168 - the job row advances with every committed batch, not once per chunk.

QA watched a MySQL -> SQL Server job read 0 rows / 5% with a frozen
``updated_at`` while the destination table grew by 1024-row batches: the engine
reports progress when a whole chunk (thousands of rows) returns, and COPY paths
reported nothing until they finished. Every batch boundary already polls
``cancel_checkpoint``; that seam now feeds a throttled progress write.
"""

from __future__ import annotations

import functools
import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.cancel_check as cc  # noqa: E402
from services.mongodb_service import MemoryMongoDBService  # noqa: E402


def test_meter_turns_batch_boundaries_into_job_progress():
    store = MemoryMongoDBService()
    store.update_job_status("j", "running", total_rows=1000, records_processed=0)
    cc.set_progress_meter(cc.ProgressMeter(store, "j", interval=0))
    try:
        stamps = []
        for written in (100, 200, 300):  # one writer call, cumulative counts
            cc.cancel_checkpoint(written)
            job = store.get_job("j")
            stamps.append((job["records_processed"], job["updated_at"]))
        cc.cancel_checkpoint(50)  # a new writer call in the same context
        assert store.get_job("j")["records_processed"] == 350
    finally:
        cc.set_progress_meter(None)
    assert [r for r, _ in stamps] == [100, 200, 300]
    assert stamps[0][1] < stamps[1][1] < stamps[2][1]
    assert 5 < store.get_job("j")["progress_pct"] < 99


def test_copy_fetch_batches_report_rows():
    store = MemoryMongoDBService()
    store.update_job_status("j", "running", total_rows=100, records_processed=0)
    cc.set_progress_meter(cc.ProgressMeter(store, "j", interval=0))
    try:
        cc.report_rows(40)
        cc.report_rows(35)
    finally:
        cc.set_progress_meter(None)
    assert store.get_job("j")["records_processed"] == 75


def test_meter_never_moves_progress_backwards_or_touches_a_finished_job():
    store = MemoryMongoDBService()
    store.update_job_status("j", "running", total_rows=100, records_processed=90)
    cc.set_progress_meter(cc.ProgressMeter(store, "j", interval=0))
    try:
        cc.report_rows(10)
        assert store.get_job("j")["records_processed"] == 90
        store.update_job_status("j", "completed", records_processed=100)
        stamp = store.get_job("j")["updated_at"]
        cc.report_rows(500)
        assert store.get_job("j")["updated_at"] == stamp
    finally:
        cc.set_progress_meter(None)


def test_engine_row_path_advances_within_a_chunk(tmp_path: Path, monkeypatch):
    """A 300-row chunk written in 10-row sub-batches shows many intermediate
    values on the job row before the chunk returns."""
    import connectors.sqlite_writer as sqlite_writer
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
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 300)
    monkeypatch.setattr(sqlite_writer, "CHUNK_SIZE", 10)
    monkeypatch.setattr(
        cc, "ProgressMeter", functools.partial(cc.ProgressMeter, interval=0)
    )

    seen: list[int] = []
    real = store.update_job_fields

    def spy(job_id, fields, **kw):
        if "records_processed" in fields:
            seen.append(int(fields["records_processed"]))
        return real(job_id, fields, **kw)

    monkeypatch.setattr(store, "update_job_fields", spy)

    src, dst = _backend("sqlite", tmp_path, "src"), _backend("sqlite", tmp_path, "dst")
    suffix = uuid.uuid4().hex[:8]
    st, dt = f"d168ps_{suffix}", f"d168pd_{suffix}"
    src.execute(f'CREATE TABLE "{st}" (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)')
    _insert(src, st, range(1, 301))
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(
        _request(src, dst, st, dt, "incremental_deduped"), job_id
    )
    assert result.success, result.error
    intermediate = sorted({n for n in seen if 0 < n < 300})
    assert len(intermediate) >= 5, seen
    assert intermediate == sorted(intermediate)
