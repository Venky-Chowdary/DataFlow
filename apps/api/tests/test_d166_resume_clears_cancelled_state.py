"""D166 — a resumed job stops reading "cancelled".

Resume moved the status out of ``cancelled`` but left the rest of the terminal
record behind: ``phase`` stayed ``cancelled``, ``error`` kept "Transfer
cancelled by user" and ``completed_at`` the finish stamp, so the resumed job
still displayed as cancelled while it ran and after it completed.
"""

from __future__ import annotations

import importlib
import os
import sqlite3
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.background as background  # noqa: E402
import src.transfer.engine as engine_mod  # noqa: E402
from services.checkpoint_service import Checkpoint  # noqa: E402
from services.mongodb_service import MemoryMongoDBService  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def test_resume_route_clears_the_cancelled_leftovers(monkeypatch):
    import services.mongodb_service as mongo_mod

    store = MemoryMongoDBService()
    router = importlib.import_module("src.routers.connectors_router")
    for module in (engine_mod, router, mongo_mod):
        monkeypatch.setattr(module, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(background, "run_transfer_async", lambda *a, **k: None)

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database="/tmp/a.db", table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database="/tmp/b.db", table="t"),
        sync_mode="full_refresh_overwrite",
    )
    job_id = UniversalTransferEngine()._create_pending_job(request)
    checkpoint = Checkpoint(
        job_id=job_id, chunk_index=2, rows_processed=2048, offset=2048,
        cursor_column="id", cursor_value="2048", write_mode="upsert", conflict_columns=["id"],
    )
    store.update_job_status(
        job_id, "running", phase="writing", progress_pct=5,
        message="Connecting to destination…", checkpoint=checkpoint.to_dict(),
        records_processed=2048,
    )
    store.request_job_cancel(job_id)
    store.update_job_status(
        job_id, "cancelled", operator_command=True, message="Cancelled",
        error="Transfer cancelled by user", phase="cancelled",
    )
    assert store.get_job(job_id)["completed_at"]

    resp = TestClient(__import__("src.main", fromlist=["app"]).app).post(
        f"/api/v1/connectors/jobs/{job_id}/resume"
    )
    assert resp.status_code == 200, resp.text
    job = store.get_job(job_id)
    assert job["status"] == "pending" and job["phase"] == "queued"
    assert not job.get("error") and not job.get("completed_at")
    assert not job.get("cancel_requested")
    assert "cancel" not in str(job.get("message")).lower()


def test_resumed_job_completes_without_cancel_residue(tmp_path: Path, monkeypatch):
    from tests.test_d134_cancel_resume_e2e import _cancel_then_resume, _insert, _request
    from tests.test_d124_watermark_tie_group_e2e import _backend
    import services.sync_cursor as sc
    import src.transfer.stream as stream_mod

    store = MemoryMongoDBService()
    import services.mongodb_service as mongo_mod

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 5)

    src, dst = _backend("sqlite", tmp_path, "src"), _backend("sqlite", tmp_path, "dst")
    suffix = uuid.uuid4().hex[:8]
    st, dt = f"d166s_{suffix}", f"d166d_{suffix}"
    src.execute(f'CREATE TABLE "{st}" (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)')
    _insert(src, st, range(1, 31))
    resumed = _cancel_then_resume(
        store, monkeypatch, _request(src, dst, st, dt, "incremental_deduped")
    )
    assert resumed.success, resumed.error
    job = [j for j in store._jobs.values() if j.get("status") == "completed"][-1]
    assert job["status"] == "completed" and job.get("phase") == "completed"
    assert not job.get("error")
    assert "cancel" not in str(job.get("message")).lower()
