"""D261 — a deploy restart mid-write resumed a full-refresh overwrite job.

Prod: Kafka -> existing PG ``r8_kf2pg_txt``, ``full_refresh_overwrite``, no
primary key. The deploy restart reclaimed job 6ae989cb… with ``resume=True``
because its checkpoint had progress. The keyless resume then failed with
"Cannot resume a streaming insert without a primary key. Re-run this transfer
with Full refresh · Overwrite…" — the mode the job already had — and the job
history said 0 processed while the destination held 30 rows.

An overwrite clears and reloads, so it converges from the start, and its
checkpoint can count rows of a held write transaction the stopped worker never
committed. Reclaim restarts it from zero; append/upsert reclaim still resumes
from its checkpoint.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services.execution_engine_contract import resolve_reclaim_resume  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_orphan_job_sweep import _store, _sweep  # noqa: E402

_CHECKPOINT = {"chunk_index": 1, "rows_processed": 30, "offset": 30}


def _sweep_request(mode: str, contract_mode: str | None = None) -> dict:
    req = {
        "source": {"kind": "database", "format": "kafka", "table": "qaenv_seed"},
        "destination": {"kind": "database", "format": "postgresql", "table": "r8_kf2pg_txt"},
        "sync_mode": mode,
    }
    if contract_mode:
        req["stream_contracts"] = [
            {"name": "qaenv_seed", "sync_mode": contract_mode, "selected": True}
        ]
    return req


def _orphan(mode: str, contract_mode: str | None = None) -> dict:
    return {
        "_id": "j1",
        "status": "running",
        "phase": "load",
        "records_processed": 0,
        "checkpoint": dict(_CHECKPOINT),
        "transfer_request": _sweep_request(mode, contract_mode),
    }


def test_overwrite_orphan_is_reclaimed_from_the_start() -> None:
    store = _store(_orphan("full_refresh_overwrite"))
    out, submitted = _sweep(store)

    assert out == [{"job_id": "j1", "action": "resumed", "reason": "fresh_start"}]
    assert [(jid, resume) for jid, _req, resume in submitted] == [("j1", False)]
    assert "from the start" in store.get_job("j1")["message"]


def test_contract_overwrite_mode_wins_over_the_request_default() -> None:
    store = _store(_orphan("full_refresh_append", contract_mode="full_refresh_overwrite"))
    _out, submitted = _sweep(store)
    assert [resume for _jid, _req, resume in submitted] == [False]


def test_append_orphan_still_resumes_from_its_checkpoint() -> None:
    store = _store(_orphan("full_refresh_append"))
    out, submitted = _sweep(store)
    assert out[0]["reason"] == "checkpoint"
    assert [resume for _jid, _req, resume in submitted] == [True]


class _Mongo:
    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {}).update(kwargs)
        self.jobs[job_id]["status"] = status
        return True

    def list_jobs(self, limit: int = 50) -> list[dict]:
        return list(self.jobs.values())

    def create_transfer_job(self, job_data: dict) -> str:
        job_id = job_data.get("job_id") or uuid.uuid4().hex[:24]
        self.jobs[job_id] = dict(job_data)
        return job_id


def _db(path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
            conn.executemany("INSERT INTO t VALUES (?, ?)", rows)
    finally:
        conn.close()


def test_reclaimed_keyless_overwrite_reloads_and_counts_match_destination(
    tmp_path: Path, monkeypatch
) -> None:
    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    src, dst = tmp_path / "s.db", tmp_path / "d.db"
    _db(src, [(i, f"n{i}") for i in range(1, 31)])
    _db(dst, [(i, f"old{i}") for i in range(1, 31)])
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode="strict",
    )
    job_id = uuid.uuid4().hex[:24]
    fake.jobs[job_id] = {
        "id": job_id,
        "status": "running",
        "records_processed": 0,
        "sync_mode": "full_refresh_overwrite",
        "checkpoint": {"job_id": job_id, **_CHECKPOINT},
    }

    resume = resolve_reclaim_resume(fake.jobs[job_id], request)
    assert resume is False
    UniversalTransferEngine().execute_tracked(request, job_id, resume=resume)

    job = fake.jobs[job_id]
    assert job["status"] == "completed", job.get("error") or job.get("message")
    assert "Cannot resume a streaming insert" not in str(job.get("error") or "")
    conn = sqlite3.connect(str(dst))
    try:
        count, stale = conn.execute(
            "SELECT COUNT(*), SUM(name LIKE 'old%') FROM t"
        ).fetchone()
    finally:
        conn.close()
    assert (count, stale) == (30, 0)
    assert int(job["records_processed"]) == count
