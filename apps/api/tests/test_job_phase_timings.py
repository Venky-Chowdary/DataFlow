"""M10 — the job record and get_job carry per-stage timings.

``destination_summary.phase_profile`` already held the run's PhaseProfile but
nothing surfaced it as the extract / transform+write / verify split an
operator asks for, and Gate-8's own wall time was only an ops event.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services.mongodb_service import MemoryMongoDBService  # noqa: E402
from services.phase_profile import job_phase_timings  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def store(monkeypatch):
    svc = MemoryMongoDBService()
    import services.mongodb_service as mongo_mod

    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: svc)
    return svc


def _run(store, tmp_path: Path, *, row_path: bool):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    with sqlite3.connect(src) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"v{i}") for i in range(1, 6)])
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        source_filter={"column": "id", "op": "gt", "value": 0} if row_path else {},
    )
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})
    assert UniversalTransferEngine().execute_tracked(request, job_id).success
    return job_id


@pytest.mark.parametrize("row_path", [False, True], ids=["engine_copy", "row_path"])
def test_finished_job_record_and_get_job_carry_stage_timings(tmp_path, store, row_path):
    job_id = _run(store, tmp_path, row_path=row_path)
    timings = store.get_job(job_id)["phase_timings"]
    assert timings["verify_seconds"] >= 0, timings
    if row_path:
        assert timings["transform_write_seconds"] > 0
    else:
        assert timings["bulk_copy_seconds"] > 0
    assert timings["dominant_phase"]

    from src.ai.copilot.tools import DataPilotTools

    out = DataPilotTools()._get_job(job_id)
    assert out.success, out.error
    assert out.output["phase_timings"] == timings


def test_job_without_recorded_timings_reports_none():
    assert job_phase_timings({"status": "completed"}) is None
    assert job_phase_timings(None) is None
