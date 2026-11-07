"""QA round 4, D188 acceptance through the scheduler, on live SQL Server -> MySQL.

QA's acceptance: "3 scheduled runs after a silent edit all complete; the
warning is visible". Route: a saved schedule (incremental_upsert, key id,
cursor updated_at) fired by the scheduler beat ``_run_due_schedules`` ->
``_run_schedule`` -> dispatched job -> ``_finalize_run``. Not
``execute_tracked`` directly.

Run 1 loads 30 rows. Then id 7's ``note`` changes WITHOUT bumping
``updated_at`` (QA's silent edit) plus normal edits that do bump the cursor.
Three further beats must all complete, write the cursor-window rows exactly,
leave id 7 alone, and carry the out-of-window drift warning on the job record,
on the REST job route the UI reads (``GET /api/v1/connectors/jobs/{id}``), on MCP
``get_job`` and on the schedule's run history.

Needs ``DATAFLOW_TEST_MSSQL_*`` and ``DATAFLOW_TEST_MYSQL_*``; skipped otherwise.
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from test_d188_incremental_out_of_window_drift_live import (
    _MS,
    _MY,
    _MySql,
    _SqlServer,
    _exec,
    _fetch,
    _up,
)

_TERMINAL = {"completed", "success", "succeeded", "failed", "error", "cancelled", "partial"}
_DRIFT = "Out-of-window drift"


@pytest.fixture
def stores(tmp_path, monkeypatch):
    if not (_up(_MS) and _up(_MY)):
        pytest.skip("live SQL Server / MySQL not reachable")
    import services.connector_store as connectors
    import services.mongodb_service as mongo_mod
    import services.schedule_store as schedules
    import services.sync_cursor as sc
    from services.mongodb_service import MemoryMongoDBService

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8,::1/128")
    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(schedules, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedules, "_mongo_backend", lambda: None)
    monkeypatch.setattr(connectors, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connectors, "_use_mongo", lambda: False)
    monkeypatch.setattr(connectors, "_resolve_backend", lambda: "file")
    monkeypatch.setattr(connectors, "_backend_choice", "file")
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)
    return memory


def _ts(i: int, bump: int = 0) -> str:
    return f"2026-01-01 {bump:02d}:{i // 60:02d}:{i % 60:02d}"


def _seed(eng, table, lo, hi, bump=0):
    rows = []
    for i in range(lo, hi + 1):
        note = {0: None, 1: "", 2: "  ", 3: "naïve 中文 ✓"}.get(i % 7, f"note {i}")
        rows.append((i, f"name {i}", note, _ts(i, bump)))
    _exec(
        eng,
        f"INSERT INTO {eng.q(table)} (id, name, note, updated_at) VALUES "  # nosec B608
        f"({eng.ph}, {eng.ph}, {eng.ph}, {eng.ph})",
        rows,
    )


def _await_job(job_id: str, timeout: float = 240.0) -> dict:
    from services import schedule_runner

    deadline = time.time() + timeout
    doc: dict = {}
    while time.time() < deadline:
        doc = dict(schedule_runner._job_doc(job_id) or {})
        if str(doc.get("status") or "").lower() in _TERMINAL:
            return doc
        time.sleep(0.4)
    return doc


def _await_idle(schedule_id: str, timeout: float = 60.0) -> None:
    from services.schedule_store import get_schedule

    deadline = time.time() + timeout
    while time.time() < deadline:
        sched = get_schedule(schedule_id)
        if sched is not None and not str(getattr(sched, "running_job_id", "") or "").strip() \
                and not getattr(sched, "running", False):
            return
        time.sleep(0.25)
    raise TimeoutError(f"schedule {schedule_id} still marked running")


def _make_due(schedule_id: str) -> None:
    import services.schedule_store as store

    due = datetime.now(timezone.utc) - timedelta(seconds=30)
    loaded = store._load_all()
    for i, row in enumerate(loaded):
        if row.id == schedule_id:
            loaded[i] = store.PipelineSchedule.from_dict(
                {**row.to_dict(), "next_run_at": due.isoformat(), "enabled": True}
            )
    store._save_all(loaded)


def _beat(schedule_id: str) -> tuple[str, dict]:
    """One scheduler beat: the due schedule fires and its job finishes."""
    from services import schedule_runner
    from services.schedule_store import get_schedule

    before = str(getattr(get_schedule(schedule_id), "last_job_id", "") or "")
    _make_due(schedule_id)
    started = schedule_runner._run_due_schedules()
    state = get_schedule(schedule_id).to_dict()
    assert started == 1, {
        k: v for k, v in state.items()
        if any(w in k for w in ("status", "error", "message", "park", "next_run", "running", "enabled", "finding"))
    }
    deadline = time.time() + 60
    job_id = ""
    while time.time() < deadline and not job_id:
        sched = get_schedule(schedule_id)
        job_id = str(getattr(sched, "running_job_id", "") or "")
        if not job_id and str(getattr(sched, "last_job_id", "") or "") not in ("", before):
            job_id = str(sched.last_job_id)
        time.sleep(0.1)
    assert job_id, get_schedule(schedule_id).to_dict()
    job = _await_job(job_id)
    _await_idle(schedule_id)
    return job_id, job


def test_three_scheduled_runs_after_a_silent_edit_complete_with_visible_warning(stores):
    from fastapi.testclient import TestClient

    from services.connector_store import create_connector
    from services.schedule_store import create_schedule, get_schedule
    from src.ai.copilot.tools import get_pilot_tools
    from src.main import app

    src, dst = _SqlServer(), _MySql()
    table = "r4d188_" + uuid.uuid4().hex[:8]
    _exec(src, src.create.format(t=src.q(table)))
    try:
        _seed(src, table, 1, 30)
        tag = uuid.uuid4().hex[:6]
        source = create_connector({
            "name": f"r4-ms-{tag}", "type": "sqlserver", "role": "source",
            "host": _MS["host"], "port": _MS["port"], "database": _MS["database"],
            "username": _MS["username"], "password": _MS["password"],
            "extra": {"trust_server_certificate": True}, "ssl": False,
        })
        dest = create_connector({
            "name": f"r4-my-{tag}", "type": "mysql", "role": "destination",
            "host": _MY["host"], "port": _MY["port"], "database": _MY["database"],
            "username": _MY["username"], "password": _MY["password"], "ssl": False,
        })
        sched = create_schedule({
            "name": f"r4 d188 {tag}",
            "source_connector_id": source.id, "source_table": table,
            "dest_connector_id": dest.id, "dest_table": table,
            "interval": "hourly", "timezone": "UTC",
            "sync_mode": "incremental_upsert", "primary_key": "id",
            "cursor_column": "updated_at", "cursor_semantics": "modification_timestamp",
            "validation_mode": "strict", "enabled": True,
            "mappings": [
                {"source": c, "target": c, "confidence": 1.0}
                for c in ("id", "name", "note", "updated_at")
            ],
        })
        assert sched.enabled, sched.to_dict().get("enabled_note")

        _jid, first = _beat(sched.id)
        assert str(first.get("status")) == "completed", (first.get("status"), first.get("error"), first.get("message"))
        assert len(_fetch(dst, f"SELECT id FROM {dst.q(table)}")) == 30  # nosec B608

        # QA's silent edit: id 7's note changes, updated_at does not move.
        _exec(src, f"UPDATE {src.q(table)} SET note = {src.ph} WHERE id = 7", [("silent edit",)])  # nosec B608
        client = TestClient(app)
        tools = get_pilot_tools()
        for beat, (lo, hi) in enumerate([(31, 32), (33, 33), (34, 35)], start=2):
            # Normal edits that bump the cursor, plus new rows.
            _exec(
                src,
                f"UPDATE {src.q(table)} SET note = {src.ph}, updated_at = {src.ph} WHERE id = {src.ph}",  # nosec B608
                [(f"bumped {beat}", _ts(beat, bump=beat), beat + 10)],
            )
            _seed(src, table, lo, hi, bump=beat)  # new rows: a later updated_at
            job_id, job = _beat(sched.id)
            assert str(job.get("status")) == "completed", (
                beat, job.get("status"), job.get("error"), job.get("message"),
            )
            got = dict(_fetch(dst, f"SELECT id, note FROM {dst.q(table)}"))  # nosec B608
            # id 7 seeded NULL (i % 7 == 0); outside the window it stays as run 1 wrote it.
            assert got[7] is None, got[7]
            assert got[beat + 10] == f"bumped {beat}"
            assert len(got) == hi
            # The warning, where the operator reads it.
            assert _DRIFT in str(job.get("message") or ""), job.get("message")
            rest = client.get(f"/api/v1/connectors/jobs/{job_id}")
            assert rest.status_code == 200, rest.text
            assert _DRIFT in str(rest.json().get("message") or ""), rest.json().get("message")
            mcp = tools.execute("get_job", {"job_id": job_id})
            assert mcp.success, mcp.error
            assert _DRIFT in str((mcp.output or {}).get("message") or ""), mcp.output
            assert "key 7" in str((mcp.output or {}).get("message") or "")
        history = get_schedule(sched.id).to_dict()
        assert str(history.get("last_status") or "") in {"completed", "success", "succeeded"}, history
    finally:
        _exec(src, src.drop(table))
        _exec(dst, dst.drop(table))
