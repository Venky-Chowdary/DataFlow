"""QA MX3-24 — a run that ends failed / needs_approval must not wedge the claim.

Live in-process SQLite→SQLite schedule. Run 1 completes; the source then grows
an unmapped column so run 2 is refused and the next beat parks the schedule on
one finding (``needs_approval``). The claim is released every time, the next
cadence boundary is in the future, the beat never reports "already in
progress", and an operator Run now is still accepted.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timezone


def _isolate(tmp_path, monkeypatch):
    import services.connector_store as connectors
    import services.schedule_store as schedules

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(schedules, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedules, "_mongo_backend", lambda: None)
    monkeypatch.setattr(connectors, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connectors, "_use_mongo", lambda: False)
    monkeypatch.setattr(connectors, "_resolve_backend", lambda: "file")
    monkeypatch.setattr(connectors, "_backend_choice", "file")


def _wait(job_id: str) -> dict:
    from services import schedule_runner

    doc: dict = {}
    for _ in range(400):
        doc = dict(schedule_runner._job_doc(job_id) or {})
        if str(doc.get("status") or "").lower() in {
            "completed", "completed_with_quarantine", "failed", "cancelled",
        }:
            return doc
        time.sleep(0.25)
    return doc


def test_failed_then_parked_schedule_releases_claim_and_keeps_cadence(
    tmp_path, monkeypatch, caplog
):
    _isolate(tmp_path, monkeypatch)
    from services import schedule_runner
    from services import schedule_store as store
    from services.connector_store import create_connector
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down

    ensure_memory_job_store_if_mongo_down()
    src_db, dst_db = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    with sqlite3.connect(src_db) as c:
        c.execute('CREATE TABLE "QA_sch_src" (id INTEGER PRIMARY KEY, label TEXT)')
        c.executemany('INSERT INTO "QA_sch_src" VALUES (?,?)', [(i, f"l{i}") for i in range(10)])
    src = create_connector({"name": "S", "type": "sqlite", "role": "source",
                            "connection_string": f"sqlite:///{src_db}", "workspace_id": ""})
    dst = create_connector({"name": "D", "type": "sqlite", "role": "destination",
                            "connection_string": f"sqlite:///{dst_db}", "workspace_id": ""})
    sched = store.create_schedule({
        "name": "QA_sched1", "source_connector_id": src.id, "source_table": "QA_sch_src",
        "dest_connector_id": dst.id, "dest_table": "QA_sch_dst", "cron": "*/5 * * * *",
        "timezone": "UTC", "sync_mode": "full_refresh_overwrite", "primary_key": "id",
        "enabled": True, "max_retries": 0, "retry_backoff_seconds": 0,
        "mappings": [{"source": "id", "target": "id", "confidence": 1.0},
                     {"source": "label", "target": "label", "confidence": 1.0}],
    })

    def released_with_future_slot():
        cur = store.get_schedule(sched.id)
        assert cur.running is False and not cur.running_job_id
        nxt = datetime.fromisoformat(str(cur.next_run_at).replace("Z", "+00:00"))
        assert nxt > datetime.now(timezone.utc), cur.next_run_at
        return cur

    first = _wait(schedule_runner._run_schedule(sched.id, manual=True))
    assert first.get("status") == "completed", first.get("error")
    time.sleep(0.5)
    released_with_future_slot()

    with sqlite3.connect(src_db) as c:
        c.execute('ALTER TABLE "QA_sch_src" ADD COLUMN extra REAL')
    second = _wait(schedule_runner._run_schedule(sched.id, manual=True))
    assert second.get("status") == "failed"
    time.sleep(0.5)
    released_with_future_slot()

    store.update_schedule(sched.id, {"next_run_at": "2020-01-01T00:00:00+00:00"})
    with caplog.at_level(logging.INFO, logger="services.schedule_runner"):
        assert schedule_runner._run_due_schedules() == 1
        time.sleep(1.5)
        schedule_runner._run_due_schedules()
    parked = released_with_future_slot()
    assert parked.last_status == "needs_approval"
    assert store.has_open_approval(parked)
    assert "already in progress" not in caplog.text
    assert sched.id not in {s.id for s in store.due_schedules()}

    third = schedule_runner._run_schedule(sched.id, manual=True)
    assert third, "Run now on a parked schedule was refused as in progress"
    _wait(third)
