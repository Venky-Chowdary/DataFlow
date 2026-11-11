"""Re-review finding 8 (MEDIUM): no process-wide lock is held while a fix is applied.

``agentic_repair.decide_proposal`` ran ``apply_fn`` — the widen ALTER, or the
re-run route waiting up to 60 s for a coroutine on the event loop — inside its
process-wide ``_LOCK``; an event-loop route taking that lock froze the
replica. And a re-run start that timed out was never cancelled, so a re-run
could start behind a fix recorded as failed. Now the write runs outside the
lock and its outcome is recorded after it; a timed-out start is cancelled.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from r8_ent_harness import ent  # noqa: F401
from services.mongodb_service import MemoryMongoDBService
from tests.test_parity_api import _job

_ROUTE = {"source": {"kind": "database", "format": "postgresql", "table": "o"},
          "destination": {"kind": "database", "format": "postgresql", "table": "o"},
          "sync_mode": "full_refresh_overwrite"}
_SHORT = {"passed": False, "source_rows": 100, "target_rows": 97, "culprit_census_clean": True}


@pytest.fixture()
def store(monkeypatch, tmp_path):
    import services.mongodb_service as mongodb_service
    from services import audit_log

    svc = MemoryMongoDBService()
    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: svc)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    return svc


def test_the_repair_lock_is_free_while_the_fix_is_applied(store):
    from services import agentic_repair
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report

    job_id = store.create_transfer_job({
        "name": "overwrite", "workspace_id": "w", "reconciliation": _SHORT, "transfer_request": _ROUTE,
        "destination_summary": {"table": "o", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite"},
    })
    store.update_job_status(job_id, "completed")
    rerun = next(f for f in build_parity_report(store.get_job(job_id), job_id=job_id)["fixes"]
                 if f["action"] == "rerun")
    assert rerun["applicable"], rerun["not_applicable_reason"]
    seen: list[bool] = []

    def start_rerun() -> str:
        def probe() -> None:
            got = agentic_repair._LOCK.acquire(timeout=1)
            if got:
                agentic_repair._LOCK.release()
            seen.append(got)

        t = threading.Thread(target=probe)
        t.start()
        t.join()
        return "child-1"

    report = approve_fix(job_id, rerun["id"], actor="ops@example.com", note="go", start_rerun=start_rerun)
    assert seen == [True], "another thread could not take the repair lock while the fix was applied"
    done = next(f for f in report["fixes"] if f["id"] == rerun["id"])
    assert done["status"] == "applied" and "child-1" in done["result"]
    [proposal] = [p for p in agentic_repair.list_proposals(job_id=job_id) if p.source == "parity"]
    assert proposal.status == "applied"


def test_a_rerun_start_that_times_out_is_cancelled_and_releases_the_claim(ent, monkeypatch):  # noqa: F811
    import importlib

    from src.routers import parity_router

    connectors_router = importlib.import_module("src.routers.connectors_router")
    finished: list[str] = []

    async def slow_rerun(job_id, background_tasks, request, force=False, body=None):
        await asyncio.sleep(1.5)
        finished.append(job_id)  # an orphan re-run would get here
        return {"success": True, "job_id": "late-child"}

    monkeypatch.setattr(connectors_router, "rerun_transfer_job", slow_rerun)
    monkeypatch.setattr(parity_router, "_RERUN_START_SECONDS", 0.2, raising=False)
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    ws = ent.workspace("Parity timeout")
    editor = ent.member(email="timeout-editor@example.com", workspace_id=ws, role="editor")
    job = _job(ws, "overwrite load", reconciliation=_SHORT, transfer_request=_ROUTE)
    report = editor.get(f"/api/v1/jobs/{job}/parity-report").json()
    rerun = next(f for f in report["fixes"] if f["action"] == "rerun")
    res = editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/{rerun['id']}/approve", json={"note": "go"})
    assert res.status_code == 409, res.text
    assert "nothing was started" in res.text
    import time

    time.sleep(2.0)
    assert finished == [], "the timed-out re-run kept running behind the fix"
    after = editor.get(f"/api/v1/jobs/{job}/parity-report").json()
    assert next(f for f in after["fixes"] if f["id"] == rerun["id"])["status"] == "proposed"
