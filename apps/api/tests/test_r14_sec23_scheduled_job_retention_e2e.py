"""Security 23 (QA r14): retention_days purges on a schedule — only when turned on.

QA (Oct 30): Settings -> General said archival was not wired up. Round 8 made
``retention_days`` an admin-run, dry-run-first purge; nothing ran by itself.
Now the same purge runs daily for every workspace once a platform admin sets
``retention_auto_purge`` (off by default), audited per run and per workspace,
with exactly the setting's stated scope: finished jobs older than the window.
Through the real routes and the real scheduler step on the live MongoDB.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from r8_ent_harness import ent  # noqa: F401


def _job(ws: str, *, status: str, age_days: int, now: datetime) -> str:
    from services.mongodb_service import get_mongodb_service

    when = now - timedelta(days=age_days)
    svc = get_mongodb_service()
    name = f"{ws[:8]}-{status}-{age_days}"
    job_id = svc.create_transfer_job({"name": name, "workspace_id": ws})
    svc.get_database()["transfer_jobs"].update_one(
        {"name": name},
        {"$set": {"status": status, "created_at": when,
                  "completed_at": when if status not in ("running", "queued") else None}},
    )
    return job_id


def _ids(ws: str) -> set[str]:
    from services.mongodb_service import get_mongodb_service

    coll = get_mongodb_service().get_database()["transfer_jobs"]
    return {str(d.get("id") or d["_id"]) for d in coll.find({"workspace_id": ws})}


def test_off_by_default_nothing_is_deleted(ent):
    from services.job_retention_schedule import run_due_retention

    ws = ent.workspace("Retention default")
    now = datetime.now(timezone.utc)
    old = _job(ws, status="completed", age_days=400, now=now)
    settings = ent.admin.get("/api/v1/workspace/settings", headers={"X-Workspace-Id": ws}).json()
    assert settings["retention_auto_purge"] is False
    assert settings["retention_enforcement"]["scheduled"] is False
    assert settings["retention_enforcement"]["mode"] == "admin_purge"
    assert run_due_retention(now=now, force=True) is None
    assert old in _ids(ws)


def test_scheduled_purge_removes_only_old_finished_jobs_audited_per_workspace(ent):
    from services.job_retention_schedule import run_due_retention

    ws = ent.workspace("Retention scheduled")
    other = ent.workspace("Retention scheduled other")
    now = datetime.now(timezone.utc)
    old_done = _job(ws, status="completed", age_days=45, now=now)
    old_failed = _job(ws, status="failed", age_days=90, now=now)
    old_running = _job(ws, status="running", age_days=60, now=now)
    old_queued = _job(ws, status="queued", age_days=60, now=now)
    recent = _job(ws, status="completed", age_days=3, now=now)
    other_old = _job(other, status="cancelled", age_days=100, now=now)
    other_recent = _job(other, status="completed", age_days=1, now=now)

    # Only a platform admin turns it on (same deployment-wide document).
    admin = ent.member(email="ret-ws-admin-r14@example.com", workspace_id=ws, role="admin")
    assert admin.patch("/api/v1/workspace/settings",
                       json={"retention_auto_purge": True}).status_code == 403
    on = ent.admin.patch("/api/v1/workspace/settings",
                         json={"retention_days": 30, "retention_auto_purge": True},
                         headers={"X-Workspace-Id": ws})
    assert on.status_code == 200, on.text
    assert on.json()["retention_auto_purge"] is True
    status = admin.get("/api/v1/workspace/settings").json()["retention_enforcement"]
    assert status["scheduled"] is True and status["mode"] == "scheduled_purge"

    result = run_due_retention(now=now)
    assert result is not None and result["retention_days"] == 30
    assert _ids(ws) == {old_running, old_queued, recent}
    assert _ids(other) == {other_recent}
    assert old_done not in _ids(ws) and old_failed not in _ids(ws) and other_old not in _ids(other)

    for workspace, removed in ((ws, 2), (other, 1)):
        events = [e for e in ent.audit(workspace, action="workspace.jobs.retention.purge")
                  if e["details"].get("mode") == "scheduled"]
        assert len(events) == 1, events
        assert events[0]["actor"] == "system:retention"
        assert events[0]["details"]["removed"] == removed
        assert events[0]["outcome"] == "succeeded"

    # Once a day per deployment: a second beat (or replica) the same day does nothing.
    assert run_due_retention(now=now + timedelta(hours=1)) is None
    later = run_due_retention(now=now + timedelta(hours=25))
    assert later is not None
    assert all(r["removed"] == 0 for r in later["workspaces"])


def test_turning_it_off_stops_the_schedule(ent):
    from services.job_retention_schedule import run_due_retention

    ws = ent.workspace("Retention off again")
    now = datetime.now(timezone.utc)
    assert ent.admin.patch("/api/v1/workspace/settings", json={"retention_auto_purge": True},
                           headers={"X-Workspace-Id": ws}).status_code == 200
    assert ent.admin.patch("/api/v1/workspace/settings", json={"retention_auto_purge": False},
                           headers={"X-Workspace-Id": ws}).status_code == 200
    old = _job(ws, status="completed", age_days=400, now=now)
    assert run_due_retention(now=now, force=True) is None
    assert old in _ids(ws)


def test_the_scheduler_beat_runs_the_maintenance_steps(monkeypatch):
    import asyncio

    from services import job_retention_schedule, sandbox_workspaces, schedule_runner

    calls: list[str] = []
    monkeypatch.setattr(job_retention_schedule, "run_due_retention", lambda: calls.append("retention"))

    def boom():
        calls.append("sandbox")
        raise RuntimeError("store down")

    monkeypatch.setattr(sandbox_workspaces, "sweep_expired_sandboxes", boom)
    asyncio.run(schedule_runner._run_maintenance())  # a failing step never raises
    assert calls == ["retention", "sandbox"]


def test_without_a_durable_store_the_setting_refuses_and_a_run_degrades_once(ent, monkeypatch, caplog):
    """Review nit: on a file-backed deployment run_due_retention raised every beat."""
    import logging

    from services import job_retention_schedule, maintenance_claim

    ws = ent.workspace("Retention no store")
    monkeypatch.setattr(maintenance_claim, "durable_store", lambda: None)
    refused = ent.admin.patch("/api/v1/workspace/settings", json={"retention_auto_purge": True},
                              headers={"X-Workspace-Id": ws})
    assert refused.status_code == 422, refused.text
    assert "MongoDB" in refused.json()["detail"]
    assert ent.admin.get("/api/v1/workspace/settings").json()["retention_auto_purge"] is False

    # Turned on earlier (store since gone): every beat returns None, one warning.
    from services.workspace_store import update_workspace_settings

    update_workspace_settings(retention_auto_purge=True, actor="test")
    job_retention_schedule._last_warned.clear()
    with caplog.at_level(logging.WARNING, logger="services.job_retention_schedule"):
        assert job_retention_schedule.run_due_retention(force=True) is None
        assert job_retention_schedule.run_due_retention(force=True) is None
    warnings = [r for r in caplog.records if "no shared metadata database" in r.getMessage()]
    assert len(warnings) == 1
    status = ent.admin.get("/api/v1/workspace/settings").json()["retention_enforcement"]
    assert status["scheduled"] is True and "warning" in status


def test_last_run_reports_a_finished_run_not_a_bare_claim(ent):
    """Review nit: a claim alone made last_run_at report a run that never finished."""
    from services import maintenance_claim
    from services.job_retention_schedule import CLAIM_ID, RUN_INTERVAL, run_due_retention

    ws = ent.workspace("Retention completion")
    assert ent.admin.patch("/api/v1/workspace/settings", json={"retention_auto_purge": True},
                           headers={"X-Workspace-Id": ws}).status_code == 200
    # A replica claimed the period and crashed before finishing.
    assert maintenance_claim.claim_run(CLAIM_ID, RUN_INTERVAL) is True
    status = ent.admin.get("/api/v1/workspace/settings").json()["retention_enforcement"]
    assert status["last_claimed_at"] and status["last_run_at"] is None
    assert run_due_retention(force=True) is not None
    status = ent.admin.get("/api/v1/workspace/settings").json()["retention_enforcement"]
    assert status["last_run_at"]
