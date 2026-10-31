"""General -> Job retention: a dry-run-first, audited purge honours retention_days.

QA (production 557f556c, enterprise #4): retention_days validated >= 1 but the
UI itself said archival isn't wired; nothing ever removed an old job. Through
the real routes on the Mongo job store: dry run, apply with its token, only
finished jobs older than the window in the caller's workspace go, audited.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from r8_ent_harness import ent  # noqa: F401


def _job(ent, ws: str, *, status: str, age_days: int) -> str:
    when = datetime.now(timezone.utc) - timedelta(days=age_days)
    from services.mongodb_service import get_mongodb_service

    coll = get_mongodb_service().get_database()["transfer_jobs"]
    name = f"{ws[:8]}-{status}-{age_days}"
    job_id = get_mongodb_service().create_transfer_job({"name": name, "workspace_id": ws})
    coll.update_one({"name": name}, {"$set": {"status": status, "created_at": when,
                                               "completed_at": when if status != "running" else None}})
    return job_id


def test_dry_run_then_purge_removes_only_old_finished_jobs_in_this_workspace(ent):
    ws = ent.workspace("Retention")
    other = ent.workspace("Retention other")
    admin = ent.member(email="retention-admin-r8@example.com", workspace_id=ws, role="admin")
    assert admin.patch("/api/v1/workspace/settings", json={"retention_days": 30}).status_code == 200
    settings = admin.get("/api/v1/workspace/settings").json()
    assert settings["retention_enforcement"]["scheduled"] is False

    old_done = _job(ent, ws, status="completed", age_days=45)
    old_failed = _job(ent, ws, status="failed", age_days=90)
    old_running = _job(ent, ws, status="running", age_days=60)
    recent = _job(ent, ws, status="completed", age_days=3)
    foreign = _job(ent, other, status="completed", age_days=100)

    # Applying without a dry run is refused.
    blind = admin.post("/api/v1/workspace/jobs/retention/purge", json={"dry_run": False})
    assert blind.status_code == 409, blind.text

    dry = admin.post("/api/v1/workspace/jobs/retention/purge", json={})
    assert dry.status_code == 200, dry.text
    assert dry.json()["would_remove"] == 2
    assert set(dry.json()["sample_job_ids"]) == {old_done, old_failed}
    listed = {j.get("id") or j.get("_id") for j in admin.get("/api/v1/jobs").json()["jobs"]}
    assert {old_done, old_failed, old_running, recent} <= {str(x) for x in listed}  # dry run deletes nothing

    applied = admin.post("/api/v1/workspace/jobs/retention/purge",
                         json={"dry_run": False, "confirm_token": dry.json()["confirm_token"]})
    assert applied.status_code == 200, applied.text
    assert applied.json()["removed"] == 2
    left = {str(j.get("id") or j.get("_id")) for j in admin.get("/api/v1/jobs").json()["jobs"]}
    assert left == {old_running, recent}
    assert ent.admin.get(f"/api/v1/jobs/{foreign}", headers={"X-Workspace-Id": other}).status_code == 200

    events = ent.audit(ws, action="workspace.jobs.retention.purge")
    done = [e for e in events if e["outcome"] == "succeeded"]
    assert sorted(e["details"]["dry_run"] for e in done) == [False, True]
    assert [e["details"].get("removed") for e in done if not e["details"]["dry_run"]] == [2]
    assert any(e["outcome"] == "failed" for e in events)  # the blind apply
    assert all(e["actor"] == "retention-admin-r8@example.com" for e in events)

    viewer = ent.member(email="retention-viewer-r8@example.com", workspace_id=ws, role="viewer")
    assert viewer.post("/api/v1/workspace/jobs/retention/purge", json={}).status_code == 403
