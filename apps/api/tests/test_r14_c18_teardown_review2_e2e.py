"""C18 second review: the workspace teardown is closed, complete, confirmed and strictly scoped.

* S3 — teardown was check-then-act: a beat claim or a member's transfer could
  start a job between the active-jobs count and the delete. The workspace is
  now tombstoned first; writes answer 409, job creation and schedule claims are
  refused, and active jobs are re-checked before the workspace row goes.
* S5 — uploads, JSONL quarantine rows, watermarks, job checkpoints, SCIM
  mappings, "last used" pointers and quality profiles (real values in
  ``top_values``) survived. Each is removed, matched exactly on the target.
* S6 — an irreversible wipe needs ``?dry_run=true`` to preview and
  ``confirm=<name>`` or a ``reason`` to apply.
* Scope — teardown runs in system scope, so every delete must name the
  target: a third workspace C holding the same kinds of rows stays intact.

Live MongoDB through the real routes (``r8_ent_harness``), synthetic
workspaces only.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pytest

from r8_ent_harness import ent  # noqa: F401
from r14_sandbox_world import provision

REASON = {"reason": "teardown review test"}


def _job_q(db, job_id: str) -> dict:
    """The job's stored ``_id`` (an ObjectId on Mongo, a string elsewhere)."""
    from bson import ObjectId

    if ObjectId.is_valid(job_id) and db["transfer_jobs"].count_documents({"_id": ObjectId(job_id)}):
        return {"_id": ObjectId(job_id)}
    return {"_id": job_id}


def _seed_everything(ent, workspace_id: str, *, tag: str) -> dict:
    """One of every workspace-scoped row the cascade owns, stamped with ``workspace_id``."""
    from services import (
        connector_store,
        file_parser,
        integrations_store,
        notification_store,
        quarantine_dlq,
        scim_store,
        sync_cursor,
        tenant_store,
        workspace_preference,
    )
    from services.data_quality_history import _profile_collection
    from services.mongodb_service import get_mongodb_service
    from services.schedule_store import PipelineSchedule, _load_all, _save_all
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter

    svc = get_mongodb_service()
    db = svc.get_database()
    conn = connector_store.create_connector({
        "name": f"{tag} pg", "type": "postgresql", "host": f"{tag}.invalid", "port": 5432,
        "username": "qa", "password": f"Secret-{tag}", "database": "qa", "workspace_id": workspace_id,
    })
    sched = PipelineSchedule(
        id=f"{tag}-sched-{uuid.uuid4().hex[:6]}", name=f"{tag} nightly", source_connector_id=conn.id,
        source_table="t", dest_connector_id=conn.id, dest_table="t_out", interval="hourly",
        enabled=True, workspace_id=workspace_id, mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
    )
    _save_all(_load_all() + [sched])
    job_id = svc.create_transfer_job({"name": f"{tag}-job", "workspace_id": workspace_id})
    db["transfer_jobs"].update_one(_job_q(db, str(job_id)), {"$set": {
        "status": "completed", "completed_at": datetime.now(timezone.utc), "cursor_key": f"{tag}-cursor"}})
    db["job_checkpoints"].insert_one({"job_id": str(job_id), "checkpoint": {"rows": 5}})
    sync_cursor.set_watermark(f"{tag}-cursor", "42", metadata={"job_id": str(job_id)})
    _profile_collection().insert_one({
        "_id": f"{tag}-profile", "job_id": str(job_id),
        "runs": [{"job_id": str(job_id), "columns": {"email": {"top_values": [f"alice@{tag}.test"]}}}],
    })
    quarantine_dlq.append_dlq_event(job_id=str(job_id), action="quarantine", rows=1, workspace_id=workspace_id)
    # The JSONL fallback holds full row bodies.
    with quarantine_dlq.DLQ_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"job_id": str(job_id), "workspace_id": workspace_id,
                             "row": {"ssn": f"{tag}-123-45-6789"}}) + "\n")
    with bound_workspace_filter(WorkspaceFilter.only(workspace_id), stamp=workspace_id):
        upload = file_parser.store_upload(f"{tag}.csv", b"id,email\n1,a@x.test\n")
    tenant_store.create_tenant(workspace_id=workspace_id, name=f"{tag} tenant")
    notification_store.create_channel(workspace_id=workspace_id, kind="webhook", label=f"{tag} hook",
                                      config={"url": "https://example.com/h"})
    key = integrations_store.create_api_key(f"{tag} key", "root-admin@example.com", role="viewer",
                                            workspace_id=workspace_id)
    mappings = scim_store.get_group_mappings()
    mappings[f"{tag}-group"] = {"workspace_id": workspace_id, "role": "editor"}
    scim_store.set_group_mappings(mappings)
    workspace_preference.remember_workspace(f"pref-{tag}@example.test", workspace_id)
    return {"conn": conn.id, "sched": sched.id, "job": str(job_id), "upload": upload["file_id"],
            "upload_path": upload["path"], "key": key["key"], "key_id": key["id"], "tag": tag}


def _present(ent, ws: str, seeded: dict) -> dict[str, bool]:
    from services import (
        connector_store, file_parser, notification_store, quarantine_dlq, scim_store,
        sync_cursor, tenant_store, workspace_preference,
    )
    from services.data_quality_history import _profile_collection
    from services.mongodb_service import get_mongodb_service
    from services.schedule_store import get_schedule
    from pathlib import Path

    db = get_mongodb_service().get_database()
    tag = seeded["tag"]
    jsonl = quarantine_dlq.DLQ_PATH.read_text(encoding="utf-8") if quarantine_dlq.DLQ_PATH.exists() else ""
    return {
        "connector": connector_store.get_connector(seeded["conn"]) is not None,
        "schedule": get_schedule(seeded["sched"]) is not None,
        "job": db["transfer_jobs"].count_documents(_job_q(db, seeded["job"])) == 1,
        "checkpoint": db["job_checkpoints"].count_documents({"job_id": seeded["job"]}) == 1,
        "watermark": sync_cursor.get_watermark(f"{tag}-cursor") is not None,
        "profile": _profile_collection().count_documents({"_id": f"{tag}-profile"}) == 1,
        "dlq_mongo": quarantine_dlq.count_dlq_events(workspace_id=ws) > 0
        if hasattr(quarantine_dlq, "count_dlq_events") else True,
        "dlq_jsonl": f"{tag}-123-45-6789" in jsonl,
        "upload_row": file_parser.get_file(seeded["upload"]) is not None,
        "upload_file": Path(seeded["upload_path"]).exists(),
        "tenant": tenant_store.get_tenant_for_workspace(ws) is not None,
        "channel": bool(notification_store.list_channels(workspace_id=ws)),
        "scim_mapping": f"{tag}-group" in scim_store.get_group_mappings(),
        "preference": workspace_preference.last_used_workspace(f"pref-{tag}@example.test") == ws,
        "key": ent.bearer(seeded["key"]).get("/api/v1/connectors/saved").status_code != 401,
    }


def test_teardown_removes_every_scoped_row_and_a_third_workspace_stays_intact(ent):
    box = provision(ent)
    other = ent.workspace("Workspace C — must stay intact")
    target_rows = _seed_everything(ent, box.workspace_id, tag=f"b{uuid.uuid4().hex[:6]}")
    c_rows = _seed_everything(ent, other, tag=f"c{uuid.uuid4().hex[:6]}")
    assert all(_present(ent, box.workspace_id, target_rows).values())
    before_c = _present(ent, other, c_rows)
    assert all(before_c.values()), before_c

    gone = ent.admin.delete(f"/api/v1/team/workspaces/{box.workspace_id}", params=REASON,
                            headers={"X-Workspace-Id": other})
    assert gone.status_code == 200, gone.text
    body = gone.json()
    assert body["retained"] and any("audit" in r for r in body["retained"])
    left = {k: v for k, v in _present(ent, box.workspace_id, target_rows).items() if v}
    assert left == {}, f"left behind in the deleted workspace: {left}"
    after_c = _present(ent, other, c_rows)
    assert after_c == before_c, f"workspace C was touched: {after_c}"


def test_dry_run_counts_and_changes_nothing_and_the_real_delete_needs_confirmation(ent):
    box = provision(ent)
    rows = _seed_everything(ent, box.workspace_id, tag=f"d{uuid.uuid4().hex[:6]}")
    preview = ent.admin.delete(f"/api/v1/team/workspaces/{box.workspace_id}", params={"dry_run": "true"})
    assert preview.status_code == 200, preview.text
    counts = preview.json()
    assert counts["dry_run"] is True and counts["workspace_removed"] is False
    for name in ("connectors_removed", "schedules_removed", "jobs_removed", "uploads_removed",
                 "api_keys_revoked", "tenants_removed", "channels_removed", "scim_mappings_removed"):
        assert counts[name] >= 1, name
    assert all(_present(ent, box.workspace_id, rows).values()), "a dry run changed something"
    from services import team_store

    assert not team_store.get_workspace(box.workspace_id).deleting_at

    refused = ent.admin.delete(f"/api/v1/team/workspaces/{box.workspace_id}")
    assert refused.status_code == 422, refused.text
    wrong = ent.admin.delete(f"/api/v1/team/workspaces/{box.workspace_id}", params={"confirm": "nope"})
    assert wrong.status_code == 422
    name = team_store.get_workspace(box.workspace_id).name
    ok = ent.admin.delete(f"/api/v1/team/workspaces/{box.workspace_id}", params={"confirm": name})
    assert ok.status_code == 200, ok.text
    sandbox_refused = provision(ent)
    assert ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{sandbox_refused.workspace_id}").status_code == 422
    assert ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{sandbox_refused.workspace_id}",
                            params={"dry_run": "true"}).json()["dry_run"] is True


def test_a_closing_workspace_refuses_writes_jobs_and_schedule_claims(ent, monkeypatch):
    """S3: a beat claim and a member POST during the teardown are refused."""
    from services import team_store, workspace_teardown
    from services.mongodb_service import get_mongodb_service
    from services.schedule_store import PipelineSchedule, _load_all, _save_all, mark_schedule_running

    box = provision(ent)
    sched = PipelineSchedule(
        id=f"s3-{uuid.uuid4().hex[:6]}", name="s3 nightly", source_connector_id="c", source_table="t",
        dest_connector_id="c", dest_table="t_out", interval="hourly", enabled=True,
        workspace_id=box.workspace_id, next_run_at="2000-01-01T00:00:00+00:00",
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
    )
    _save_all(_load_all() + [sched])
    seen: dict[str, object] = {}
    real_api_keys = workspace_teardown._api_keys

    def mid_teardown(workspace_id, dry_run):
        # Right after the tombstone, before any authority is revoked.
        seen["closing"] = team_store.workspace_closing(workspace_id)
        seen["post"] = box.tester.post(
            "/api/v1/connectors/saved",
            json={"name": "late", "type": "postgresql", "host": "h.invalid", "port": 5432,
                  "username": "u", "password": "p", "database": "d"},
        ).status_code
        seen["claim"] = mark_schedule_running(sched.id, "beat-1")
        try:
            get_mongodb_service().create_transfer_job({"name": "late", "workspace_id": workspace_id})
            seen["job"] = "created"
        except team_store.WorkspaceClosing:
            seen["job"] = "refused"
        return real_api_keys(workspace_id, dry_run)

    monkeypatch.setattr(workspace_teardown, "_api_keys", mid_teardown)
    gone = ent.admin.delete(f"/api/v1/team/workspaces/{box.workspace_id}", params=REASON)
    assert gone.status_code == 200, gone.text
    assert seen == {"closing": True, "post": 409, "claim": None, "job": "refused"}, seen


def test_a_job_that_slips_in_is_caught_by_the_recheck(ent, monkeypatch):
    from services import team_store, workspace_teardown
    from services.mongodb_service import get_mongodb_service

    box = provision(ent)
    svc = get_mongodb_service()
    real_connectors = workspace_teardown._connectors

    def sneak(workspace_id, dry_run):
        # A worker that bypassed the guard (an older replica) marks a job active.
        svc.get_database()["transfer_jobs"].insert_one(
            {"_id": f"sneak-{uuid.uuid4().hex[:6]}", "workspace_id": workspace_id, "status": "running"})
        return real_connectors(workspace_id, dry_run)

    monkeypatch.setattr(workspace_teardown, "_connectors", sneak)
    busy = ent.admin.delete(f"/api/v1/team/workspaces/{box.workspace_id}", params=REASON)
    assert busy.status_code == 409, busy.text
    ws = team_store.get_workspace(box.workspace_id)
    assert ws is not None and ws.deleting_at, "the workspace must stay closed, not reopen"


def test_unknown_job_store_refuses_the_teardown(ent, monkeypatch):
    from services import team_store
    from services.mongodb_service import get_mongodb_service

    box = provision(ent)
    svc = get_mongodb_service()

    def broken(*a, **k):
        raise RuntimeError("job store down")

    monkeypatch.setattr(type(svc), "count_jobs", broken)
    busy = ent.admin.delete(f"/api/v1/team/workspaces/{box.workspace_id}", params=REASON)
    assert busy.status_code == 409, busy.text
    assert "cannot say" in busy.json()["detail"]
    assert team_store.get_workspace(box.workspace_id).deleting_at == ""


def test_logins_the_tester_created_inside_the_sandbox_are_removed(ent):
    from services import user_store

    box = provision(ent)
    made = box.tester.post(f"/api/v1/team/workspaces/{box.workspace_id}/members",
                           json={"email": "made-by-tester@example.test", "role": "viewer", "create_account": True})
    assert made.status_code == 200, made.text
    assert user_store.get_user("made-by-tester@example.test") is not None
    gone = ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{box.workspace_id}", params=REASON)
    assert gone.status_code == 200, gone.text
    assert gone.json()["tester_created_logins_removed"] == 1
    assert user_store.get_user("made-by-tester@example.test") is None


def test_a_tester_login_that_became_a_real_account_is_kept(ent):
    """Review 3: a tester later added to a real workspace was still deleted."""
    from services import team_store, user_store

    box = provision(ent)
    real = ent.workspace("Real team")
    team_store._write_membership(team_store.Membership(
        workspace_id=real, email=box.tester_email, role="editor", added_by="root-admin@example.com"))
    gone = ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{box.workspace_id}", params=REASON)
    assert gone.status_code == 200, gone.text
    assert gone.json()["tester_login_removed"] is False and gone.json()["tester_login_kept"] is True
    assert user_store.get_user(box.tester_email) is not None
    assert team_store.get_workspace_role(workspace_id=real, email=box.tester_email) == "editor"
    assert [e for e in ent.audit(action="team.sandbox.tester_login_kept")]


def test_a_session_for_a_deleted_account_is_refused(ent):
    """Review 3: the middleware accepted a v2 token for a missing account as a viewer."""
    from services import user_store

    box = provision(ent)
    assert box.tester.get("/api/v1/auth/me").status_code == 200
    # Delete the account WITHOUT revoking its sessions (a race with the revoke).
    assert user_store.delete_user(email=box.tester_email)
    assert box.tester.get("/api/v1/auth/me").status_code == 401
    assert box.tester.get("/api/v1/workspace/settings").status_code == 401


def test_create_account_false_is_refused(ent):
    res = ent.admin.post("/api/v1/team/sandbox-workspaces", json={
        "tester_email": "nf-c18@example.test", "reason": "r", "create_account": False})
    assert res.status_code == 400, res.text


def test_rollback_when_the_fingerprint_write_fails_still_removes_the_minted_login(ent, monkeypatch):
    from services import sandbox_workspaces, team_store, user_store

    real_write = team_store._write_workspace
    calls = {"n": 0}

    def flaky(ws):
        calls["n"] += 1
        if getattr(ws, "sandbox_tester_created_at", ""):
            raise RuntimeError("workspace store down")
        return real_write(ws)

    monkeypatch.setattr(team_store, "_write_workspace", flaky)
    with pytest.raises(RuntimeError):
        sandbox_workspaces.create_sandbox(
            name="flaky", tester_email="flaky-c18@example.test", reason="r", actor="root-admin@example.com")
    assert user_store.get_user("flaky-c18@example.test") is None
