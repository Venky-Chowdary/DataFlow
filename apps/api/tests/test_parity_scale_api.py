"""Parity at Scale routes on the real app and a live MongoDB (r8 enterprise harness).

Behind DATAWRAP_PARITY_SCALE (404 when off, as if absent); another
workspace's job is a 404 on every route; reading is a viewer's right,
re-verifying a table needs job.run; the explorer answers 409 until a scan of
the table has finished.
"""

from __future__ import annotations

import pytest

from r8_ent_harness import ent  # noqa: F401


def _job(workspace_id: str, name: str) -> str:
    from services.mongodb_service import get_mongodb_service

    store = get_mongodb_service()
    job_id = store.create_transfer_job({
        "name": name, "workspace_id": workspace_id, "status": "completed",
        "reconciliation": {"passed": True, "source_rows": 10, "target_rows": 10,
                           "verification_verdict_qualifier": "coverage_limited"},
        "destination_summary": {"table": "payments", "primary_key_columns": ["id"]},
        "parity_scale": {"state": "running", "started_at": "2026-11-21T00:00:00+00:00",
                         "tables": [{"name": "payments", "status": "running", "scan_id": "s1",
                                     "progress": {"items_done": 2, "items_queued": 6}}]},
    })
    store.update_job_status(job_id, "completed")
    return job_id


@pytest.fixture()
def world(ent, monkeypatch):
    monkeypatch.setenv("DATAWRAP_PARITY_SCALE", "1")
    ws, other = ent.workspace("Scale"), ent.workspace("Scale elsewhere")
    return {
        "job": _job(ws, "payments"), "foreign": _job(other, "theirs"),
        "viewer": ent.member(email="scale-viewer@example.com", workspace_id=ws, role="viewer"),
        "editor": ent.member(email="scale-editor@example.com", workspace_id=ws, role="editor"),
    }


def test_verification_state_isolation_and_flag(world, monkeypatch) -> None:
    viewer, job, foreign = world["viewer"], world["job"], world["foreign"]
    res = viewer.get(f"/api/v1/jobs/{job}/verification")
    assert res.status_code == 200, res.text
    assert res.json()["state"] == "running"
    assert res.json()["tables"][0]["progress"]["items_done"] == 2
    assert viewer.get(f"/api/v1/jobs/{foreign}/verification").status_code == 404
    report = viewer.get(f"/api/v1/jobs/{job}/parity-report").json()
    assert report["verdict"] == "verifying"
    assert report["signoff"]["status"] == "blocked"
    monkeypatch.delenv("DATAWRAP_PARITY_SCALE")
    assert viewer.get(f"/api/v1/jobs/{job}/verification").status_code == 404


def test_explorer_waits_for_a_finished_scan_and_reverify_needs_job_run(world) -> None:
    viewer, editor, job = world["viewer"], world["editor"], world["job"]
    for tab in ("columns", "keys", "values", "repair-sql"):
        res = viewer.get(f"/api/v1/jobs/{job}/verification/tables/payments/{tab}")
        assert res.status_code == 409, (tab, res.text)
    assert viewer.post(f"/api/v1/jobs/{job}/verification/tables/payments").status_code == 403
    # Already running: a second start is refused rather than doubled.
    assert editor.post(f"/api/v1/jobs/{job}/verification/tables/payments").status_code == 409
    assert editor.post(f"/api/v1/jobs/{world['foreign']}/verification/tables/payments").status_code == 404
