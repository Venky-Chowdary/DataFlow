"""Re-review finding 7 (MEDIUM): a re-run fix is offered only where the re-run route starts one.

The ``rerun`` fix was applicable on jobs ``POST /connectors/jobs/{id}/rerun``
refuses (a failed job, an append that committed rows); each refusal burned a
write round (three approvals settled "failed", contradicting contract item 17)
and the UI toasted "Fix applied and re-checked" for any 200. The old API test
replaced the route with a stub, so no refusal was ever exercised: these run
the real route (only the transfer scheduler is captured, so no transfer runs).
"""

from __future__ import annotations

import pytest

from r8_ent_harness import ent  # noqa: F401
from tests.test_parity_api import _job

_ROUTE = {"source": {"kind": "database", "format": "postgresql", "table": "o"},
          "destination": {"kind": "database", "format": "postgresql", "table": "o"}}
_SHORT = {"passed": False, "source_rows": 100, "target_rows": 97, "culprit_census_clean": True}
#: The run committed 97 rows (the job shell's ``records_processed`` is seeded to 0 on create).
_WROTE = {"table": "payments", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite",
          "rows_written": 97}


@pytest.fixture()
def editor_ws(ent, monkeypatch):  # noqa: F811
    import src.transfer.background as background

    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    scheduled: list[str] = []
    monkeypatch.setattr(background, "run_transfer_async",
                        lambda job_id, req, resume=False: scheduled.append(job_id))
    ws = ent.workspace("Parity rerun")
    editor = ent.member(email="rerun-editor@example.com", workspace_id=ws, role="editor")
    return ent, ws, editor, scheduled


def _rerun_fix(report: dict) -> dict:
    return next(f for f in report["fixes"] if f["action"] == "rerun" and f["status"] == "proposed")


def _writes_used(report: dict) -> int:
    return sum(1 for f in report["fixes"]
               if f["action"] in {"widen_column", "resync_keys", "rerun"} and f["status"] in {"applied", "failed", "applying"})


def test_a_failed_job_offers_no_rerun_and_burns_no_round(editor_ws):
    _ent, ws, editor, _scheduled = editor_ws
    job = _job(ws, "failed load", status="failed", reconciliation=_SHORT,
               transfer_request={**_ROUTE, "sync_mode": "full_refresh_overwrite"})
    report = editor.get(f"/api/v1/jobs/{job}/parity-report").json()
    rerun = _rerun_fix(report)
    assert rerun["applicable"] is False
    assert "failed job uses retry" in rerun["not_applicable_reason"]
    for _ in range(3):
        res = editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/{rerun['id']}/approve", json={"note": "go"})
        assert res.status_code == 409, res.text
    assert _writes_used(editor.get(f"/api/v1/jobs/{job}/parity-report").json()) == 0


def test_an_append_that_committed_rows_offers_no_rerun(editor_ws):
    _ent, ws, editor, _scheduled = editor_ws
    job = _job(ws, "append load", reconciliation=_SHORT, destination_summary=_WROTE,
               transfer_request={**_ROUTE, "sync_mode": "full_refresh_append"})
    rerun = _rerun_fix(editor.get(f"/api/v1/jobs/{job}/parity-report").json())
    assert rerun["applicable"] is False and "duplicate" in rerun["not_applicable_reason"].lower()


def test_an_allowed_rerun_runs_the_real_route(editor_ws):
    ent_, ws, editor, scheduled = editor_ws
    from services.mongodb_service import get_mongodb_service

    job = _job(ws, "overwrite load", reconciliation=_SHORT, records_processed=97,
               transfer_request={**_ROUTE, "sync_mode": "full_refresh_overwrite"})
    report = editor.get(f"/api/v1/jobs/{job}/parity-report").json()
    rerun = _rerun_fix(report)
    assert rerun["applicable"], rerun["not_applicable_reason"]
    res = editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/{rerun['id']}/approve", json={"note": "go"})
    assert res.status_code == 200, res.text
    done = next(f for f in res.json()["fixes"] if f["id"] == rerun["id"])
    assert done["status"] == "applied"
    [child] = scheduled  # the route scheduled exactly one new job
    assert child in done["result"]
    stored = get_mongodb_service().get_job(child)
    assert stored and stored.get("rerun_of") == job
    assert res.json()["rounds"] == []  # a re-run is checked by its own report, not a round here


def test_a_route_refusal_releases_the_claim(editor_ws):
    ent_, ws, editor, scheduled = editor_ws
    # A file source whose upload is gone: the decision allows it, the route refuses (400).
    job = _job(ws, "file load", reconciliation=_SHORT, records_processed=3, transfer_request={
        "source": {"kind": "file", "format": "csv", "file_name": "gone.csv"},
        "destination": {"kind": "database", "format": "postgresql", "table": "o"},
        "sync_mode": "full_refresh_overwrite",
    })
    rerun = _rerun_fix(editor.get(f"/api/v1/jobs/{job}/parity-report").json())
    assert rerun["applicable"], rerun["not_applicable_reason"]
    res = editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/{rerun['id']}/approve", json={"note": "go"})
    assert res.status_code == 409, res.text
    # ERR-38 (W3): the route's refusal is the coded object.upload_expired copy.
    assert "uploaded file is no longer on the server" in res.text, res.text
    after = editor.get(f"/api/v1/jobs/{job}/parity-report").json()
    assert _writes_used(after) == 0
    assert _rerun_fix(after)["id"] == rerun["id"]  # proposed again, not "failed"
    assert scheduled == []
    assert "job.parity.fix_refused" in [e.get("action") for e in ent_.audit(ws)]


def test_a_started_rerun_is_scheduled_even_if_recording_the_outcome_fails(editor_ws, monkeypatch):
    # Re-review 3 (low): a failure after the route started the re-run dropped the request's
    # BackgroundTasks, leaving the new job pending forever. The start is scheduled on its own.
    import time

    from services import parity_apply

    _ent, ws, editor, scheduled = editor_ws
    job = _job(ws, "overwrite load", reconciliation=_SHORT, transfer_request={**_ROUTE, "sync_mode": "full_refresh_overwrite"})
    rerun = _rerun_fix(editor.get(f"/api/v1/jobs/{job}/parity-report").json())

    def broken(*_a, **_k):
        raise RuntimeError("the job store went away")

    monkeypatch.setattr(parity_apply, "settle_fix", broken)
    try:
        res = editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/{rerun['id']}/approve", json={"note": "go"})
        assert res.status_code >= 500
    except RuntimeError as exc:  # the test client may re-raise the server's error instead
        assert "the job store went away" in str(exc)
    for _ in range(50):
        if scheduled:
            break
        time.sleep(0.05)
    assert len(scheduled) == 1, "the started re-run was never scheduled"
