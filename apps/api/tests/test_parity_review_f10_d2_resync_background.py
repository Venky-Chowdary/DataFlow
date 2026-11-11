"""Review finding 10 and duplication D2: fixes go through the one repair approval
workflow, and a key re-sync never runs inside the request.

10: the approve request ran the re-sync transfer (a full source scan filtered in
    Python) synchronously while holding the job lock. It now starts a transfer
    job on the existing scheduler (``run_transfer_async``) and returns; the fix
    reads ``applying`` until that job finishes, then is settled (re-check,
    round) by the job's completion callback or by the next read on any replica.
D2: Parity Studio kept its own propose -> approve -> apply workflow next to
    services.agentic_repair, and widened columns with only job.run. Each
    decision is now a ``source="parity"`` repair proposal decided with
    ``decide_proposal``; the routes need job.manage like the repair decide
    route, which refuses parity proposals (one entry point per kind).
P (plausible, confirmed): a re-sync whose key rendering matched no source row
    wrote 0 rows and still said "Re-synced N key(s)" — it now fails.
"""

from __future__ import annotations

import asyncio

import pytest

try:
    from services.parity_apply import _launch as SCHEDULER_LAUNCH
except ImportError:  # the code before this fix had no scheduler launch at all
    SCHEDULER_LAUNCH = None
from tests.test_parity_e2e_engine import _corrupting_route, _exec, _request, _run, store  # noqa: F401


@pytest.fixture()
def captured(store, monkeypatch):  # noqa: F811
    """The scheduler, captured: approve must hand the job over and not run it."""
    import src.transfer.background as background

    jobs: list[tuple[str, object]] = []
    if SCHEDULER_LAUNCH is not None:
        from services import parity_apply

        monkeypatch.setattr(parity_apply, "_launch", SCHEDULER_LAUNCH)  # the real launcher, not the inline one
    monkeypatch.setattr(background, "run_transfer_async", lambda child, req, resume=False: jobs.append((child, req)))
    return jobs


def test_the_approve_request_never_runs_the_transfer(store, captured, tmp_path, monkeypatch):  # noqa: F811
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report
    from src.transfer.engine import UniversalTransferEngine

    src, dst = _corrupting_route(tmp_path)
    job_id = _run(store, _request(src, dst))
    report = build_parity_report(store.get_job(job_id), job_id=job_id)
    resync = next(f for f in report["fixes"] if f["action"] == "resync_keys")

    def in_request(*_a, **_k):
        raise AssertionError("the re-sync transfer ran inside the approve request")

    monkeypatch.setattr(UniversalTransferEngine, "execute_tracked", in_request)
    returned = approve_fix(job_id, resync["id"], actor="ops@example.com", note="go")
    fix = next(f for f in returned["fixes"] if f["id"] == resync["id"])
    assert "inside the approve request" not in str(fix.get("result")), fix
    assert fix["status"] == "applying"


def test_approve_hands_the_resync_to_the_scheduler_and_settles_later(store, captured, tmp_path):  # noqa: F811
    from services.agentic_repair import list_proposals
    from services.parity_apply import settle_pending
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report
    from src.transfer.engine import UniversalTransferEngine

    src, dst = _corrupting_route(tmp_path)
    job_id = _run(store, _request(src, dst))
    report = build_parity_report(store.get_job(job_id), job_id=job_id)
    resync = next(f for f in report["fixes"] if f["action"] == "resync_keys")
    _exec(dst, "DROP TRIGGER cut", "DROP TRIGGER drop13")

    returned = approve_fix(job_id, resync["id"], actor="ops@example.com", note="go")
    assert len(captured) == 1, "the re-sync job went to the transfer scheduler"
    child_id, req = captured[0]
    assert req.source_filter == {"column": "id", "op": "in", "value": resync["_keys"]}
    fix = next(f for f in returned["fixes"] if f["id"] == resync["id"])
    assert fix["status"] == "applying" and fix["child_job_id"] == child_id
    assert returned["rounds"] == [] and returned["signoff"]["status"] == "blocked"
    # One approval workflow: the decision is a repair proposal, approved and awaiting its job.
    proposals = [p for p in list_proposals(job_id=job_id) if p.source == "parity"]
    assert [p.status for p in proposals] == ["approved"]
    assert fix["repair_proposal_id"] == proposals[0].id

    assert settle_pending(job_id) is None  # still running: nothing to settle
    UniversalTransferEngine().execute_tracked(req, child_id)  # the worker runs it
    settled = settle_pending(job_id)
    done = next(f for f in settled["fixes"] if f["id"] == resync["id"])
    assert done["status"] == "applied" and "Re-synced 4 key(s)" in done["result"]
    assert [r["unexplained_after"] for r in settled["rounds"]] == [0]
    assert [p.status for p in list_proposals(job_id=job_id) if p.source == "parity"] == ["applied"]


def test_a_resync_that_wrote_nothing_is_failed(store, captured, tmp_path):  # noqa: F811
    from services.parity_apply import settle_pending
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report

    src, dst = _corrupting_route(tmp_path)
    job_id = _run(store, _request(src, dst))
    report = build_parity_report(store.get_job(job_id), job_id=job_id)
    resync = next(f for f in report["fixes"] if f["action"] == "resync_keys")
    approve_fix(job_id, resync["id"], actor="ops@example.com", note="go")
    child_id, _req = captured[0]
    store.update_job_status(child_id, "completed", records_transferred=0)  # the IN filter matched nothing
    settled = settle_pending(job_id)
    fix = next(f for f in settled["fixes"] if f["id"] == resync["id"])
    assert fix["status"] == "failed" and "wrote no rows" in fix["result"], fix["result"]
    assert settled["rounds"] == []


def test_fix_decisions_need_job_manage_like_the_repair_decide_route():
    from services.rbac import Permission, _required_permission

    for action in ("approve", "dismiss"):
        assert _required_permission("POST", f"/api/v1/jobs/j/parity-report/fixes/f/{action}") == Permission.JOB_MANAGE
    assert _required_permission("POST", "/api/v1/repair/proposals/p/decide") == Permission.JOB_MANAGE


def test_the_repair_decide_route_refuses_a_parity_proposal(tmp_path, monkeypatch):
    from fastapi import HTTPException

    from services.agentic_repair import propose_from_parity
    from src.routers.repair_router import DecideBody, decide_repair

    proposal = propose_from_parity(job_id="job-9", fix={"id": "f_1", "action": "widen_column", "title": "Widen"})

    class _Req:
        class state:  # noqa: N801 - mimics request.state
            user_email = "x@example.com"

    with pytest.raises(HTTPException) as err:
        asyncio.run(decide_repair(proposal.id, DecideBody(approve=True), _Req()))
    assert err.value.status_code == 409 and "Trust Report" in str(err.value.detail)


def test_a_resync_that_cannot_start_is_failed_and_recorded(store, captured, tmp_path, monkeypatch):  # noqa: F811
    from services.agentic_repair import list_proposals
    from services.parity_fixes import approve_fix
    from services.parity_report import build_parity_report
    from src.transfer.engine import UniversalTransferEngine

    src, dst = _corrupting_route(tmp_path)
    job_id = _run(store, _request(src, dst))
    report = build_parity_report(store.get_job(job_id), job_id=job_id)
    resync = next(f for f in report["fixes"] if f["action"] == "resync_keys")

    def refuse(_self, _req):
        raise RuntimeError("job store unavailable")

    monkeypatch.setattr(UniversalTransferEngine, "_create_pending_job", refuse)
    after = approve_fix(job_id, resync["id"], actor="ops@example.com", note="go")
    fix = next(f for f in after["fixes"] if f["id"] == resync["id"])
    assert fix["status"] == "failed" and "could not start: job store unavailable" in fix["result"]
    assert not captured
    failed = [p for p in list_proposals(job_id=job_id) if p.source == "parity"]
    assert [p.status for p in failed] == ["failed"] and failed[0].apply_result["error"] == "job store unavailable"
