"""C20: rerun/retry gating checks the key/cursor contract, not the mode family.

D5  — keyed upsert rerun must not need force; a forced response must not pair
      success=true with a refused decision.
D12 — an incremental rerun with a cursor contract continues from the persisted
      watermark; it cannot re-commit the previous prefix.
D33 — retry after a rolled-back run must not refuse on "committed" rows the
      undo already erased.
D6  — reruns, retries and schedule fires record triggered_by / confirmation.
Minor — cancel on a terminal job reports success=False (nothing was done).
"""

import pytest

from services.execution_engine_contract import (
    ResumeKind,
    committed_rows_of,
    decide_job_rerun,
    decide_retry_from_start,
)


# --------------------------------------------------------------------------- #
# D5: keyed modes collapse a second pass — no force required                  #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("engine", ["sqlite", "postgres", "snowflake"])
def test_rerun_keyed_upsert_no_force(engine):
    # engine param pins the matrix intent; the gate is engine-agnostic.
    decision = decide_job_rerun(
        status="completed",
        sync_mode="incremental_deduped",
        rows_committed=320,
        has_key_contract=True,
    )
    assert decision["allowed"] is True, engine
    assert decision["kind"] == ResumeKind.FROM_ZERO_IDEMPOTENT.value


def test_rerun_incremental_deduped_converges_on_mode_name_alone():
    decision = decide_job_rerun(
        status="completed",
        sync_mode="incremental_deduped",
        rows_committed=10,
    )
    assert decision["allowed"] is True


# --------------------------------------------------------------------------- #
# D12: incremental rerun with a cursor contract reads only new rows           #
# --------------------------------------------------------------------------- #


def test_rerun_append_cursor_no_force():
    decision = decide_job_rerun(
        status="completed",
        sync_mode="incremental_append",
        rows_committed=60_000,
        has_cursor_contract=True,
    )
    assert decision["allowed"] is True
    assert decision["kind"] == ResumeKind.FROM_CURSOR.value
    assert "watermark" in decision["reason"]


def test_rerun_append_without_cursor_still_refused():
    decision = decide_job_rerun(
        status="completed",
        sync_mode="incremental_append",
        rows_committed=60,
    )
    assert decision["allowed"] is False
    assert decision["kind"] == ResumeKind.REFUSED.value


def test_cursor_contract_does_not_rescue_full_refresh_append():
    # A cursor on a full-refresh mode changes nothing: it still re-reads all.
    decision = decide_job_rerun(
        status="completed",
        sync_mode="full_refresh_append",
        rows_committed=60,
        has_cursor_contract=True,
    )
    assert decision["allowed"] is False


def test_retry_append_with_cursor_contract_allowed():
    decision = decide_retry_from_start(
        status="failed",
        sync_mode="incremental_append",
        rows_committed=4_000,
        rows_committed_known=True,
        operator_requested=True,
        has_cursor_contract=True,
    )
    assert decision["allowed"] is True
    assert decision["kind"] == ResumeKind.FROM_CURSOR.value


# --------------------------------------------------------------------------- #
# D33: undo-cleared rows are not "committed" for the duplicate gate           #
# --------------------------------------------------------------------------- #


def test_retry_after_rollback_no_force():
    job = {
        "records_processed": 320,
        "destination_summary": {
            "rows_written": 320,
            "partial_batch_undo": "cleared",
        },
    }
    rows, known = committed_rows_of(job)
    assert (rows, known) == (0, True)
    decision = decide_retry_from_start(
        status="failed",
        sync_mode="full_refresh_append",
        rows_committed=rows,
        rows_committed_known=known,
    )
    assert decision["allowed"] is True


def test_committed_rows_zero_when_auto_create_table_was_dropped():
    job = {
        "records_processed": 8,
        "error_details": {"auto_create_rolled_back": ["hq0800_ap"]},
    }
    assert committed_rows_of(job) == (0, True)


def test_committed_rows_honest_when_undo_retained_the_batch():
    job = {
        "records_processed": 12,
        "destination_summary": {"partial_batch_undo": "retained"},
    }
    assert committed_rows_of(job) == (12, True)


# --------------------------------------------------------------------------- #
# REST level: rerun honors the saved contract; forced decisions are honest    #
# --------------------------------------------------------------------------- #


def _job_shell(status: str, sync_mode: str, rows: int, **req_extra) -> dict:
    request = {
        "source": {"kind": "database", "type": "sqlite", "table": "src"},
        "destination": {"kind": "database", "type": "sqlite", "table": "dst"},
        "sync_mode": sync_mode,
        "skip_preflight": True,
    }
    request.update(req_extra)
    return {
        "job_id": "job-c20",
        "status": status,
        "records_processed": rows,
        "transfer_request": request,
    }


def _post_job(monkeypatch, path: str, job: dict):
    from fastapi.testclient import TestClient

    from services.mongodb_service import get_mongodb_service
    from src.main import app

    mongo = get_mongodb_service()
    created: dict = {}
    captured: dict = {}
    monkeypatch.setattr(
        mongo, "get_job", lambda jid: job if jid == job["job_id"] else None
    )
    monkeypatch.setattr(
        mongo,
        "update_job_status",
        lambda jid, status, **kw: created.update(
            {"job_id": jid, "status": status, **kw}
        )
        or True,
    )
    import src.transfer.engine as engine_mod

    class _Engine:
        def _create_pending_job(self, req):
            captured["req"] = req
            return "new-job"

    monkeypatch.setattr(engine_mod, "get_transfer_engine", lambda: _Engine())
    monkeypatch.setattr(
        "src.transfer.background.run_transfer_async", lambda *a, **k: None
    )
    with TestClient(app) as client:
        response = client.post(path)
    return response, created, captured


def test_rest_rerun_keyed_upsert_needs_no_force(monkeypatch):
    res, created, _ = _post_job(
        monkeypatch,
        "/api/v1/connectors/jobs/job-c20/rerun",
        _job_shell(
            "completed",
            "incremental_deduped",
            320,
            stream_contracts=[{"primary_key": "id"}],
        ),
    )
    assert res.status_code == 200, res.text
    assert res.json()["success"] is True
    assert created.get("rerun_of") == "job-c20"


def test_rest_rerun_append_with_cursor_needs_no_force(monkeypatch):
    res, _, _ = _post_job(
        monkeypatch,
        "/api/v1/connectors/jobs/job-c20/rerun",
        _job_shell(
            "completed",
            "incremental_append",
            60_000,
            stream_contracts=[{"cursor_field": "updated_at"}],
        ),
    )
    assert res.status_code == 200, res.text


def test_rest_forced_rerun_decision_is_consistent(monkeypatch):
    res, _, _ = _post_job(
        monkeypatch,
        "/api/v1/connectors/jobs/job-c20/rerun?force=true",
        _job_shell("completed_with_quarantine", "full_refresh_append", 500),
    )
    assert res.status_code == 200, res.text
    body = res.json()
    # D5: success=true must never ship alongside allowed=false / refused.
    assert body["success"] is True
    assert body["retry_decision"]["allowed"] is True
    assert body["retry_decision"]["kind"] != ResumeKind.REFUSED.value
    assert body["retry_decision"]["forced"] is True


# --------------------------------------------------------------------------- #
# D6: triggered_by + confirmation are recorded on rerun and schedule fires    #
# --------------------------------------------------------------------------- #


def test_governance_recorded_on_rerun(monkeypatch):
    res, created, captured = _post_job(
        monkeypatch,
        "/api/v1/connectors/jobs/job-c20/rerun",
        _job_shell("completed", "full_refresh_overwrite", 10),
    )
    assert res.status_code == 200, res.text
    req = captured["req"]
    assert req.triggered_by  # never blank
    confirmation = created.get("confirmation")
    assert isinstance(confirmation, dict)
    assert confirmation["action"] == "rerun"
    assert confirmation["of"] == "job-c20"


def test_governance_recorded_on_schedule_fire():
    from types import SimpleNamespace

    import services.schedule_runner as runner
    from services import schedule_store as store

    sched = store.PipelineSchedule.from_dict(
        {
            "id": "s-c20",
            "name": "nightly",
            "source_connector_id": "src",
            "source_table": "orders",
            "dest_connector_id": "dst",
            "dest_table": "orders_wh",
            "interval": "hourly",
            "mappings": [{"source": "id", "target": "id"}],
        }
    )
    src = {"_id": "src", "id": "src", "type": "sqlite", "database": "d"}
    dst = {"_id": "dst", "id": "dst", "type": "sqlite", "database": "d"}
    req = runner.build_schedule_request(sched, src, dst)
    assert req.triggered_by == "schedule:nightly"

    req2 = runner.build_schedule_request(
        sched, src, dst, triggered_by="alice@corp"
    )
    assert req2.triggered_by == "alice@corp"


# --------------------------------------------------------------------------- #
# Minor: cancel on a terminal job reports failure honestly                    #
# --------------------------------------------------------------------------- #


def test_cancel_terminal_job_not_success(monkeypatch):
    from fastapi.testclient import TestClient

    from services.mongodb_service import get_mongodb_service
    from src.main import app

    mongo = get_mongodb_service()
    job = {
        "job_id": "job-c20",
        "status": "completed",
        "transfer_request": {},
    }
    monkeypatch.setattr(
        mongo, "get_job", lambda jid: job if jid == "job-c20" else None
    )
    with TestClient(app) as client:
        res = client.post("/api/v1/connectors/jobs/job-c20/cancel")
    assert res.status_code == 200
    body = res.json()
    assert body["success"] is False
    assert "terminal" in body["message"].lower()
