"""QA ACC-05 — Resume after a cancelled incremental run that committed nothing.

Run 2 of a PG→Mongo incremental_append was cancelled at 5% with
records_processed 0 and no checkpoint. Resume answered "Resume refused —
checkpoint not safe" although nothing landed: restarting from the last
committed watermark re-reads exactly the uncommitted delta. Anything that
committed rows, or a non-incremental / non-cancelled run, stays refused.
"""

from __future__ import annotations

from services.checkpoint_service import evaluate_resume_safety


def _job(**over):
    job = {
        "status": "cancelled",
        "records_processed": 0,
        "rows_processed": 0,
        "transfer_request": {"sync_mode": "incremental_append"},
    }
    job.update(over)
    return job


def test_cancelled_zero_row_incremental_resumes_from_the_watermark():
    out = evaluate_resume_safety(None, job=_job())
    assert out["ok"] is True, out["reasons"]
    assert out.get("restart_from_watermark") is True
    assert any("watermark" in w for w in out["warnings"])


def test_cancelled_zero_row_with_empty_checkpoint_resumes_too():
    out = evaluate_resume_safety({"chunk_index": 0, "rows_processed": 0}, job=_job())
    assert out["ok"] is True, out["reasons"]


def test_rows_committed_or_unknown_still_refused():
    assert evaluate_resume_safety(None, job=_job(records_processed=40))["ok"] is False
    unknown = _job()
    unknown.pop("records_processed")
    unknown.pop("rows_processed")
    assert evaluate_resume_safety(None, job=unknown)["ok"] is False


def test_full_refresh_or_failed_run_without_checkpoint_still_refused():
    full = _job(transfer_request={"sync_mode": "full_refresh_append"})
    assert evaluate_resume_safety(None, job=full)["ok"] is False
    assert evaluate_resume_safety(None, job=_job(status="failed"))["ok"] is False
