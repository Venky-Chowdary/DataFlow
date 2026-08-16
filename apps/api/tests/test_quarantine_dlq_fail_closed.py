"""Module 5 — Quarantine DLQ must fail closed (never best-effort lose rejects)."""

from __future__ import annotations

import pytest

import services.quarantine_dlq as dlq


def test_no_rejects_is_durable_vacuously():
    summary = {"rejected_details": []}
    dlq.assert_quarantine_durable_or_raise(summary)
    assert dlq.persist_job_quarantine_outcome(summary)["ok"] is True


def test_durable_true_passes():
    summary = {
        "rejected_details": [{"row": 1, "reason": "bad"}],
        "quarantine_durable": True,
    }
    dlq.assert_quarantine_durable_or_raise(summary)
    assert dlq.persist_job_quarantine_outcome(summary)["ok"] is True


def test_durable_false_with_rejects_fails_closed():
    summary = {
        "rejected_details": [{"row": 1, "reason": "bad"}],
        "quarantine_durable": False,
        "quarantine_dlq_error": "disk full",
    }
    # Bind exception class from the live module (suite may reload services.*).
    with pytest.raises(dlq.QuarantineDlqLostError) as ei:
        dlq.assert_quarantine_durable_or_raise(summary)
    assert "disk full" in str(ei.value).lower() or "durable" in str(ei.value).lower()
    out = dlq.persist_job_quarantine_outcome(summary)
    assert out["ok"] is False
    assert out["fail_closed"] is True


def test_missing_durable_flag_with_rejects_fails_closed():
    """Unknown durability with rejects is not a silent success."""
    summary = {"rejected_details": [{"row": 1}]}
    with pytest.raises(dlq.QuarantineDlqLostError):
        dlq.assert_quarantine_durable_or_raise(summary)


def _finding(row: int, age: str, *, status: str = "open") -> dict:
    return {
        "row": row,
        "column": "age",
        "value": age,
        "reason": "Invalid integer",
        "values": {"id": str(row), "age": age},
        "source_values": {"id": str(row), "age": age},
        "retry_status": status,
    }


def test_replay_closure_closed_is_not_migration_proven():
    findings = [_finding(2, "25")]
    stamped = dlq.stamp_replay_attempt(
        findings, child_rejected=[], gate8_passed=True, child_job_id="child"
    )
    state = dlq.evaluate_replay_closure(stamped, last_replay={"gate8_passed": True, "rejected": 0})
    assert state["verdict"] == dlq.VERDICT_CLOSED
    assert state["open_count"] == 0
    assert state["migration_proven"] is False
    assert "checksum" in state["next_action"].lower() or "not" in state["note"].lower()


def test_replay_closure_gate8_failure_promotes_nothing():
    findings = [_finding(2, "25"), _finding(3, "30")]
    stamped = dlq.stamp_replay_attempt(
        findings, child_rejected=[], gate8_passed=False, child_job_id="child"
    )
    assert all(d["retry_status"] == "replay_failed" for d in stamped)
    state = dlq.evaluate_replay_closure(
        stamped, last_replay={"gate8_passed": False, "rejected": 0}
    )
    assert state["verdict"] == dlq.VERDICT_DIVERGING
    assert state["open_count"] == 2
    assert state["promoted_count"] == 0


def test_partial_promote_does_not_claim_closed():
    findings = [_finding(2, "bad"), _finding(3, "also-bad")]
    stamped = dlq.stamp_replay_attempt(
        findings,
        attempted=[findings[0]],
        child_rejected=[],
        gate8_passed=True,
        child_job_id="child",
    )
    state = dlq.evaluate_replay_closure(stamped, last_replay={"gate8_passed": True, "rejected": 0})
    assert state["verdict"] == dlq.VERDICT_IN_PROGRESS
    assert state["promoted_count"] == 1
    assert state["open_count"] == 1


def test_skip_row_is_vacuous_for_closure():
    skips = [{
        "row": 1,
        "disposition": "skipped",
        "execution_policy": "SKIP_ROW",
        "quarantine_required": False,
        "reason": "audit skip",
    }]
    state = dlq.evaluate_replay_closure(skips)
    assert state["verdict"] == dlq.VERDICT_VACUOUS
    assert state["durable_count"] == 0


def test_historical_rejected_does_not_false_incomplete_after_promote():
    """Same class of bug as Full Append dest-Δ: do not compare to a stale census."""
    job = {
        "rejected_rows": 40,
        "rejected_details": [_finding(2, "25", status="promoted")],
    }
    assert dlq.quarantine_sample_incomplete(job, job["rejected_details"]) is None
