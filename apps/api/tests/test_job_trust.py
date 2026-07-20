"""Composite job trust score proofs."""

from __future__ import annotations

from services.job_trust import attach_trust_to_updates, compute_job_trust


def test_clean_completed_job_high_trust() -> None:
    trust = compute_job_trust({
        "status": "completed",
        "records_processed": 1000,
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "reconciliation": {"passed": True},
    })
    assert trust["score"] >= 90
    assert trust["grade"] == "A"
    assert trust["next_action"]["code"] == "ok"


def test_quarantine_lowers_score() -> None:
    trust = compute_job_trust({
        "status": "completed_with_quarantine",
        "records_processed": 100,
        "rejected_rows": 40,
        "reconciliation": {"passed": True},
    })
    assert trust["score"] < 85
    assert trust["next_action"]["code"] == "quarantine"


def test_reconcile_fail_next_action() -> None:
    trust = compute_job_trust({
        "status": "failed",
        "records_processed": 50,
        "rejected_rows": 0,
        "reconciliation": {"passed": False, "message": "checksum mismatch"},
    })
    assert trust["score"] < 50
    # Failed status prefers resume over reconcile.
    assert trust["next_action"]["code"] == "resume"


def test_attach_trust_only_on_terminal() -> None:
    running = {"phase": "load"}
    attach_trust_to_updates("running", running)
    assert "trust" not in running

    done: dict = {
        "records_processed": 10,
        "rejected_rows": 0,
        "reconciliation": {"passed": True},
    }
    attach_trust_to_updates("completed", done)
    assert done["trust_score"] >= 90
    assert done["trust"]["grade"] == "A"
