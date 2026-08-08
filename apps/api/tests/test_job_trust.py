"""Composite job trust score proofs — Gate-8 assurance honesty."""

from __future__ import annotations

from services.job_trust import attach_trust_to_updates, compute_job_trust, has_full_checksum_proof


def test_clean_completed_job_high_trust() -> None:
    trust = compute_job_trust({
        "status": "completed",
        "records_processed": 1000,
        "rejected_rows": 0,
        "coerced_null_rows": 0,
        "reconciliation": {
            "passed": True,
            "assurance_level": "full_checksum",
            "coverage": "full_checksum",
            "phase": "post_write_verified",
            "source_checksum": "aaa",
            "target_checksum": "aaa",
        },
    })
    assert trust["score"] >= 90
    assert trust["grade"] == "A"
    assert trust["next_action"]["code"] == "ok"


def test_passed_without_assurance_not_grade_a() -> None:
    """passed=True alone must not invent Verified / grade A."""
    trust = compute_job_trust({
        "status": "completed",
        "records_processed": 1000,
        "rejected_rows": 0,
        "reconciliation": {"passed": True},
    })
    assert trust["score"] <= 89
    assert trust["grade"] != "A"
    factor = next(f for f in trust["factors"] if f["id"] == "reconcile")
    assert factor["score"] <= 70
    assert "incomplete" in factor["note"].lower() or "without" in factor["note"].lower()


def test_writer_ack_not_full_gate8() -> None:
    full = compute_job_trust({
        "status": "completed",
        "records_processed": 1000,
        "rejected_rows": 0,
        "reconciliation": {
            "passed": True,
            "assurance_level": "full_checksum",
            "source_checksum": "a",
            "target_checksum": "a",
            "phase": "post_write_verified",
        },
    })
    ack = compute_job_trust({
        "status": "completed",
        "records_processed": 1000,
        "rejected_rows": 0,
        "reconciliation": {
            "passed": True,
            "phase": "post_write_writer_ack",
            "assurance_level": "writer_ack",
            "message": "Transfer verified by writer: 10 rows written (read-back verifier not available)",
            "source_checksum": "abc",
        },
    })
    assert ack["score"] < full["score"]
    assert ack["grade"] != "A"
    factor = next(f for f in ack["factors"] if f["id"] == "reconcile")
    assert "writer" in factor["note"].lower()
    assert factor["score"] <= 58


def test_sample_not_full_checksum_grade_a() -> None:
    trust = compute_job_trust({
        "status": "completed",
        "records_processed": 1000,
        "rejected_rows": 0,
        "reconciliation": {
            "passed": True,
            "assurance_level": "sample",
            "coverage": "sample",
            "phase": "post_write_sample_verified",
            "sample_compare": {"passed": True, "compared": 5},
        },
    })
    assert trust["score"] <= 89
    assert trust["grade"] != "A"
    factor = next(f for f in trust["factors"] if f["id"] == "reconcile")
    assert factor["score"] <= 68
    assert "sample" in factor["note"].lower()


def test_file_export_unproven_caps_trust() -> None:
    trust = compute_job_trust({
        "status": "completed",
        "records_processed": 100,
        "rejected_rows": 0,
        "reconciliation": {
            "passed": True,
            "unproven": True,
            "skipped_readback": True,
            "phase": "post_write_skipped",
            "assurance_level": "none",
            "message": "File/object export wrote successfully — Gate-8 cell fidelity unproven",
        },
    })
    assert trust["score"] <= 89
    assert trust["grade"] != "A"
    factor = next(f for f in trust["factors"] if f["id"] == "reconcile")
    assert factor["score"] <= 45
    assert "unproven" in factor["note"].lower()


def test_has_full_checksum_proof() -> None:
    assert has_full_checksum_proof({
        "passed": True,
        "assurance_level": "full_checksum",
        "source_checksum": "x",
        "target_checksum": "x",
    })
    assert not has_full_checksum_proof({
        "passed": True,
        "assurance_level": "writer_ack",
        "source_checksum": "x",
    })
    assert not has_full_checksum_proof({"passed": True})


def test_quarantine_lowers_score() -> None:
    trust = compute_job_trust({
        "status": "completed_with_quarantine",
        "records_processed": 100,
        "rejected_rows": 40,
        "reconciliation": {
            "passed": True,
            "assurance_level": "full_checksum",
            "source_checksum": "a",
            "target_checksum": "a",
        },
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
        "reconciliation": {
            "passed": True,
            "assurance_level": "full_checksum",
            "source_checksum": "a",
            "target_checksum": "a",
        },
    }
    attach_trust_to_updates("completed", done)
    assert done["trust_score"] >= 90
    assert done["trust"]["grade"] == "A"
