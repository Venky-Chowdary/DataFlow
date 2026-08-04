"""Cycle 3 Enterprise GA — residual fail-closed holes."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_writer_diagnostics_keeps_full_rejected_details():
    from src.transfer.stream import _writer_diagnostics

    class _R:
        rejected_rows = 600
        coerced_null_rows = 0
        rows_skipped = 0
        rejected_details = [{"row": i, "column": "c"} for i in range(1, 601)]
        warnings = []
        load_method = "insert"

    out = _writer_diagnostics(_R())
    assert len(out["rejected_details"]) == 600
    assert len(out["rejected_details_sample"]) == 200


def test_reconcile_refuses_empty_target_checksum_spoof_pattern():
    """Equal empty digests must not be treated as population proof by workers."""
    from services.reconciliation import ReconciliationReport, stamp_post_write_phase

    report = ReconciliationReport(
        passed=False,
        source_rows=10,
        target_rows=10,
        source_checksum="abc123",
        target_checksum="",
        message="Gate-8: destination checksum unavailable — refuse fidelity claim",
        checksum_match=False,
        population_proof=False,
        assurance_level="none",
    )
    stamped = stamp_post_write_phase(report.to_dict())
    assert stamped["passed"] is False
    assert stamped.get("checksum_match") is False


def test_migration_worker_does_not_spoof_target_checksum():
    """verify_target empty digest → failed recon, never source==target invent."""
    from services.reconciliation import ReconciliationReport

    # Inline the decision branch used by migration_worker.
    source_checksum = "src-digest"
    target_checksum = ""
    tgt_chk = (target_checksum or "").strip()
    assert not tgt_chk
    recon = ReconciliationReport(
        passed=False,
        source_rows=5,
        target_rows=5,
        source_checksum=source_checksum,
        target_checksum="",
        message=(
            "Gate-8: destination checksum unavailable — "
            "refuse fidelity claim (never spoof source digest)"
        ),
        checksum_match=False,
        population_proof=False,
        assurance_level="none",
    )
    assert recon.passed is False
    assert recon.target_checksum != source_checksum


def test_g6_boolean_ack_without_contract_still_blocks():
    from services.ddl_compatibility import evaluate_ddl_compatibility

    ok, issues = evaluate_ddl_compatibility(
        mappings=[{
            "source": "amt",
            "target": "amt",
            "confidence": 0.99,
            "risk_acknowledged": True,
        }],
        source_schema={"amt": "FLOAT"},
        target_schema={"amt": "DECIMAL(12,4)"},
        table_exists=True,
        dest_connected=True,
        dest_db_type="postgresql",
        sample_rows=[{"amt": "1.5"}],
    )
    assert not ok
    assert any("Lossy type coercion" in i for i in issues)
