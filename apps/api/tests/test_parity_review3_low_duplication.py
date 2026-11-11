"""Re-review 3 (LOW, duplication): Gate-8's stored-value length and the job status sets have one source."""

from __future__ import annotations


def test_the_stored_value_limit_is_gate8s_own():
    from services import parity_report, reconciliation

    assert parity_report.STORED_VALUE_LIMIT is reconciliation.STORED_MISMATCH_VALUE_CHARS


def test_gate8_stores_values_cut_at_its_constant():
    from services.reconciliation import STORED_MISMATCH_VALUE_CHARS, sample_compare_rows

    src = [{"id": 1, "note": "x" * 300}]
    dst = [{"id": 1, "note": "y" * 300}]
    out = sample_compare_rows(src, dst, [{"source": "id", "target": "id"}, {"source": "note", "target": "note"}],
                              target_columns=["id", "note"], sample_size=1, sort_key="id")
    [mm] = out["mismatches"]
    assert len(mm["source_value"]) == len(mm["target_value"]) == STORED_MISMATCH_VALUE_CHARS


def test_resync_outcomes_use_the_platform_status_sets():
    from services import parity_apply
    from services.job_status import COMPLETED_STATUSES, TERMINAL_STATUSES

    assert parity_apply._CHILD_OK == COMPLETED_STATUSES
    assert parity_apply._CHILD_FAILED == TERMINAL_STATUSES - COMPLETED_STATUSES
