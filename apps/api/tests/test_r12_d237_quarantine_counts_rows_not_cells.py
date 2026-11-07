"""D237: a row with several refused cells is one quarantined row, not one per cell.

Neo4j node with no properties -> new Postgres table: the writer refused 5
typed cells of that one row (rejected_rows=1, 5 findings), yet the job said
"rejected 5", Gate-8 expected 30 - 5 = 25 against 29 written and failed with
"target has 4 extra rows". The headline and the stored-table alignment counted
findings (one quarantine record per cell) as rows.
"""

from __future__ import annotations

from src.transfer.job_quarantine import (
    align_rejects_to_stored_table,
    align_reported_rejects,
    quarantined_row_count,
)

FIVE_CELLS_ONE_ROW = [
    {"row": 23, "column": c, "reason": f"Empty value cannot coerce ({c})", "policy": "quarantine"}
    for c in ("amount", "flag", "d", "qty", "id")
]


def test_findings_of_one_row_are_one_quarantined_row():
    assert quarantined_row_count(FIVE_CELLS_ONE_ROW) == 1
    assert quarantined_row_count(FIVE_CELLS_ONE_ROW + [{"reason": "no row"}]) == 2


def test_headline_counts_the_row_the_writer_rejected():
    summary = {"rejected_rows": 1, "rejected_details": [dict(d) for d in FIVE_CELLS_ONE_ROW]}
    assert align_reported_rejects(summary) == 1
    assert summary["rejected_rows"] == 1


def test_stored_quarantine_records_do_not_reinflate_the_row_count():
    summary = {"rejected_rows": 1, "rejected_details": [dict(d) for d in FIVE_CELLS_ONE_ROW]}
    align_rejects_to_stored_table(summary, {"ok": True, "skipped": False, "rows_written": 5})
    assert summary["rejected_rows"] == 1
