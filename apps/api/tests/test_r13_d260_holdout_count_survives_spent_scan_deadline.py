"""D260 residual: preflight said "1 row(s) will be held out" for 3 holdouts.

Production pf_ae210503ed3b: PostgreSQL ``pv_allbad`` (3 rows, qty 'oops' / 'x1'
/ 'bad') into a new PostgreSQL table, qty → integer under a signed
CAST_AND_CONTINUE contract. The sample transform said "3 contracted holdout(s)"
and the job quarantined 3, but g3f said 1. Over a slow tunnel the earlier gates
spent the population-fit deadline, and the walk stopped after its first row —
so the gate forecast the one holdout it had seen as the whole count.
"""

from __future__ import annotations

import pytest

from services.population_fit_scan import GATE_ID
from services.preflight_service import run_file_preflight

def _signed_cast_and_continue() -> dict:
    """The production run's signed contract for qty: without it the column has
    no holdout under balanced (validation_mode_contract.mode_holdout_kind)."""
    from services.migration_risk_contract import sign_risk_contract

    body: dict = {
        "risk_id": "sample_transform", "severity": "high", "root_cause": "sample_transform",
        "column": "qty", "source_type": "TEXT", "destination_type": "INTEGER",
        "transform": "integer", "rows_sampled": 3, "estimated_rows": 3,
        "expected_failure_pct": 1.0, "expected_precision_loss": False,
        "expected_truncation": False, "expected_nulls": False,
        "execution_policy": "CAST_AND_CONTINUE", "quarantine_policy": "DLQ",
        "retry_policy": "NONE", "rollback_strategy": "DOCUMENT_ONLY",
        "approved_by": "admin@dataflow.app", "approved_at": "2026-08-17T00:00:00Z",
        "reason": "Bad qty values are held out for this load", "target": "qty",
    }
    body["signature"] = sign_risk_contract(body)
    return body


_MAPPINGS = [
    {"source": "id", "target": "id", "confidence": 0.99, "target_type": "INTEGER"},
    {"source": "qty", "target": "qty", "confidence": 0.99, "target_type": "INTEGER",
     "transform": "integer", "risk_contract": _signed_cast_and_continue()},
]


def _fit_gate(rows: list[dict[str, str]], sample: int = 0, **kw) -> dict:
    result = run_file_preflight(
        columns=["id", "qty"],
        column_types={"id": "INTEGER", "qty": "TEXT"},
        row_count=len(rows),
        mappings=_MAPPINGS,
        destination_connected=True,
        destination_column_types={"id": "INTEGER", "qty": "INTEGER"},
        destination_db_type="postgresql",
        destination_table_exists=False,
        estimated_bytes=256,
        validation_mode="balanced",
        sample_rows=rows[:sample] if sample else rows,
        population_rows=iter(list(rows)),
        rows_are_population=True,
        **kw,
    )
    return next(g for g in result["gates"] if g["id"] == GATE_ID)


@pytest.mark.parametrize(
    "rows",
    [
        [{"id": "1", "qty": "oops"}, {"id": "2", "qty": "x1"}, {"id": "3", "qty": "bad"}],
        [{"id": "1", "qty": "4"}, {"id": "2", "qty": "x1"}, {"id": "3", "qty": "bad"}],
    ],
    ids=["3-holdouts", "2-holdouts"],
)
def test_spent_deadline_does_not_shrink_the_holdout_count_below_the_sample(rows) -> None:
    bad = sum(1 for r in rows if not r["qty"].isdigit())
    gate = _fit_gate(rows, fit_scan_deadline=0)
    assert gate["status"] == "warn"
    assert gate["message"].startswith(f"{bad} row(s) will be held out in quarantine on write")
    assert gate["details"]["rows_scanned"] == len(rows)


def test_a_truly_partial_scan_never_states_its_holdouts_as_the_total(monkeypatch) -> None:
    import services.population_fit_scan as fit

    monkeypatch.setattr(fit, "ENVELOPE_CONTINUE_SECONDS", 0.0)
    rows = [{"id": str(i), "qty": "bad"} for i in range(1, 4)]
    result = run_file_preflight(
        columns=["id", "qty"],
        column_types={"id": "INTEGER", "qty": "TEXT"},
        row_count=3,
        mappings=_MAPPINGS,
        destination_connected=True,
        destination_column_types={"id": "INTEGER", "qty": "INTEGER"},
        destination_db_type="postgresql",
        destination_table_exists=False,
        estimated_bytes=256,
        validation_mode="balanced",
        sample_rows=rows[:1],
        population_rows=iter(list(rows)),
        rows_are_population=True,
        fit_scan_deadline=0,
    )
    gate = next(g for g in result["gates"] if g["id"] == GATE_ID)
    details = gate["details"]
    covered = details["rows_scanned"] + details["envelope_rows_scanned"]
    assert details["evidence"] == "partial"
    assert covered < 3
    # Every scanned row is bad, so the count is exactly the rows it covered.
    assert gate["message"].startswith(f"At least {covered} row(s) will be held out")
    assert f"after {covered} of 3 source row(s)" in gate["message"]


def test_the_envelope_rescans_the_row_the_deadline_stopped_at() -> None:
    """The row in hand when the deadline fired was dropped: 3 bad rows read as 2."""
    rows = [{"id": str(i), "qty": "bad"} for i in range(1, 4)]
    gate = _fit_gate(rows, sample=1, fit_scan_deadline=0)
    assert gate["details"]["truncated_reason"] == "time"
    assert gate["message"].startswith("3 row(s) will be held out")
