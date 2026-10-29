"""The create-new stamp re-derives the fidelity verdict on the final carrier.

It must judge the same measured population the first verdict did. A PostgreSQL
TIMESTAMPTZ declares microseconds; a BSON date keeps milliseconds. Re-deriving
without the samples read whole-millisecond cells as a truncation, set
``lossy_cast`` and parked the mapping at 0.70 under the G4 floor.
"""

from __future__ import annotations

from services.create_new_risk_stamp import apply_create_new_risk_stamps
from services.source_engine_scope import bind_source_engine


def _row() -> dict:
    return {
        "source": "created_at",
        "target": "created_at",
        "source_type": "TIMESTAMPTZ",
        "target_type": "DATE",
        "assignment_strategy": "identity_passthrough",
        "create_new": True,
        "transform": "datetime",
        "confidence": 0.93,
    }


def test_rederived_verdict_uses_the_measured_samples() -> None:
    samples = {"created_at": ["2024-01-05T10:30:00", "2024-02-11T23:59:59"]}
    with bind_source_engine("postgresql"):
        out = apply_create_new_risk_stamps(
            [_row()], "mongodb", source_samples=samples, source_db_type="postgresql"
        )
    row = out[0]
    assert row["fidelity"] == "preserve", row.get("fidelity_reason")
    assert row["requires_review"] is False
    assert row["confidence"] >= 0.85
