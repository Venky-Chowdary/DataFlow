"""D245: a pause_on_change refusal for an added column named no column.

The pause verdict described only breaking changes; an additive-only change
left "...in a way this transfer reads: " with nothing after it.
"""

from __future__ import annotations

from services.source_schema_memory import evaluate_source_drift

PREV = {"id": "integer", "name": "text", "updated_at": "timestamp"}
MAPPINGS = [{"source": c, "target": c} for c in PREV]


def test_pause_on_added_column_names_the_column_and_type():
    verdict = evaluate_source_drift(
        previous_schema=PREV,
        current_schema={**PREV, "extra": "text"},
        mappings=MAPPINGS,
        schema_policy="pause_on_change",
        source_db="postgresql",
    )
    assert verdict.verdict == "block"
    assert "added column extra (text)" in verdict.summary, verdict.summary
    assert not verdict.summary.rstrip().endswith(":"), verdict.summary


def test_mapped_type_change_still_named():
    verdict = evaluate_source_drift(
        previous_schema=PREV,
        current_schema={**PREV, "name": "integer"},
        mappings=MAPPINGS,
        schema_policy="pause_on_change",
        source_db="postgresql",
    )
    assert verdict.verdict == "block"
    assert "name" in verdict.summary and "integer" in verdict.summary, verdict.summary
