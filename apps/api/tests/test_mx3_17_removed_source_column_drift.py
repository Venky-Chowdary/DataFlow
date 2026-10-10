"""MX3-17 — a destination column the source no longer has is schema drift.

QA (PG -> existing PG table): the source lost a column the destination still
has. Stage preview reported no blocker under schema_policy=manual_review and
pause_on_change; the only trace was a non-blocking "destination column(s) are
unmapped" note. The column silently stops being fed. It is a ``drop`` and the
policy decides: manual_review -> review, pause_on_change -> pause.
"""

from __future__ import annotations

import pytest


def _drift(policy: str, *, target_extra: bool = True):
    from services.schema_drift import detect_schema_drift

    source = {"id": "INTEGER", "name": "VARCHAR(60)"}
    target = dict(source)
    if target_extra:
        target["email"] = "VARCHAR(60)"
    return detect_schema_drift(
        source_columns=list(source),
        source_schema=source,
        target_columns=list(target),
        target_schema=target,
        mappings=[{"source": c, "target": c} for c in source],
        destination_db_type="postgresql",
        schema_policy=policy,
        table_exists=True,
    )


@pytest.mark.parametrize(
    ("policy", "action"),
    # type_locked already treats every drop as hard-breaking: it pauses.
    [("manual_review", "review"), ("pause_on_change", "pause"), ("type_locked", "pause")],
)
def test_destination_only_column_is_drop_drift(policy, action):
    drift = _drift(policy)
    evo = drift["schema_evolution"]
    assert evo["action"] == action, drift
    drops = [b for b in (drift["classification"] or {}).get("breaking") or [] if b["kind"] == "drop"]
    assert [d["column"] for d in drops] == ["email"], drift
    assert drift["orphan_targets"] == ["email"]


def test_propagate_keeps_destination_history_without_pausing():
    evo = _drift("propagate_columns")["schema_evolution"]
    assert evo["action"] != "pause", evo


def test_identical_schema_is_not_drift():
    evo = _drift("pause_on_change", target_extra=False)["schema_evolution"]
    assert evo["action"] == "continue", evo
