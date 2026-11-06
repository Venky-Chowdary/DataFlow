"""D257 (CI shard 3) — a write-time "widen" never changes a column's logical family.

Devin's append-replay test failed on the branch head: the parent CSV -> PG
append landed 'not-a-number' in an INTEGER-stamped ``age`` and failed its own
checksum. Bisected to the D200 commit (ebc0759c8): a DataFlow-projected stamp
stopped being an operator ceiling, so ``desired_types_honoring_map_stamps`` let
the source-derived candidate (``string`` -> TEXT) replace the declared INTEGER —
TEXT is "wider" than everything on the lattice, but INTEGER -> TEXT is a family
change, the one ``schema_drift.propose_dest_widens`` already refuses (D107).
The seam now applies the same rule: a candidate in another logical family is
not a desire; same-family widens (VARCHAR(5) -> VARCHAR(50), DECIMAL(8,2) ->
DECIMAL(12,2)) still go through.
"""

from __future__ import annotations

from connectors.writer_common import desired_types_honoring_map_stamps


def _projected(target: str, target_type: str) -> dict:
    return {"source": target, "target": target, "target_type": target_type,
            "create_new": True, "assignment_strategy": "identity_passthrough"}


def test_cross_family_candidate_keeps_the_declared_type():
    desired, refusals = desired_types_honoring_map_stamps(
        target_cols=["id", "age"],
        current_target_types=["INTEGER", "INTEGER"],
        mappings=[_projected("id", "INTEGER"), _projected("age", "INTEGER")],
        candidate_by_col={"id": "TEXT", "age": "TEXT"},
    )
    assert desired == ["INTEGER", "INTEGER"]
    assert refusals == []


def test_same_family_widen_still_applies_to_a_projected_stamp():
    desired, refusals = desired_types_honoring_map_stamps(
        target_cols=["note", "amount", "label"],
        current_target_types=["VARCHAR(5)", "DECIMAL(8,2)", "VARCHAR(50)"],
        mappings=[_projected("note", "VARCHAR(5)"), _projected("amount", "DECIMAL(8,2)"),
                  {"source": "label", "target": "label", "target_type": "VARCHAR(50)", "user_override": True}],
        candidate_by_col={"note": "VARCHAR(50)", "amount": "DECIMAL(12,2)", "label": "VARCHAR(500)"},
    )
    assert desired == ["VARCHAR(50)", "DECIMAL(12,2)", "VARCHAR(50)"]
    assert [r["column"] for r in refusals] == ["label"]  # the operator's ceiling still refuses
