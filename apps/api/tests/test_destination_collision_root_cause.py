"""A key the destination already stores is not a duplicate in the source.

The run reported "Duplicate identity keys … on the Validate sample" and
prescribed "dedupe the source" for a source that was perfectly unique — the
destination enforced ``username`` and already held those rows. The operator was
sent to fix correct data instead of to the one control that resolves it: the
sync mode.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.root_cause_engine import build_root_causes  # noqa: E402

_COLLISION_DETAILS = {
    "sample_collisions": ["alice", "bob", "carol"],
    "primary_key": {"target": "username"},
    "sync_mode": "full_refresh_append",
    "rule_id": "g6_target_ddl.append_key_collision",
    "remediation_kind": "change_sync_mode",
}
_COLLISION_MESSAGE = (
    "Append would duplicate 3 existing destination key(s) on username — the "
    "destination enforces uniqueness, so the insert aborts."
)


def _collision_preflight() -> dict:
    gate = {
        "id": "g6_target_ddl",
        "status": "block",
        "message": _COLLISION_MESSAGE,
        "details": dict(_COLLISION_DETAILS),
    }
    return {
        "gates": [gate],
        "blockers": [
            {
                "id": "g6_target_ddl",
                "message": _COLLISION_MESSAGE,
                "details": dict(_COLLISION_DETAILS),
            }
        ],
        "row_count": 5,
    }


def test_destination_collision_owns_its_root_cause() -> None:
    roots = build_root_causes(_collision_preflight())
    kinds = [r.kind for r in roots]
    assert "destination_key_collision" in kinds
    root = next(r for r in roots if r.kind == "destination_key_collision")
    assert "username" in root.summary
    assert "upsert" in root.recommended_fix.lower()
    # The controls that resolve it are the sync mode (or the dedupe_skip
    # duplicate policy, R11), so no advice to dedupe the source.
    blob = " ".join(
        [root.summary, root.recommended_fix, root.business_impact, *root.alternative_fixes]
    ).lower()
    assert "dedupe_skip" in blob and "incremental_deduped" in blob
    assert "dedupe" not in blob.replace("dedupe_skip", "").replace("incremental_deduped", "")
    assert "duplicate rows in the source" not in blob


def test_destination_collision_does_not_also_raise_a_source_duplicate_root() -> None:
    roots = build_root_causes(_collision_preflight())
    # One root cause, one primary action: a second "duplicate identity keys"
    # root would give the operator two competing fixes for one refusal.
    assert [r.kind for r in roots].count("duplicate_identity_keys") == 0
    assert len({r.kind for r in roots}) == len(roots)


def test_real_source_duplicates_still_raise_the_duplicate_root() -> None:
    roots = build_root_causes(
        {
            "gates": [
                {
                    "id": "g5_identity",
                    "status": "block",
                    "message": "3 duplicate identity key(s) on the Validate sample",
                    "details": {"duplicate_keys": ["alice", "bob", "carol"]},
                }
            ],
            "blockers": [
                {
                    "id": "g5_identity",
                    "message": "3 duplicate identity key(s) on the Validate sample",
                    "details": {"duplicate_keys": ["alice", "bob", "carol"]},
                }
            ],
            "row_count": 5,
        }
    )
    kinds = [r.kind for r in roots]
    assert "destination_key_collision" not in kinds
    assert any("duplicate" in k for k in kinds)


def test_unreadable_collision_is_not_a_source_duplicate_root() -> None:
    details = {
        "primary_key": {"target": "id"},
        "rule_id": "g6_target_ddl.append_collision_unreadable",
        "remediation_kind": "choose_duplicate_rows",
        "collision_probe": {"identity_basis": "contract", "status": "error", "key_column": "id", "values_probed": 3},
    }
    message = "Append key collision on id could not be checked (Destination collision probe skipped: (psycopg2.errors.InsufficientPrivilege) permission denied for table restricted\n\n[SQL: SELECT \"id\" \nFROM \"qa_s2_audit_1108\".\"restricted\" \nWHERE \"id\" IN (%(id_1_1)s, %(id_1_2)s, %(id_1_3)s) \n LIMIT %(param_1)s]\n[parameters: {'param_1': 500, 'id_1_1': 1, 'id_1_2': 2, 'id_1_3': 3}]\n(Background on this error at: https://sqlalche.me/e/20/f405)). The destination enforces this key, so a stored key that repeats aborts the insert. Grant read (SELECT) on the destination table and re-run Validate, or choose Duplicate rows: skip or update keys already stored."
    pf = {
        "gates": [{"id": "g6_target_ddl", "status": "block", "message": message, "details": dict(details)}],
        "blockers": [{"id": "g6_target_ddl", "message": message, "details": dict(details)}],
        "row_count": 3,
    }
    roots = build_root_causes(pf)
    kinds = [r.kind for r in roots]
    assert "duplicate_identity" not in kinds, [(r.kind, r.title) for r in roots]
    assert "destination_key_collision" not in kinds
    assert all("Duplicate identity" not in (r.title or "") for r in roots)
