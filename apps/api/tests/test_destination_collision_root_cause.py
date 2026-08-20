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
    # The one control that resolves it is the sync mode, so no dedupe advice.
    blob = " ".join(
        [root.summary, root.recommended_fix, root.business_impact, *root.alternative_fixes]
    ).lower()
    assert "dedupe" not in blob
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
