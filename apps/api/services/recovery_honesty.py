"""Recovery / rollback honesty SSOT for Migration Assurance.

Charter: Recovery Integrity requires explainable capabilities. Datawrap must
never advertise one-click transfer undo, staging swap, or warehouse restore as
product features when they are not implemented.

See ``docs/MIGRATION_ROLLBACK.md`` for the operator runbook.
"""

from __future__ import annotations

from typing import Any

# Product claims — explicit False until a route ships with tests + docs.
TRANSFER_UNDO_CLAIMED = False
STAGING_SWAP_CLAIMED = False
WAREHOUSE_RESTORE_CLAIMED = False
BRANCH_SWITCH_CLAIMED = False
CDC_REWIND_CLAIMED = False


def honesty_dict() -> dict[str, Any]:
    """Canonical recovery posture for workspace / proof / Theater.

    Distinguishes what operators can rely on today from what must not be claimed.
    """
    return {
        "transfer_undo_claimed": TRANSFER_UNDO_CLAIMED,
        "staging_swap_claimed": STAGING_SWAP_CLAIMED,
        "warehouse_restore_claimed": WAREHOUSE_RESTORE_CLAIMED,
        "branch_switch_claimed": BRANCH_SWITCH_CLAIMED,
        "cdc_rewind_claimed": CDC_REWIND_CLAIMED,
        "capabilities": {
            "quarantine_holdout": {
                "available": True,
                "note": "Bad rows held out of primary write — not silent drop.",
            },
            "checkpoint_resume": {
                "available": True,
                "note": (
                    "Fail-closed checkpoint persistence; resume only when safety "
                    "evaluation allows (not a substitute for destination restore)."
                ),
            },
            "mapping_repair_revalidate": {
                "available": True,
                "note": "Map repair → re-run G1–G9 before Execute.",
            },
            "cdc_lease": {
                "available": True,
                "note": "Prevents concurrent consumers; delivery remains at-least-once.",
            },
            "gate8_proof_export": {
                "available": True,
                "note": "HMAC proof packs are cutover evidence — not an undo button.",
            },
            "staging_discard": {
                "available": True,
                "note": (
                    "Module 6: DISCARD_STAGING drops `{table}_df_staging` only — "
                    "never mutates the primary table; population undo not claimed."
                ),
            },
            "transfer_undo": {
                "available": False,
                "note": "One-click destination undo of committed rows is not productized.",
            },
            "staging_swap": {
                "available": False,
                "note": "Blue-green / synonym swap orchestration is not a Studio action.",
            },
            "warehouse_restore": {
                "available": False,
                "note": (
                    "Snowflake Time Travel / PG PITR / vendor restore remain DBA tooling — "
                    "Datawrap does not replace them. REQUIRE_WAREHOUSE_RESTORE plans point here."
                ),
            },
            "branch_switch": {
                "available": False,
                "note": "DDL branch/undo after create-new is not productized.",
            },
            "cdc_rewind": {
                "available": False,
                "note": "Exactly-once CDC rewind / continuous replication undo is not claimed.",
            },
        },
        "rollback_strategies": {
            "DOCUMENT_ONLY": {"executable": False},
            "DISCARD_STAGING": {"executable": True},
            "REQUIRE_WAREHOUSE_RESTORE": {"executable": False},
        },
        "operator_runbook": "docs/MIGRATION_ROLLBACK.md",
        "notes": [
            "Prefer create-new or staging schema before cutover.",
            "DISCARD_STAGING is the only executable DataWrap rollback today.",
            "If production already swapped — restore from your warehouse backup / time-travel.",
            "Never claim Datawrap replaces DBA restore tooling.",
        ],
    }
