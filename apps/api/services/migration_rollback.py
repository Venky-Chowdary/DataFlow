"""Migration Rollback Workflow — Module 6 SSOT.

Charter: "No rollback workflow" is P0 debt. DataWrap must ship an explicit,
auditable rollback plan for every migration — without inventing one-click
undo of committed production rows.

Strategies
----------
DOCUMENT_ONLY
    Immutable plan + operator runbook. Not executable. Default for primary writes.

DISCARD_STAGING
    Drop ``{table}_df_staging`` only. Never touches the primary table.
    Executable when a staging table was used (pre-ingestion staging).

REQUIRE_WAREHOUSE_RESTORE
    Plan that points operators at DBA time-travel / PITR. Never executed by DataWrap.

Honesty
-------
``population_undo_claimed`` is always False.
``transfer_undo_claimed`` remains False in recovery_honesty.
Only ``staging_discard`` is marked available.

See ``docs/MIGRATION_ROLLBACK.md``.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

ROLLBACK_STRATEGIES = frozenset(
    {
        "DOCUMENT_ONLY",
        "DISCARD_STAGING",
        "REQUIRE_WAREHOUSE_RESTORE",
    }
)

EXECUTABLE_STRATEGIES = frozenset({"DISCARD_STAGING"})

DEFAULT_ROLLBACK_STRATEGY = "DOCUMENT_ONLY"


class RollbackContractError(ValueError):
    """Plan is missing, tampered, or not a valid rollback contract."""


class RollbackRefuseError(RuntimeError):
    """Execution refused — fail closed (strategy not executable or drop failed)."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(payload: dict[str, Any]) -> str:
    body = {k: v for k, v in payload.items() if k != "signature"}
    return json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)


def sign_rollback_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def verify_rollback_signature(payload: dict[str, Any]) -> bool:
    sig = str(payload.get("signature") or "")
    if not sig:
        return False
    expected = sign_rollback_payload(payload)
    return sig == expected


@dataclass(frozen=True)
class RollbackPlan:
    """Immutable rollback plan for one migration job."""

    rollback_id: str
    job_id: str
    strategy: str
    executable: bool
    destination_table: str
    staging_table: str | None
    sync_mode: str
    rows_written: int
    promote_blocked: bool
    population_undo_claimed: bool
    guarantees: list[str]
    non_guarantees: list[str]
    recovery_steps: list[str]
    documentation: str
    created_at: str
    signature: str
    status: str = "planned"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def plan_rollback(
    *,
    job_id: str,
    sync_mode: str,
    destination_table: str,
    staging_table: str | None = None,
    rows_written: int = 0,
    promote_blocked: bool = False,
    dest_type: str = "",
) -> RollbackPlan:
    """Build a signed rollback plan. Never claims population undo."""
    stage = (staging_table or "").strip() or None
    mode = (sync_mode or "").strip().lower()
    table = (destination_table or "").strip() or "unknown"

    if stage:
        strategy = "DISCARD_STAGING"
        executable = True
        guarantees = [
            f"Can discard staging table `{stage}` without mutating primary `{table}`.",
            "Primary table rows are untouched by DISCARD_STAGING.",
        ]
        non_guarantees = [
            "Does not undo rows already promoted to the primary table.",
            "Does not restore warehouse snapshots or CDC positions.",
            "Population / production undo is NOT claimed.",
        ]
        recovery_steps = [
            f"Approve DISCARD_STAGING for job `{job_id}`.",
            f"Drop staging table `{stage}`.",
            "Re-run Map → Validate → Execute into a clean staging target if needed.",
            "If primary was already promoted — use warehouse time-travel / PITR (DBA).",
        ]
    elif mode in {"full_refresh_overwrite", "overwrite", "truncate", "full_overwrite"}:
        strategy = "REQUIRE_WAREHOUSE_RESTORE"
        executable = False
        guarantees = [
            "Plan records that overwrite landed on the destination table.",
            "Operators receive an explicit restore runbook pointer.",
        ]
        non_guarantees = [
            "DataWrap does not execute warehouse restore.",
            "TRUNCATE of primary is not auto-executed (refuse silent destructive undo).",
            "Population undo is NOT claimed.",
        ]
        recovery_steps = [
            "Do not resume an unsafe checkpoint into a polluted primary.",
            "Restore destination from vendor time-travel / PITR / backup (DBA tooling).",
            "Re-land into staging, Validate, then cut over only after Gate-8 proof.",
        ]
    else:
        strategy = "DOCUMENT_ONLY"
        executable = False
        guarantees = [
            "Rollback posture is recorded on the job for audit.",
        ]
        non_guarantees = [
            "No automatic destination undo for incremental / append loads.",
            "Population undo is NOT claimed.",
        ]
        recovery_steps = [
            "Quarantine remediations re-enter Validate — preferred for bad cells.",
            "Prefer create-new / staging schema for the next attempt.",
            "If production already swapped — restore from warehouse backup / time-travel.",
        ]

    if promote_blocked and stage:
        recovery_steps = [
            "Promote was blocked — primary should be untouched.",
            *recovery_steps,
        ]

    plan = RollbackPlan(
        rollback_id=str(uuid.uuid4()),
        job_id=str(job_id or ""),
        strategy=strategy,
        executable=executable,
        destination_table=table,
        staging_table=stage,
        sync_mode=mode,
        rows_written=int(rows_written or 0),
        promote_blocked=bool(promote_blocked),
        population_undo_claimed=False,
        guarantees=list(guarantees),
        non_guarantees=list(non_guarantees),
        recovery_steps=list(recovery_steps),
        documentation="docs/MIGRATION_ROLLBACK.md",
        created_at=_now(),
        signature="",  # filled below
        status="planned",
        metadata={"dest_type": (dest_type or "").strip().lower()},
    )
    payload = plan.to_dict()
    sig = sign_rollback_payload(payload)
    return RollbackPlan(**{**payload, "signature": sig})


def execute_rollback(
    plan: dict[str, Any] | RollbackPlan,
    *,
    approved_by: str,
    reason: str,
    drop_staging_fn: Callable[[str], bool] | None = None,
) -> dict[str, Any]:
    """Execute an approved rollback plan. Fail closed on refuse / drop failure."""
    raw = plan.to_dict() if isinstance(plan, RollbackPlan) else dict(plan or {})
    if not verify_rollback_signature(raw):
        raise RollbackContractError(
            "Rollback plan signature invalid — refuse execution (possible tamper)"
        )

    strategy = str(raw.get("strategy") or "")
    if strategy not in ROLLBACK_STRATEGIES:
        raise RollbackContractError(f"Unknown rollback strategy: {strategy!r}")

    if strategy not in EXECUTABLE_STRATEGIES or not raw.get("executable"):
        raise RollbackRefuseError(
            f"Strategy {strategy} is not executable by DataWrap — "
            "follow docs/MIGRATION_ROLLBACK.md (warehouse restore / staging re-land). "
            "Population undo is not claimed."
        )

    actor = (approved_by or "").strip()
    why = (reason or "").strip()
    if not actor or not why:
        raise RollbackRefuseError(
            "Rollback execution requires approved_by and reason (audit contract)"
        )

    stage = str(raw.get("staging_table") or "").strip()
    if strategy == "DISCARD_STAGING":
        if not stage:
            raise RollbackRefuseError(
                "DISCARD_STAGING requires staging_table — refuse (no target)"
            )
        if drop_staging_fn is None:
            raise RollbackRefuseError(
                "DISCARD_STAGING requires a drop_staging_fn binding — refuse"
            )
        ok = bool(drop_staging_fn(stage))
        if not ok:
            raise RollbackRefuseError(
                f"Failed to discard staging table `{stage}` — fail closed"
            )

    return {
        "ok": True,
        "rollback_id": raw.get("rollback_id"),
        "job_id": raw.get("job_id"),
        "strategy": strategy,
        "status": "executed",
        "population_undo_claimed": False,
        "staging_table": stage or None,
        "destination_table": raw.get("destination_table"),
        "audit": {
            "approved_by": actor,
            "reason": why,
            "executed_at": _now(),
            "signature": raw.get("signature"),
        },
        "guarantees": list(raw.get("guarantees") or []),
        "non_guarantees": list(raw.get("non_guarantees") or []),
        "documentation": raw.get("documentation") or "docs/MIGRATION_ROLLBACK.md",
    }


def attach_rollback_plan(
    dest_summary: dict[str, Any],
    *,
    job_id: str,
    sync_mode: str = "",
    destination_table: str = "",
    dest_type: str = "",
) -> dict[str, Any]:
    """Stamp a signed rollback plan onto destination_summary (Module 6)."""
    staging = (
        dest_summary.get("staging_table")
        or (dest_summary.get("pre_ingestion_staging") or {}).get("staging_table")
        or None
    )
    plan = plan_rollback(
        job_id=job_id,
        sync_mode=sync_mode or str(dest_summary.get("sync_mode") or ""),
        destination_table=destination_table
        or str(dest_summary.get("table") or dest_summary.get("collection") or ""),
        staging_table=str(staging) if staging else None,
        rows_written=int(
            dest_summary.get("rows_written") or dest_summary.get("records_written") or 0
        ),
        promote_blocked=bool(dest_summary.get("promote_blocked")),
        dest_type=dest_type,
    )
    dest_summary["rollback_plan"] = plan.to_dict()
    return dest_summary


def discard_staging_table(destination: Any, staging_table: str) -> bool:
    """Drop a staging table via table_manager — used by execute_rollback binding."""
    from dataclasses import replace

    from services.pre_ingestion_staging import _drop_table

    ep = replace(destination, table=staging_table, collection=staging_table)
    return bool(_drop_table(ep))
