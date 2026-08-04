"""Migration Risk Contract — SSOT for intentional fidelity loss.

Charter: ``risk_acknowledged: bool`` is not an execution contract. Accepting a
migration risk must produce an immutable, signed artifact that records:

- what can be lost (precision / truncation / nulls)
- what the writer must do (execution policy)
- quarantine / retry / rollback posture
- who approved, when, and why

Default execution policy is FAIL_JOB — never silent continue.

See ``docs/MIGRATION_RISK_CONTRACT.md``.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

ExecutionPolicy = Literal[
    "FAIL_JOB",
    "STOP_TABLE",
    "QUARANTINE_ROW",
    "SKIP_ROW",
    "RETRY",
    "CAST_AND_CONTINUE",
    "TRANSFORM_AND_CONTINUE",
    "ABORT_TRANSACTION",
]

# Policies that may clear Validate / Execute for a lossy mapping.
CONTINUE_POLICIES: frozenset[str] = frozenset(
    {
        "QUARANTINE_ROW",
        "SKIP_ROW",
        "CAST_AND_CONTINUE",
        "TRANSFORM_AND_CONTINUE",
        "RETRY",
    }
)

# Policies that record awareness but keep Execute locked / fail closed on write.
FAIL_CLOSED_POLICIES: frozenset[str] = frozenset(
    {
        "FAIL_JOB",
        "STOP_TABLE",
        "ABORT_TRANSACTION",
    }
)

DEFAULT_EXECUTION_POLICY: ExecutionPolicy = "FAIL_JOB"

ALL_POLICIES: frozenset[str] = CONTINUE_POLICIES | FAIL_CLOSED_POLICIES


@dataclass(frozen=True)
class MigrationRiskContract:
    """Immutable operator contract for one lossy / fidelity root mapping."""

    risk_id: str
    severity: str
    root_cause: str
    column: str
    source_type: str
    destination_type: str
    transform: str | None
    rows_sampled: int
    estimated_rows: int | None
    expected_failure_pct: float | None
    expected_precision_loss: bool
    expected_truncation: bool
    expected_nulls: bool
    execution_policy: str
    quarantine_policy: str
    retry_policy: str
    rollback_strategy: str
    approved_by: str
    approved_at: str
    reason: str
    signature: str
    proof_pack_ref: str | None = None
    mapping_hash: str = ""
    plan_id: str | None = None
    target: str = ""
    version: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _canonical_payload(payload: dict[str, Any]) -> str:
    """Stable JSON for signing — signature field excluded."""
    body = {k: v for k, v in payload.items() if k != "signature"}
    return json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)


def sign_risk_contract(payload: dict[str, Any]) -> str:
    """Tamper-evident digest of the contract body (not a PKI signature)."""
    digest = hashlib.sha256(_canonical_payload(payload).encode("utf-8")).hexdigest()
    return f"mrc-sha256:{digest}"


def create_migration_risk_contract(
    *,
    column: str,
    source_type: str,
    destination_type: str,
    approved_by: str,
    reason: str,
    execution_policy: str = DEFAULT_EXECUTION_POLICY,
    root_cause: str = "",
    severity: str = "high",
    transform: str | None = None,
    rows_sampled: int = 0,
    estimated_rows: int | None = None,
    expected_failure_pct: float | None = None,
    expected_precision_loss: bool = True,
    expected_truncation: bool = False,
    expected_nulls: bool = False,
    quarantine_policy: str = "holdout_rejected_rows",
    retry_policy: str = "none",
    rollback_strategy: str = "not_productized_see_MIGRATION_ROLLBACK",
    proof_pack_ref: str | None = None,
    mapping_hash: str = "",
    plan_id: str | None = None,
    target: str = "",
    metadata: dict[str, Any] | None = None,
) -> MigrationRiskContract:
    """Build a signed contract. Default policy is FAIL_JOB (Execute stays locked)."""
    policy = (execution_policy or DEFAULT_EXECUTION_POLICY).strip().upper()
    if policy not in ALL_POLICIES:
        raise ValueError(f"Unknown execution_policy {execution_policy!r}")
    actor = (approved_by or "").strip()
    why = (reason or "").strip()
    if not actor:
        raise ValueError("approved_by is required for a Migration Risk Contract")
    if not why:
        raise ValueError("reason is required for a Migration Risk Contract")
    if not (column or "").strip():
        raise ValueError("column is required for a Migration Risk Contract")

    risk_id = f"mrc-{uuid.uuid4().hex[:16]}"
    approved_at = datetime.now(timezone.utc).isoformat()
    root = (root_cause or "").strip() or (
        f"{(source_type or '').strip() or '?'} → "
        f"{(destination_type or '').strip() or '?'} fidelity risk"
    )
    draft: dict[str, Any] = {
        "risk_id": risk_id,
        "severity": severity or "high",
        "root_cause": root,
        "column": column.strip(),
        "source_type": (source_type or "").strip(),
        "destination_type": (destination_type or "").strip(),
        "transform": transform,
        "rows_sampled": int(rows_sampled or 0),
        "estimated_rows": estimated_rows,
        "expected_failure_pct": expected_failure_pct,
        "expected_precision_loss": bool(expected_precision_loss),
        "expected_truncation": bool(expected_truncation),
        "expected_nulls": bool(expected_nulls),
        "execution_policy": policy,
        "quarantine_policy": quarantine_policy,
        "retry_policy": retry_policy,
        "rollback_strategy": rollback_strategy,
        "approved_by": actor,
        "approved_at": approved_at,
        "reason": why,
        "proof_pack_ref": proof_pack_ref,
        "mapping_hash": mapping_hash or "",
        "plan_id": plan_id,
        "target": (target or column).strip(),
        "version": 1,
        "metadata": dict(metadata or {}),
    }
    signature = sign_risk_contract(draft)
    return MigrationRiskContract(**draft, signature=signature)


def verify_risk_contract(contract: MigrationRiskContract | dict[str, Any]) -> bool:
    """True when signature matches canonical body."""
    payload = contract.to_dict() if isinstance(contract, MigrationRiskContract) else dict(contract)
    expected = sign_risk_contract(payload)
    return bool(payload.get("signature")) and payload.get("signature") == expected


def contract_from_dict(raw: dict[str, Any] | None) -> MigrationRiskContract | None:
    if not raw or not isinstance(raw, dict):
        return None
    try:
        return MigrationRiskContract(
            risk_id=str(raw.get("risk_id") or ""),
            severity=str(raw.get("severity") or "high"),
            root_cause=str(raw.get("root_cause") or ""),
            column=str(raw.get("column") or ""),
            source_type=str(raw.get("source_type") or ""),
            destination_type=str(raw.get("destination_type") or ""),
            transform=raw.get("transform"),
            rows_sampled=int(raw.get("rows_sampled") or 0),
            estimated_rows=raw.get("estimated_rows"),
            expected_failure_pct=raw.get("expected_failure_pct"),
            expected_precision_loss=bool(raw.get("expected_precision_loss")),
            expected_truncation=bool(raw.get("expected_truncation")),
            expected_nulls=bool(raw.get("expected_nulls")),
            execution_policy=str(raw.get("execution_policy") or DEFAULT_EXECUTION_POLICY),
            quarantine_policy=str(raw.get("quarantine_policy") or ""),
            retry_policy=str(raw.get("retry_policy") or ""),
            rollback_strategy=str(raw.get("rollback_strategy") or ""),
            approved_by=str(raw.get("approved_by") or ""),
            approved_at=str(raw.get("approved_at") or ""),
            reason=str(raw.get("reason") or ""),
            signature=str(raw.get("signature") or ""),
            proof_pack_ref=raw.get("proof_pack_ref"),
            mapping_hash=str(raw.get("mapping_hash") or ""),
            plan_id=raw.get("plan_id"),
            target=str(raw.get("target") or raw.get("column") or ""),
            version=int(raw.get("version") or 1),
            metadata=dict(raw.get("metadata") or {}),
        )
    except (TypeError, ValueError):
        return None


def boolean_ack_is_execution_contract() -> bool:
    """Charter invariant — a boolean is never enough."""
    return False


def contract_clears_validate_block(contract: MigrationRiskContract | dict[str, Any] | None) -> bool:
    """Only a verified continue-policy contract may unlock lossy Validate→Execute."""
    c = (
        contract
        if isinstance(contract, MigrationRiskContract)
        else contract_from_dict(contract if isinstance(contract, dict) else None)
    )
    if c is None:
        return False
    if not verify_risk_contract(c):
        return False
    if not (c.approved_by or "").strip() or not (c.reason or "").strip():
        return False
    policy = (c.execution_policy or "").strip().upper()
    return policy in CONTINUE_POLICIES


def mapping_has_clearing_risk_contract(mapping: Any) -> bool:
    """True when mapping carries a continue-policy Migration Risk Contract."""
    if mapping is None:
        return False
    raw = None
    if isinstance(mapping, dict):
        raw = mapping.get("risk_contract") or mapping.get("riskContract")
        # Legacy boolean alone never clears — charter.
        if raw is None and (
            mapping.get("risk_acknowledged") or mapping.get("riskAcknowledged")
        ):
            return False
    else:
        raw = getattr(mapping, "risk_contract", None)
        if raw is None and bool(getattr(mapping, "risk_acknowledged", False)):
            return False
    return contract_clears_validate_block(raw)


def lossy_mappings_missing_risk_contracts(
    mappings: list[Any],
    *,
    requires_contract: Any = None,
) -> list[str]:
    """Return source column names that need a clearing contract but lack one.

    ``requires_contract`` — optional callable(mapping) -> bool. When omitted,
    any mapping with risk_acknowledged or fidelity lossy_cast/type_narrowing
    is treated as needing a contract for Execute-approve.
    """

    def _needs(m: Any) -> bool:
        if requires_contract is not None:
            return bool(requires_contract(m))
        if isinstance(m, dict):
            if m.get("intentional_omit") or m.get("intentionalOmit"):
                return False
            if m.get("risk_acknowledged") or m.get("riskAcknowledged"):
                return True
            fidelity = str(m.get("fidelity") or "").lower()
            if fidelity in {"lossy_cast", "mutate", "cast"}:
                return True
            return bool(m.get("type_narrowing") or m.get("typeNarrowing"))
        if getattr(m, "intentional_omit", False):
            return False
        if getattr(m, "risk_acknowledged", False):
            return True
        fidelity = str(getattr(m, "fidelity", None) or "").lower()
        if fidelity in {"lossy_cast", "mutate", "cast"}:
            return True
        return bool(getattr(m, "type_narrowing", False))

    missing: list[str] = []
    for m in mappings or []:
        if not _needs(m):
            continue
        if mapping_has_clearing_risk_contract(m):
            continue
        if isinstance(m, dict):
            missing.append(str(m.get("source") or m.get("column") or "?"))
        else:
            missing.append(str(getattr(m, "source", None) or "?"))
    return missing


# ── Module 1b: write-path enforcement ───────────────────────────────────────

_VALID_WRITE_ACTIONS = frozenset({"fail", "quarantine", "coerce_null"})


def _normalize_job_write_policy(job_error_policy: str | None) -> str:
    selected = (job_error_policy or "quarantine").strip().lower()
    return selected if selected in _VALID_WRITE_ACTIONS else "quarantine"


def mapping_risk_contract(mapping: Any) -> MigrationRiskContract | None:
    """Return verified contract from a mapping, or None."""
    if mapping is None:
        return None
    raw = (
        mapping.get("risk_contract") or mapping.get("riskContract")
        if isinstance(mapping, dict)
        else getattr(mapping, "risk_contract", None)
    )
    c = contract_from_dict(raw if isinstance(raw, dict) else None)
    if c is None or not verify_risk_contract(c):
        return None
    return c


def resolve_write_action_for_mapping(
    mapping: Any,
    job_error_policy: str | None,
) -> tuple[str, str | None, str | None]:
    """Resolve effective writer action for one mapping on cell failure.

    Returns ``(write_action, execution_policy, risk_id)`` where write_action is
    ``fail`` | ``quarantine`` | ``coerce_null``.

    Rules (contract narrows / specializes job policy — never silent invent):
    - No verified contract → job error_policy
    - FAIL_JOB / STOP_TABLE / ABORT_TRANSACTION → ``fail`` (wins over quarantine)
    - QUARANTINE_ROW / SKIP_ROW → ``quarantine`` holdout
    - CAST_AND_CONTINUE / TRANSFORM_AND_CONTINUE / RETRY → on failure, honor
      ``quarantine_policy`` (default quarantine holdout; never invent NULL unless
      the contract's quarantine_policy explicitly asks for coerce/null)
    """
    job = _normalize_job_write_policy(job_error_policy)
    c = mapping_risk_contract(mapping)
    if c is None:
        return job, None, None

    exec_pol = (c.execution_policy or DEFAULT_EXECUTION_POLICY).strip().upper()
    risk_id = c.risk_id

    if exec_pol in FAIL_CLOSED_POLICIES:
        return "fail", exec_pol, risk_id

    if exec_pol in {"QUARANTINE_ROW", "SKIP_ROW"}:
        return "quarantine", exec_pol, risk_id

    if exec_pol in CONTINUE_POLICIES:
        qp = (c.quarantine_policy or "").strip().upper()
        if "FAIL" in qp and "QUARANTINE" not in qp:
            return "fail", exec_pol, risk_id
        if "NULL" in qp or "COERCE" in qp:
            return "coerce_null", exec_pol, risk_id
        # Default: hold out bad cells; good rows continue (cast & continue).
        return "quarantine", exec_pol, risk_id

    return job, exec_pol, risk_id


def rejected_details_require_job_abort(rejected_details: list[dict[str, Any]] | None) -> bool:
    """True when any rejected cell carries a fail-closed Migration Risk Contract."""
    for d in rejected_details or []:
        pol = str(d.get("execution_policy") or "").strip().upper()
        if pol in FAIL_CLOSED_POLICIES:
            return True
        if d.get("policy") == "fail" and pol in FAIL_CLOSED_POLICIES:
            return True
    return False


def rejected_details_are_continue_contract_only(
    rejected_details: list[dict[str, Any]] | None,
) -> bool:
    """True when every rejection is under a continue-policy contract (quarantine OK)."""
    details = list(rejected_details or [])
    if not details:
        return False
    for d in details:
        pol = str(d.get("execution_policy") or "").strip().upper()
        if pol not in CONTINUE_POLICIES:
            return False
    return True
