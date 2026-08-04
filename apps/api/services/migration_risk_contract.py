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
    "STOP_COLUMN",
    "QUARANTINE_ROW",
    "SKIP_ROW",
    "RETRY",
    "CAST_AND_CONTINUE",
    "TRANSFORM_AND_CONTINUE",
    "ABORT_TRANSACTION",
]

# Policies that may clear Validate / Execute for a lossy mapping.
# RETRY is intentionally excluded — there is no cell-level retry engine; claiming
# RETRY as a continue policy would invent silent quarantine (fail closed).
CONTINUE_POLICIES: frozenset[str] = frozenset(
    {
        "QUARANTINE_ROW",
        "SKIP_ROW",
        "CAST_AND_CONTINUE",
        "TRANSFORM_AND_CONTINUE",
    }
)

# Policies that record awareness but keep Execute locked / fail closed on write.
FAIL_CLOSED_POLICIES: frozenset[str] = frozenset(
    {
        "FAIL_JOB",
        "STOP_TABLE",
        "STOP_COLUMN",
        "ABORT_TRANSACTION",
        "RETRY",  # reserved — no cell retry engine; write fails closed
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
    # Charter audit fields (Enterprise GA) — included in newly signed bodies.
    migration_id: str = ""
    table: str = ""
    loss_classification: str = ""
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


def infer_loss_classification(
    *,
    source_type: str = "",
    destination_type: str = "",
    expected_precision_loss: bool = False,
    expected_truncation: bool = False,
    expected_nulls: bool = False,
    fidelity: str = "",
) -> str:
    """Stable loss label for audit — never invents a green/lossless class."""
    fid = (fidelity or "").strip().lower()
    if fid in {"lossy_cast", "mutate", "cast"}:
        return fid
    if expected_truncation:
        return "truncation"
    if expected_precision_loss:
        return "precision_loss"
    if expected_nulls:
        return "null_coercion"
    if (source_type or "").strip() and (destination_type or "").strip():
        return "type_narrowing_or_domain"
    return "fidelity_risk"


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
    rollback_strategy: str = "DOCUMENT_ONLY",
    proof_pack_ref: str | None = None,
    mapping_hash: str = "",
    plan_id: str | None = None,
    target: str = "",
    migration_id: str = "",
    table: str = "",
    loss_classification: str = "",
    fidelity: str = "",
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
    mig = str(migration_id or plan_id or "").strip()
    loss = (loss_classification or "").strip() or infer_loss_classification(
        source_type=source_type,
        destination_type=destination_type,
        expected_precision_loss=expected_precision_loss,
        expected_truncation=expected_truncation,
        expected_nulls=expected_nulls,
        fidelity=fidelity,
    )
    meta = dict(metadata or {})
    meta.setdefault("loss_classification", loss)
    if table:
        meta.setdefault("table", table)
    if mig:
        meta.setdefault("migration_id", mig)
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
        "migration_id": mig,
        "table": (table or "").strip(),
        "loss_classification": loss,
        "version": 1,
        "metadata": meta,
    }
    signature = sign_risk_contract(draft)
    return MigrationRiskContract(**draft, signature=signature)


def verify_risk_contract(contract: MigrationRiskContract | dict[str, Any]) -> bool:
    """True when signature matches canonical body.

    Dict path verifies the stored payload as-is (backward compatible with older
    field sets). Dataclass path uses ``to_dict()``.
    """
    if isinstance(contract, MigrationRiskContract):
        payload = contract.to_dict()
    else:
        payload = dict(contract or {})
    expected = sign_risk_contract(payload)
    return bool(payload.get("signature")) and payload.get("signature") == expected


def contract_from_dict(raw: dict[str, Any] | None) -> MigrationRiskContract | None:
    if not raw or not isinstance(raw, dict):
        return None
    try:
        meta = dict(raw.get("metadata") or {})
        mig = str(
            raw.get("migration_id")
            or meta.get("migration_id")
            or raw.get("plan_id")
            or ""
        )
        table = str(raw.get("table") or meta.get("table") or "")
        loss = str(
            raw.get("loss_classification")
            or meta.get("loss_classification")
            or ""
        )
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
            migration_id=mig,
            table=table,
            loss_classification=loss,
            version=int(raw.get("version") or 1),
            metadata=meta,
        )
    except (TypeError, ValueError):
        return None


def boolean_ack_is_execution_contract() -> bool:
    """Charter invariant — a boolean is never enough."""
    return False


def contract_clears_validate_block(contract: MigrationRiskContract | dict[str, Any] | None) -> bool:
    """Only a verified continue-policy contract may unlock lossy Validate→Execute."""
    # Dict path: verify stored payload as-is so legacy field sets still clear.
    if isinstance(contract, dict):
        if not verify_risk_contract(contract):
            return False
        if not (str(contract.get("approved_by") or "").strip()):
            return False
        if not (str(contract.get("reason") or "").strip()):
            return False
        policy = str(contract.get("execution_policy") or "").strip().upper()
        return policy in CONTINUE_POLICIES
    if not isinstance(contract, MigrationRiskContract):
        return False
    if not verify_risk_contract(contract):
        return False
    if not (contract.approved_by or "").strip() or not (contract.reason or "").strip():
        return False
    policy = (contract.execution_policy or "").strip().upper()
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
    # Verify stored dict as-is (preserves older field sets).
    if isinstance(raw, dict):
        if not verify_risk_contract(raw):
            return False
        policy = str(raw.get("execution_policy") or "").strip().upper()
        if policy not in CONTINUE_POLICIES:
            return False
        if not (str(raw.get("approved_by") or "").strip()):
            return False
        if not (str(raw.get("reason") or "").strip()):
            return False
        return True
    return contract_clears_validate_block(raw)


# Align with Map FE ``SAFE_NORMALIZE_TRANSFORMS`` — trim/case/email/phone are
# not fidelity Risk Contract paths (Approve is enough).
_SAFE_NORMALIZE_TRANSFORMS: frozenset[str] = frozenset(
    {
        "trim",
        "lower",
        "upper",
        "email",
        "phone",
        "strip_controls",
    }
)


def _is_safe_normalize_mapping(m: Any) -> bool:
    """True when transform is a safe normalize and fidelity is not lossy_cast."""
    if isinstance(m, dict):
        if m.get("type_narrowing") or m.get("typeNarrowing"):
            return False
        if str(m.get("fidelity") or "").lower() == "lossy_cast":
            return False
        transform = str(m.get("transform") or "").strip().lower()
    else:
        if getattr(m, "type_narrowing", False):
            return False
        if str(getattr(m, "fidelity", None) or "").lower() == "lossy_cast":
            return False
        transform = str(getattr(m, "transform", None) or "").strip().lower()
    return transform in _SAFE_NORMALIZE_TRANSFORMS


def lossy_mappings_missing_risk_contracts(
    mappings: list[Any],
    *,
    requires_contract: Any = None,
) -> list[str]:
    """Return source column names that need a clearing contract but lack one.

    ``requires_contract`` — optional callable(mapping) -> bool. When omitted,
    any mapping with risk_acknowledged or fidelity lossy_cast/type_narrowing
    is treated as needing a contract for Execute-approve. Safe normalize
    transforms (email/trim/case/phone) are excluded unless risk_acknowledged.
    """

    def _needs(m: Any) -> bool:
        if requires_contract is not None:
            return bool(requires_contract(m))
        if isinstance(m, dict):
            if m.get("intentional_omit") or m.get("intentionalOmit"):
                return False
            # Explicit boolean ack without contract is always incomplete.
            if m.get("risk_acknowledged") or m.get("riskAcknowledged"):
                return True
            if _is_safe_normalize_mapping(m):
                return False
            fidelity = str(m.get("fidelity") or "").lower()
            if fidelity in {"lossy_cast", "mutate", "cast"}:
                return True
            return bool(m.get("type_narrowing") or m.get("typeNarrowing"))
        if getattr(m, "intentional_omit", False):
            return False
        if getattr(m, "risk_acknowledged", False):
            return True
        if _is_safe_normalize_mapping(m):
            return False
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
    """Return verified contract from a mapping, or None.

    Verifies the stored dict as-is (legacy field sets) before hydrating the
    dataclass — never re-sign/re-serialize new defaults into the digest check.
    """
    if mapping is None:
        return None
    raw = (
        mapping.get("risk_contract") or mapping.get("riskContract")
        if isinstance(mapping, dict)
        else getattr(mapping, "risk_contract", None)
    )
    if isinstance(raw, dict):
        if not verify_risk_contract(raw):
            return None
        return contract_from_dict(raw)
    if isinstance(raw, MigrationRiskContract):
        return raw if verify_risk_contract(raw) else None
    return None


def resolve_write_action_for_mapping(
    mapping: Any,
    job_error_policy: str | None,
) -> tuple[str, str | None, str | None]:
    """Resolve effective writer action for one mapping on cell failure.

    Returns ``(write_action, execution_policy, risk_id)`` where write_action is
    ``fail`` | ``quarantine`` | ``coerce_null``.

    Rules (contract narrows / specializes job policy — never silent invent):
    - No verified contract → job error_policy
    - FAIL_JOB / STOP_* / ABORT_TRANSACTION / RETRY → ``fail`` (RETRY has no
      cell-retry engine; fail-closed)
    - QUARANTINE_ROW / SKIP_ROW → ``quarantine`` holdout
    - CAST_AND_CONTINUE / TRANSFORM_AND_CONTINUE → on failure, honor
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
        # FAIL_JOB / STOP_* / ABORT / RETRY — never invent silent continue.
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

    # Unknown policy — fail closed (never fall through to job quarantine default).
    return "fail", exec_pol or DEFAULT_EXECUTION_POLICY, risk_id


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
