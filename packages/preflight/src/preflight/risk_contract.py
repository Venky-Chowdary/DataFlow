"""Migration Risk Contract helpers for preflight gates (no apps/api import).

Boolean ``risk_acknowledged`` alone never clears a lossy gate. Only a verified
continue-policy contract may unlock G3/G4.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

# Must match apps/api services.migration_risk_contract.CONTINUE_POLICIES.
# RETRY excluded — fail-closed after one re-attempt; never clears Validate.
CONTINUE_POLICIES: frozenset[str] = frozenset(
    {
        "QUARANTINE_ROW",
        "SKIP_ROW",
        "CAST_AND_CONTINUE",
        "TRANSFORM_AND_CONTINUE",
        "STOP_COLUMN",
    }
)

# Align with Map FE SAFE_NORMALIZE_TRANSFORMS + apps/api migration_risk_contract.
# trim_id is the engine pipeline id preserved by uiTransformToEngine (UI shows trim).
SAFE_NORMALIZE_TRANSFORMS: frozenset[str] = frozenset(
    {
        "trim",
        "trim_id",
        "lower",
        "upper",
        "email",
        "phone",
        "strip_controls",
    }
)


def is_safe_normalize_mapping(mapping: Any) -> bool:
    """True when transform is a safe normalize and fidelity is not lossy_cast.

    Map marks these Ready without a Migration Risk Contract; G4 must match.
    """
    if isinstance(mapping, dict):
        if mapping.get("type_narrowing") or mapping.get("typeNarrowing"):
            return False
        if str(mapping.get("fidelity") or "").lower() == "lossy_cast":
            return False
        transform = str(mapping.get("transform") or "").strip().lower()
    else:
        if getattr(mapping, "type_narrowing", False):
            return False
        if str(getattr(mapping, "fidelity", None) or "").lower() == "lossy_cast":
            return False
        transform = str(getattr(mapping, "transform", None) or "").strip().lower()
    return transform in SAFE_NORMALIZE_TRANSFORMS


def _canonical_payload(payload: dict[str, Any]) -> str:
    body = {k: v for k, v in payload.items() if k != "signature"}
    return json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)


def verify_risk_contract_signature(contract: dict[str, Any] | None) -> bool:
    if not isinstance(contract, dict):
        return False
    sig = str(contract.get("signature") or "")
    if not sig:
        return False
    digest = hashlib.sha256(_canonical_payload(contract).encode("utf-8")).hexdigest()
    expected = f"mrc-sha256:{digest}"
    return sig == expected


def contract_clears_validate_block(contract: dict[str, Any] | None) -> bool:
    if not verify_risk_contract_signature(contract):
        return False
    assert isinstance(contract, dict)
    if not (str(contract.get("approved_by") or "").strip()):
        return False
    if not (str(contract.get("reason") or "").strip()):
        return False
    policy = str(contract.get("execution_policy") or "").strip().upper()
    return policy in CONTINUE_POLICIES


def mapping_has_clearing_risk_contract(mapping: Any) -> bool:
    """True when mapping carries a verified continue-policy Risk Contract."""
    raw = None
    if isinstance(mapping, dict):
        raw = mapping.get("risk_contract") or mapping.get("riskContract")
    else:
        raw = getattr(mapping, "risk_contract", None)
    if isinstance(raw, dict):
        return contract_clears_validate_block(raw)
    return False


def mapping_risk_cleared(mapping: Any) -> bool:
    """Gate clearance helper — contract only (boolean ack never sufficient)."""
    return mapping_has_clearing_risk_contract(mapping)


def sign_risk_contract_payload(payload: dict[str, Any]) -> str:
    digest = hashlib.sha256(_canonical_payload(payload).encode("utf-8")).hexdigest()
    return f"mrc-sha256:{digest}"


def make_clearing_risk_contract(
    *,
    column: str,
    source_type: str = "TEXT",
    destination_type: str = "INTEGER",
    approved_by: str = "tester@example.com",
    reason: str = "test continue policy",
    execution_policy: str = "CAST_AND_CONTINUE",
    **extra: Any,
) -> dict[str, Any]:
    """Build a verified continue-policy contract for tests / Map drafts."""
    draft: dict[str, Any] = {
        "risk_id": f"mrc-test-{hashlib.sha256(column.encode()).hexdigest()[:12]}",
        "severity": "high",
        "root_cause": f"{source_type} → {destination_type}",
        "column": column,
        "source_type": source_type,
        "destination_type": destination_type,
        "transform": None,
        "rows_sampled": 0,
        "estimated_rows": None,
        "expected_failure_pct": None,
        "expected_precision_loss": True,
        "expected_truncation": False,
        "expected_nulls": False,
        "execution_policy": execution_policy,
        "quarantine_policy": "holdout_rejected_rows",
        "retry_policy": "none",
        "rollback_strategy": "DOCUMENT_ONLY",
        "approved_by": approved_by,
        "approved_at": "2026-01-01T00:00:00+00:00",
        "reason": reason,
        "proof_pack_ref": None,
        "mapping_hash": "",
        "plan_id": None,
        "target": column,
        "migration_id": str(extra.get("migration_id") or extra.get("plan_id") or ""),
        "table": str(extra.get("table") or ""),
        "loss_classification": str(
            extra.get("loss_classification") or "precision_loss"
        ),
        "version": 1,
        "metadata": {},
    }
    draft.update(extra)
    # Re-stamp after extra so signature covers final field set.
    draft.setdefault("migration_id", draft.get("plan_id") or "")
    draft.setdefault("table", "")
    draft.setdefault("loss_classification", "precision_loss")
    draft["signature"] = sign_risk_contract_payload(draft)
    return draft
