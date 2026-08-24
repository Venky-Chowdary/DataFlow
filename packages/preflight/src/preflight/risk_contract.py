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


def _attr(mapping: Any, name: str, camel: str = "") -> Any:
    """Read one mapping field from either a dict payload or a model object."""
    if isinstance(mapping, dict):
        value = mapping.get(name)
        if value is None and camel:
            value = mapping.get(camel)
        return value
    value = getattr(mapping, name, None)
    if value is None and camel:
        value = getattr(mapping, camel, None)
    return value


def mapping_is_lossy(mapping: Any) -> bool:
    """Lossy cast or narrowing carrier — the contract families, not a warning."""
    if str(_attr(mapping, "fidelity") or "").strip().lower() == "lossy_cast":
        return True
    return bool(_attr(mapping, "type_narrowing", "typeNarrowing"))


def mapping_requires_risk_contract(mapping: Any) -> bool:
    """Lossy casts, narrowing, and value-mutating transforms need a contract.

    Safe normalize (trim / trim_id / email / phone / case) is Map-Ready — not a
    Migration Risk Contract path. Must stay aligned with Map
    ``isSafeNormalizeMapping``.
    """
    if is_safe_normalize_mapping(mapping):
        return False
    if mapping_is_lossy(mapping):
        return True
    return str(_attr(mapping, "fidelity") or "").strip().lower() == "mutate"


def mapping_is_structural_review(mapping: Any) -> bool:
    """STRUCT flatten / specialty identity cannot clear via bare user_override."""
    if bool(_attr(mapping, "struct_derived", "structDerived")):
        return True
    policy = str(_attr(mapping, "struct_policy", "structPolicy") or "").strip().lower()
    if policy in {"flatten_top_level_keys", "flatten_deep", "explode_rows"}:
        return True
    transform = str(_attr(mapping, "transform") or "").strip().lower()
    return transform in {"identity_specialty", "specialty"}


def mapping_operator_overridden(mapping: Any) -> bool:
    """An explicit Studio confirmation on an ambiguous (non-lossy) mapping."""
    return bool(
        _attr(mapping, "user_override", "userOverride")
        or _attr(mapping, "approved")
        or _attr(mapping, "operator_approved", "operatorApproved")
    )


def mapping_review_cleared(mapping: Any) -> bool:
    """True when the operator already resolved this mapping's review demand.

    Single owner for Validate (G4) and Execute so a green Validate cannot fail
    at Execute: a lossy / narrowing / mutating / structural mapping clears only
    with a verified continue-policy Risk Contract, and an ambiguous mapping
    clears with an explicit override.
    """
    if mapping_requires_risk_contract(mapping) or mapping_is_structural_review(mapping):
        return mapping_risk_cleared(mapping)
    return mapping_operator_overridden(mapping)


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


def _mapping_source(mapping: Any) -> str:
    if isinstance(mapping, dict):
        return str(mapping.get("source") or "").strip()
    return str(getattr(mapping, "source", None) or "").strip()


def _mapping_target(mapping: Any) -> str:
    if isinstance(mapping, dict):
        return str(mapping.get("target") or "").strip()
    return str(getattr(mapping, "target", None) or "").strip()


def partition_transform_dry_run_errors(
    errors: list[str] | None,
    mappings: list[Any] | None,
) -> tuple[list[str], list[str]]:
    """Split dry-run errors into hard blocks vs continue-policy holdouts.

    G8 already demotes cast failures when the mapping carries a verified
    continue-policy Risk Contract. G5 / G9 must use the same partition so
    signed CAST_AND_CONTINUE / QUARANTINE_ROW does not leave Sample dry-run
    and Data integrity blocked while Gate-8 passes.
    """
    import re

    hard: list[str] = []
    contracted: list[str] = []
    maps = list(mappings or [])
    by_pair: dict[tuple[str, str], Any] = {}
    by_source: dict[str, Any] = {}
    for m in maps:
        src = _mapping_source(m)
        tgt = _mapping_target(m) or src
        if src:
            by_pair[(src, tgt)] = m
            by_source.setdefault(src, m)

    pair_re = re.compile(
        r"^(?:row\s+\d+\s+)?(.+?)\s*(?:→|->)\s*(.+?)\s*:",
        re.I,
    )
    for raw in errors or []:
        line = str(raw or "").strip()
        if not line:
            continue
        # Missing source column is always a hard structural failure.
        if line.lower().startswith("source column missing"):
            hard.append(line)
            continue
        m = pair_re.match(line)
        mapping = None
        if m:
            src = m.group(1).strip()
            tgt = m.group(2).strip()
            mapping = by_pair.get((src, tgt)) or by_source.get(src)
        if mapping is not None and mapping_risk_cleared(mapping):
            contracted.append(line)
        else:
            hard.append(line)
    return hard, contracted


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
