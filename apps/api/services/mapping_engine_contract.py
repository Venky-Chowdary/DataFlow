"""Module 13 — Mapping Engine Contract.

Charter mapping fields (always explainable):
  Confidence, Semantic Evidence, Lexical Evidence, Datatype Compatibility,
  Constraint Compatibility, Historical Success, AI Explanation,
  User Overrides, Version History

Never silently overwrite an operator-locked mapping (user_override /
risk_acknowledged / approved / risk_contract).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

MAPPING_ENGINE_CONTRACT_VERSION = "mapping_engine_contract.v1"

# Keys that prove an operator has taken ownership of the column decision.
_OPERATOR_LOCK_FLAGS = (
    "user_override",
    "risk_acknowledged",
    "riskAcknowledged",
    "approved",
    "operator_approved",
    "risk_contract",
    "riskContract",
)


def is_operator_locked(mapping: dict[str, Any] | None) -> bool:
    """True when auto-map / LLM must not replace this column's decision."""
    if not isinstance(mapping, dict):
        return False
    if mapping.get("intentional_omit") or mapping.get("intentionalOmit"):
        return True
    for key in _OPERATOR_LOCK_FLAGS:
        val = mapping.get(key)
        if val is True:
            return True
        if isinstance(val, dict) and val:
            return True
        if isinstance(val, str) and val.strip():
            return True
    return False


def merge_mappings_preserve_overrides(
    baseline: list[dict[str, Any]] | None,
    proposed: list[dict[str, Any]] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Merge engine proposals without silently overwriting locked mappings.

    Locked baseline rows are kept verbatim. Proposed alternatives are attached
    as ``engine_suggestion`` for explainability — never applied silently.
    """
    base_list = [dict(m) for m in (baseline or []) if isinstance(m, dict)]
    prop_list = [dict(m) for m in (proposed or []) if isinstance(m, dict)]
    base_by_src = {
        str(m.get("source") or "").strip(): m
        for m in base_list
        if str(m.get("source") or "").strip()
    }
    prop_by_src = {
        str(m.get("source") or "").strip(): m
        for m in prop_list
        if str(m.get("source") or "").strip()
    }

    preserved = 0
    overwritten = 0  # should stay 0 for locked rows
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    for src, base in base_by_src.items():
        seen.add(src)
        prop = prop_by_src.get(src)
        if is_operator_locked(base):
            kept = dict(base)
            if prop and (
                str(prop.get("target") or "") != str(base.get("target") or "")
                or str(prop.get("transform") or "") != str(base.get("transform") or "")
                or str(prop.get("target_type") or "") != str(base.get("target_type") or "")
            ):
                kept["engine_suggestion"] = {
                    "target": prop.get("target"),
                    "target_type": prop.get("target_type") or prop.get("dest_type"),
                    "transform": prop.get("transform"),
                    "confidence": prop.get("confidence"),
                    "reasoning": prop.get("reasoning") or prop.get("ai_explanation"),
                    "suppressed": True,
                    "reason": "operator_locked_mapping_not_silently_overwritten",
                }
            out.append(kept)
            preserved += 1
            continue
        if prop:
            merged = {**base, **prop}
            # Never drop lock flags if somehow present on base.
            for key in _OPERATOR_LOCK_FLAGS:
                if base.get(key) and not merged.get(key):
                    merged[key] = base[key]
            out.append(merged)
            overwritten += 1
        else:
            out.append(base)

    for src, prop in prop_by_src.items():
        if src in seen:
            continue
        out.append(prop)

    report = {
        "contract_version": MAPPING_ENGINE_CONTRACT_VERSION,
        "operator_locked_preserved": preserved,
        "engine_applied": overwritten,
        "silent_overwrite_of_locked": 0,
        "note": (
            "Operator-locked mappings are preserved; engine alternatives attach "
            "as engine_suggestion only."
        ),
    }
    return out, report


def stamp_mapping_evidence(
    mapping: dict[str, Any],
    *,
    version: int | None = None,
    historical_success: float | None = None,
) -> dict[str, Any]:
    """Attach charter evidence fields without inventing missing science."""
    out = dict(mapping)
    conf = out.get("confidence")
    try:
        confidence = float(conf) if conf is not None else None
    except (TypeError, ValueError):
        confidence = None

    semantic = out.get("semantic_evidence")
    if not isinstance(semantic, dict):
        semantic = {
            "score": out.get("semantic_score"),
            "method": out.get("method") or out.get("assignment_strategy"),
            "tokens": out.get("semantic_tokens") or out.get("matched_tokens"),
        }

    lexical = out.get("lexical_evidence")
    if not isinstance(lexical, dict):
        lexical = {
            "score": out.get("lexical_score") or out.get("bm25_score"),
            "strategy": out.get("assignment_strategy"),
            "name_similarity": out.get("name_similarity"),
        }

    datatype = out.get("datatype_compatibility")
    if not isinstance(datatype, dict):
        datatype = {
            "source_type": out.get("source_type") or out.get("inferred_type"),
            "target_type": out.get("target_type") or out.get("dest_type"),
            "conversion_class": out.get("conversion_class"),
            "fidelity": out.get("fidelity"),
            "invents_capacity": out.get("invents_capacity"),
        }

    constraint = out.get("constraint_compatibility")
    if not isinstance(constraint, dict):
        constraint = {
            "notes": out.get("constraint_notes") or [],
            "pk": out.get("is_primary_key") or out.get("primary_key"),
            "fk": out.get("is_foreign_key"),
        }

    ai_explanation = (
        out.get("ai_explanation")
        or out.get("reasoning")
        or out.get("llm_reasoning")
        or ""
    )

    overrides = out.get("user_overrides")
    if not isinstance(overrides, dict):
        overrides = {
            "user_override": bool(out.get("user_override")),
            "risk_acknowledged": bool(
                out.get("risk_acknowledged") or out.get("riskAcknowledged")
            ),
            "approved": bool(out.get("approved") or out.get("operator_approved")),
            "has_risk_contract": bool(out.get("risk_contract") or out.get("riskContract")),
            "locked": is_operator_locked(out),
        }

    history = out.get("version_history")
    if not isinstance(history, list):
        history = []
    if version is not None:
        history = [
            *history,
            {
                "version": int(version),
                "at": datetime.now(timezone.utc).isoformat(),
                "target": out.get("target"),
                "target_type": out.get("target_type") or out.get("dest_type"),
                "transform": out.get("transform"),
            },
        ][-20:]

    out.update(
        {
            "confidence": confidence,
            "semantic_evidence": semantic,
            "lexical_evidence": lexical,
            "datatype_compatibility": datatype,
            "constraint_compatibility": constraint,
            "historical_success": historical_success
            if historical_success is not None
            else out.get("historical_success"),
            "ai_explanation": str(ai_explanation),
            "user_overrides": overrides,
            "version_history": history,
            "mapping_engine_contract": MAPPING_ENGINE_CONTRACT_VERSION,
        }
    )
    return out


def stamp_mappings_evidence(
    mappings: list[dict[str, Any]] | None,
    *,
    version: int | None = None,
) -> list[dict[str, Any]]:
    return [
        stamp_mapping_evidence(m, version=version)
        for m in (mappings or [])
        if isinstance(m, dict)
    ]
