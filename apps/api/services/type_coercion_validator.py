"""Validate source→target type coercions for mapping contracts."""

from __future__ import annotations

from typing import Any

from services.type_system import is_lossy_coercion, normalize_logical_type


def validate_mapping_coercions(
    mappings: list[dict],
    *,
    source_types: dict[str, str],
    target_types: dict[str, str],
    schema_policy: str = "manual_review",
    confidence_floor: float = 0.85,
) -> list[dict[str, Any]]:
    """Return structured coercion issues for each mapping pair.

    When ``schema_policy`` is ``type_locked`` the target type is treated as
    immutable: any logical type change is a hard blocker, regardless of
    confidence or whether the coercion is usually lossy. This prevents silent
    data loss from schema drift.

    Lossy coercions always block — Validate must not green-light a write that will
    fail or silently truncate at the warehouse. Confidence only affects non-lossy
    logical-type changes under non-type_locked policies.
    """
    type_locked = (schema_policy or "").lower() == "type_locked"
    floor = max(0.0, min(1.0, float(confidence_floor)))
    issues: list[dict[str, Any]] = []
    for m in mappings:
        src = m.get("source", "")
        tgt = m.get("target", "")
        src_type = source_types.get(src, "VARCHAR")
        tgt_type = target_types.get(tgt, src_type)
        src_logical = normalize_logical_type(src_type)
        tgt_logical = normalize_logical_type(tgt_type)
        if src_logical == tgt_logical:
            continue
        lossy = is_lossy_coercion(src_type, tgt_type)
        if type_locked or lossy:
            severity = "block"
        else:
            severity = "block" if float(m.get("confidence", 0)) < floor else "warn"
        issues.append({
            "source": src,
            "target": tgt,
            "source_type": src_type,
            "target_type": tgt_type,
            "source_logical": src_logical,
            "target_logical": tgt_logical,
            "lossy": lossy,
            "severity": severity,
            "message": f"{src} ({src_type}) → {tgt} ({tgt_type})",
            "suggested_fix": (
                f"Remap '{src}' to a compatible {tgt_logical} column, or change the "
                f"target type — '{src}' ({src_logical}) does not safely become {tgt_logical}."
                if severity == "block"
                else None
            ),
        })
    return issues


def coercion_blocks_transfer(issues: list[dict[str, Any]]) -> bool:
    return any(i.get("severity") == "block" for i in issues)


# Alias used by mapping_pipeline and tests
coerce_blocks_transfer = coercion_blocks_transfer
