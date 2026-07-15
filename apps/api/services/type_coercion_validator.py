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
) -> list[dict[str, Any]]:
    """Return structured coercion issues for each mapping pair.

    When ``schema_policy`` is ``type_locked`` the target type is treated as
    immutable: any logical type change is a hard blocker, regardless of
    confidence or whether the coercion is usually lossy. This prevents silent
    data loss from schema drift.
    """
    type_locked = (schema_policy or "").lower() == "type_locked"
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
        if type_locked:
            severity = "block"
        else:
            severity = "block" if lossy and float(m.get("confidence", 0)) < 0.85 else "warn"
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
        })
    return issues


def coercion_blocks_transfer(issues: list[dict[str, Any]]) -> bool:
    return any(i.get("severity") == "block" for i in issues)


# Alias used by mapping_pipeline and tests
coerce_blocks_transfer = coercion_blocks_transfer
