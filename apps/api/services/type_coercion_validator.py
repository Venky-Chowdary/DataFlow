"""Validate source→target type coercions for mapping contracts."""

from __future__ import annotations

from typing import Any

from services.type_system import (
    is_lossy_coercion,
    is_precision_collapse_coercion,
    normalize_logical_type,
    resolve_mapping_target_type,
    specialty_carrier_base,
    specialty_wire_preserves_value,
    uuid_capacity_string_carrier,
)


def validate_mapping_coercions(
    mappings: list[dict],
    *,
    source_types: dict[str, str],
    target_types: dict[str, str],
    schema_policy: str = "manual_review",
    confidence_floor: float = 0.85,
    validation_mode: str = "strict",
) -> list[dict[str, Any]]:
    """Return structured coercion issues for each mapping pair.

    When ``schema_policy`` is ``type_locked`` the target type is treated as
    immutable: any logical type change is a hard blocker, regardless of
    confidence or whether the coercion is usually lossy. This prevents silent
    data loss from schema drift.

    Under ``strict`` / ``maximum``, lossy coercions always block. Under
    ``balanced`` / ``review``, declared lossy pairs warn (mirrors G3) so Map and
    Validate agree — value-level sentinel NULL loss is still enforced by
    ``coercion_probe`` during preflight.

    Same-logical pairs still run precision-collapse checks (DECIMAL/VARCHAR/TZ
    narrowing) — an early ``continue`` used to green G9 while G3/G6 blocked.
    """
    type_locked = (schema_policy or "").lower() == "type_locked"
    mode = (validation_mode or "strict").strip().lower()
    balanced = mode in {"balanced", "review"}
    floor = max(0.0, min(1.0, float(confidence_floor)))
    issues: list[dict[str, Any]] = []
    for m in mappings:
        src = m.get("source", "")
        tgt = m.get("target", "")
        src_type = source_types.get(src, "VARCHAR")
        # Create-new stamps (e.g. BQ UUID→STRING) must win over missing live DDL.
        tgt_type = resolve_mapping_target_type(
            m, target_types=target_types, source_type=src_type
        )
        src_logical = normalize_logical_type(src_type)
        tgt_logical = normalize_logical_type(tgt_type)
        # Width-safe UUID / ObjectId wires are value-preserving create-new sinks
        # (unless type_locked — contract forbids logical drift even to safe wires).
        specialty = specialty_carrier_base(src_type)
        wire_ok = (
            (src_logical == "uuid" and uuid_capacity_string_carrier(tgt_type))
            or (specialty and specialty_wire_preserves_value(specialty, tgt_type))
            or (specialty and specialty_carrier_base(tgt_type) == specialty)
        )
        if wire_ok and not type_locked:
            continue
        precision_collapse = is_precision_collapse_coercion(src_type, tgt_type)
        if src_logical == tgt_logical and not precision_collapse and not wire_ok:
            continue
        lossy = is_lossy_coercion(src_type, tgt_type) or precision_collapse
        create_new = bool(m.get("create_new"))
        # Create-new UUID→bare STRING/TEXT (BQ/Databricks/SQLite): polarity warn.
        uuid_string_create_new = bool(
            create_new
            and src_logical == "uuid"
            and tgt_logical in {"string", "text"}
            and not uuid_capacity_string_carrier(tgt_type)
        )
        if type_locked and (src_logical != tgt_logical or wire_ok):
            severity = "block"
        elif type_locked and precision_collapse:
            severity = "block"
        elif uuid_string_create_new:
            severity = "warn"
        elif lossy:
            severity = "warn" if balanced else "block"
        elif src_logical == tgt_logical:
            # Unreachable when precision_collapse is False (continued above).
            continue
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
            "validation_mode": mode,
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
