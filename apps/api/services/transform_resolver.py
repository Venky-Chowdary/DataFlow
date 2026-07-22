"""Unified transform resolution — UI, pipeline, preflight, and write path."""

from __future__ import annotations

from typing import Any

from services.transform_engine import infer_transform_for_mapping
from services.type_system import normalize_logical_type

ENGINE_TO_UI: dict[str, str] = {
    "none": "none",
    "identity": "none",
    "trim": "trim",
    "trim_id": "trim",
    "uuid": "none",
    "upper": "upper",
    "lower": "lower",
    "date": "date_iso",
    "datetime": "date_iso",
    "time": "time_iso",
    "timestamp": "date_iso",
    "decimal": "cast_number",
    "integer": "cast_integer",
    "boolean": "cast_boolean",
    "json": "parse_json",
    "binary": "binary",
    "hash_pii": "hash_pii",
    "mask_pii": "mask_pii",
    "strip_controls": "strip_controls",
    "normalize_unicode": "strip_controls",
    "phone": "phone",
    "email": "email",
    "currency": "currency",
    "percentage": "percentage",
}

# Frontend MappingTransform → engine transform id
UI_TO_ENGINE: dict[str, str] = {
    "none": "none",
    "trim": "trim",
    "upper": "upper",
    "lower": "lower",
    "date_iso": "datetime",
    "time_iso": "time",
    "hash_pii": "hash_pii",
    "mask_pii": "mask_pii",
    "cast_number": "decimal",
    "cast_integer": "integer",
    "cast_boolean": "boolean",
    "strip_controls": "strip_controls",
    "normalize_unicode": "normalize_unicode",
    "parse_json": "json",
    "binary": "binary",
    "phone": "phone",
    "email": "email",
    "currency": "currency",
    "percentage": "percentage",
    "identity_specialty": "none",
}

# Transforms that naturally produce string output and are safe for string targets.
_STRING_TRANSFORMS: frozenset[str] = {
    "trim", "trim_id", "upper", "lower", "uuid", "hash_pii", "mask_pii", "none",
    "date", "datetime", "json", "binary", "decimal",
    "phone", "email", "url", "iban", "postal",
    "currency", "percentage", "base64", "strip_controls", "normalize_unicode",
}


def _type_compatible_transform(target_type: str, raw: str) -> bool:
    """Return True if raw transform is compatible with the target logical type."""
    t = normalize_logical_type(target_type)
    if t in {"string", "text"}:
        return raw in _STRING_TRANSFORMS
    if t == "integer":
        # Boolean coerce is valid for INTEGER destinations that store flags as 0/1
        # (SQLite BOOLEAN → INTEGER affinity, MySQL TINYINT(1), etc.).
        return raw in {"integer", "decimal", "currency", "percentage", "boolean"}
    if t == "decimal":
        return raw in {"decimal", "integer", "currency", "percentage"}
    if t == "boolean":
        return raw in {"boolean"}
    if t == "datetime":
        return raw in {"datetime", "date", "timestamp"}
    if t == "date":
        return raw in {"date", "datetime", "timestamp"}
    if t in {"json", "array"}:
        return raw in {"json", "binary", "decimal", "integer", "boolean", "date", "datetime", "uuid"}
    if t == "binary":
        return raw in {"binary"}
    if t == "uuid":
        return raw in {"uuid", "trim", "trim_id"}
    if t == "time":
        return raw in {"time", "date", "datetime"}
    return raw in _STRING_TRANSFORMS


def resolve_transform(
    mapping: dict,
    *,
    column_types: dict[str, str] | None = None,
    dest_types: dict[str, str] | None = None,
) -> str:
    """Pick engine transform for a mapping dict.

    Respects explicit transforms when they are compatible with the target type;
    otherwise falls back to a type-correct transform derived from source/target
    logical types.
    """
    column_types = column_types or {}
    dest_types = dest_types or {}
    raw = mapping.get("transform")
    if raw in UI_TO_ENGINE:
        raw = UI_TO_ENGINE[raw]

    source_type = normalize_logical_type(column_types.get(mapping["source"], "VARCHAR"))
    target_type = normalize_logical_type(
        mapping.get("target_type") or dest_types.get(mapping["target"]) or column_types.get(mapping["target"]) or "VARCHAR",
    )

    if raw and _type_compatible_transform(target_type, raw):
        return str(raw)

    return infer_transform_for_mapping(
        mapping["source"],
        mapping["target"],
        source_type,
        target_type,
    )


def attach_transforms_to_mappings(
    mappings: list[dict],
    *,
    column_types: dict[str, str] | None = None,
    dest_types: dict[str, str] | None = None,
) -> list[dict]:
    """Ensure every mapping carries a resolved engine transform."""
    out: list[dict] = []
    for m in mappings:
        enriched = dict(m)
        enriched["transform"] = resolve_transform(enriched, column_types=column_types, dest_types=dest_types)
        out.append(enriched)
    return out


def mapping_for_api(m: dict) -> dict[str, Any]:
    """Normalize mapping for API responses."""
    engine = m.get("transform") or "none"
    return {
        **m,
        "transform": engine,
        "ui_transform": ENGINE_TO_UI.get(engine, "none"),
    }
