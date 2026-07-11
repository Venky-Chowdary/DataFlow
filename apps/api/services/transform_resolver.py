"""Unified transform resolution — UI, pipeline, preflight, and write path."""

from __future__ import annotations

from typing import Any

from services.transform_engine import infer_transform_for_mapping

# Frontend MappingTransform → engine transform id
UI_TO_ENGINE: dict[str, str] = {
    "none": "trim",
    "trim": "trim",
    "upper": "upper",
    "lower": "lower",
    "date_iso": "datetime",
    "hash_pii": "hash_pii",
    "cast_number": "decimal",
    "cast_boolean": "boolean",
}

ENGINE_TO_UI: dict[str, str] = {
    "trim": "none",
    "trim_id": "none",
    "uuid": "none",
    "upper": "upper",
    "lower": "lower",
    "date": "date_iso",
    "datetime": "date_iso",
    "decimal": "cast_number",
    "integer": "cast_number",
    "boolean": "cast_boolean",
    "json": "none",
    "binary": "none",
    "hash_pii": "hash_pii",
}


def resolve_transform(
    mapping: dict,
    *,
    column_types: dict[str, str] | None = None,
    dest_types: dict[str, str] | None = None,
) -> str:
    """Pick engine transform for a mapping dict."""
    column_types = column_types or {}
    dest_types = dest_types or {}
    raw = mapping.get("transform")
    if raw in UI_TO_ENGINE:
        return UI_TO_ENGINE[raw]
    if raw in {"trim", "decimal", "integer", "boolean", "date", "datetime", "json", "binary", "hash_pii", "upper", "lower", "trim_id", "uuid"}:
        return str(raw)
    return infer_transform_for_mapping(
        mapping["source"],
        mapping["target"],
        column_types.get(mapping["source"], "VARCHAR"),
        dest_types.get(mapping["target"]) or mapping.get("target_type"),
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
    engine = m.get("transform") or "trim"
    return {
        **m,
        "transform": engine,
        "ui_transform": ENGINE_TO_UI.get(engine, "none"),
    }
