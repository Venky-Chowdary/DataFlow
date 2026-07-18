"""Schema drift detection — compare live schemas against persisted transfer contracts."""

from __future__ import annotations

from typing import Any

from services.db_type_utils import SCHEMALESS_DESTS, ci_get, normalize_dest_kind
from services.schema_fingerprint import fingerprint_schema, schemas_match
from services.type_system import is_lossy_coercion


def _norm_type(value: str | None) -> str:
    return (value or "VARCHAR").strip().upper()


def detect_schema_drift(
    *,
    source_columns: list[str],
    source_schema: dict[str, str] | None,
    target_columns: list[str] | None,
    target_schema: dict[str, str] | None,
    stored_source_fp: str = "",
    stored_target_fp: str = "",
    mappings: list[dict[str, Any]] | None = None,
    destination_db_type: str = "",
) -> dict[str, Any]:
    """
    Compare current schemas to stored fingerprints and mapping coverage.
    Returns structured drift report for preflight and plan audit.
    """
    source_schema = source_schema or {}
    target_columns = target_columns or []
    target_schema = target_schema or {}
    mappings = mappings or []
    dest_kind = normalize_dest_kind(destination_db_type)
    schemaless = dest_kind in SCHEMALESS_DESTS

    live_source_fp = fingerprint_schema(source_columns, source_schema)
    live_target_fp = fingerprint_schema(target_columns, target_schema) if target_columns else ""

    source_changed = bool(stored_source_fp) and not schemas_match(stored_source_fp, source_columns, source_schema)
    target_changed = bool(stored_target_fp and target_columns) and stored_target_fp != live_target_fp

    mapped_sources = {str(m.get("source")) for m in mappings if m.get("source")}
    mapped_targets = {str(m.get("target")).lower() for m in mappings if m.get("target")}
    unmapped_sources = [c for c in source_columns if c not in mapped_sources]
    orphan_targets = [c for c in target_columns if c.lower() not in mapped_targets]

    type_mismatches: list[dict[str, str]] = []
    if not schemaless:
        for m in mappings:
            src = str(m.get("source") or "")
            tgt = str(m.get("target") or "")
            if not src or not tgt:
                continue
            src_type = source_schema.get(src) or "VARCHAR"
            tgt_type = ci_get(target_schema, tgt) or "VARCHAR"
            if target_schema and is_lossy_coercion(src_type, tgt_type):
                type_mismatches.append({"source": src, "target": tgt, "source_type": src_type.upper(), "target_type": tgt_type.upper()})

    issues: list[str] = []
    if source_changed:
        issues.append("Source schema changed since last mapping revision")
    if target_changed:
        issues.append("Destination schema changed since last mapping revision")
    if unmapped_sources:
        issues.append(f"{len(unmapped_sources)} source column(s) have no mapping")
    if orphan_targets:
        issues.append(f"{len(orphan_targets)} destination column(s) are unmapped")
    if type_mismatches:
        issues.append(f"{len(type_mismatches)} mapped column pair(s) have type mismatch")

    severity = "none"
    if source_changed or target_changed or type_mismatches:
        severity = "breaking"
    elif unmapped_sources or orphan_targets:
        severity = "warning"

    return {
        "drift_detected": bool(issues),
        "severity": severity,
        "issues": issues,
        "source_fingerprint": live_source_fp,
        "target_fingerprint": live_target_fp,
        "source_changed": source_changed,
        "target_changed": target_changed,
        "unmapped_sources": unmapped_sources,
        "orphan_targets": orphan_targets,
        "type_mismatches": type_mismatches,
        "mapping_coverage": round(len(mapped_sources) / max(len(source_columns), 1), 3),
    }
