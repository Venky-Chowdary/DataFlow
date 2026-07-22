"""Schema drift detection — compare live schemas against persisted transfer contracts."""

from __future__ import annotations

import re
from typing import Any

from services.db_type_utils import SCHEMALESS_DESTS, ci_get, normalize_dest_kind
from services.schema_fingerprint import fingerprint_schema, schemas_match
from services.type_system import is_lossy_coercion, normalize_logical_type


def _norm_type(value: str | None) -> str:
    return (value or "VARCHAR").strip().upper()


def _type_length(type_name: str) -> int | None:
    match = re.search(r"\((\d+)", type_name or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _unpack_schema(schema: dict[str, Any] | None) -> tuple[dict[str, str], dict[str, bool], list[str]]:
    """Accept flat col→type maps or nested {columns, nullable, primary_key}."""
    schema = schema or {}
    if "columns" in schema and isinstance(schema.get("columns"), dict):
        columns = {str(k): str(v) for k, v in schema["columns"].items()}
        nullable_raw = schema.get("nullable") or {}
        nullable = {str(k): bool(v) for k, v in nullable_raw.items()} if isinstance(nullable_raw, dict) else {}
        pk_raw = schema.get("primary_key") or schema.get("primary_keys") or []
        if isinstance(pk_raw, str):
            primary_key = [pk_raw] if pk_raw else []
        else:
            primary_key = [str(p) for p in pk_raw]
        return columns, nullable, primary_key

    columns = {str(k): str(v) for k, v in schema.items() if not str(k).startswith("_")}
    return columns, {}, []


def _is_type_widen(old_type: str, new_type: str) -> bool:
    """True when new_type can hold all values of old_type without loss."""
    old_logical = normalize_logical_type(old_type)
    new_logical = normalize_logical_type(new_type)
    if old_logical == new_logical:
        old_len = _type_length(old_type)
        new_len = _type_length(new_type)
        return (
            old_len is not None
            and new_len is not None
            and new_len > old_len
        )
    # Differing logical types: safe (non-lossy) promotions count as widens.
    return not is_lossy_coercion(old_type, new_type)


def _is_type_narrow(old_type: str, new_type: str) -> bool:
    old_logical = normalize_logical_type(old_type)
    new_logical = normalize_logical_type(new_type)
    if old_logical == new_logical:
        old_len = _type_length(old_type)
        new_len = _type_length(new_type)
        return (
            old_len is not None
            and new_len is not None
            and new_len < old_len
        )
    return is_lossy_coercion(old_type, new_type)


def classify_schema_change(
    old_schema: dict[str, Any] | None,
    new_schema: dict[str, Any] | None,
) -> dict[str, Any]:
    """Classify a schema evolution as additive vs breaking.

    Additive: new nullable columns, widen types.
    Breaking: drop/rename/type-narrow/pk change / new NOT NULL columns.
    """
    old_cols, old_null, old_pk = _unpack_schema(old_schema)
    new_cols, new_null, new_pk = _unpack_schema(new_schema)

    additive: list[dict[str, Any]] = []
    breaking: list[dict[str, Any]] = []

    old_names = set(old_cols)
    new_names = set(new_cols)
    added = sorted(new_names - old_names)
    dropped = sorted(old_names - new_names)

    # Heuristic rename: match dropped↔added by compatible (non-narrowing) types.
    # Single-pair keeps the classic path; multi-column uses greedy type matching
    # so N renames are not misclassified as N drops + N adds (false breaking).
    renamed_pairs: list[tuple[str, str]] = []
    if dropped and added:
        remaining_dropped = list(dropped)
        remaining_added = list(added)
        # Prefer exact logical-type matches, then any non-narrow pair.
        for prefer_exact in (True, False):
            for d in list(remaining_dropped):
                best: str | None = None
                for a in remaining_added:
                    if _is_type_narrow(old_cols[d], new_cols[a]):
                        continue
                    same = normalize_logical_type(old_cols[d]) == normalize_logical_type(
                        new_cols[a]
                    )
                    if prefer_exact and not same:
                        continue
                    if not prefer_exact and same:
                        continue
                    best = a
                    break
                if best is None:
                    continue
                renamed_pairs.append((d, best))
                breaking.append({
                    "kind": "rename",
                    "column": d,
                    "to": best,
                    "old_type": old_cols[d],
                    "new_type": new_cols[best],
                })
                remaining_dropped.remove(d)
                remaining_added.remove(best)
        dropped = remaining_dropped
        added = remaining_added

    for col in dropped:
        breaking.append({"kind": "drop", "column": col, "old_type": old_cols[col]})

    for col in added:
        nullable = new_null.get(col, True)
        entry = {
            "kind": "add_column",
            "column": col,
            "new_type": new_cols[col],
            "nullable": nullable,
        }
        if nullable:
            additive.append(entry)
        else:
            breaking.append({**entry, "kind": "add_not_null"})

    for col in sorted(old_names & new_names):
        old_t, new_t = old_cols[col], new_cols[col]
        same_logical = normalize_logical_type(old_t) == normalize_logical_type(new_t)
        same_length = _type_length(old_t) == _type_length(new_t)
        if same_logical and same_length:
            # Same declared type; nullability tighten is breaking.
            if col in old_null and col in new_null and old_null[col] and not new_null[col]:
                breaking.append({
                    "kind": "nullability_tighten",
                    "column": col,
                    "old_type": old_t,
                    "new_type": new_t,
                })
            continue
        if _is_type_widen(old_t, new_t):
            additive.append({
                "kind": "widen_type",
                "column": col,
                "old_type": old_t,
                "new_type": new_t,
            })
        elif _is_type_narrow(old_t, new_t):
            breaking.append({
                "kind": "narrow_type",
                "column": col,
                "old_type": old_t,
                "new_type": new_t,
            })
        elif not same_logical:
            breaking.append({
                "kind": "type_change",
                "column": col,
                "old_type": old_t,
                "new_type": new_t,
            })

    if old_pk or new_pk:
        if [c.lower() for c in old_pk] != [c.lower() for c in new_pk]:
            breaking.append({
                "kind": "primary_key_change",
                "old_primary_key": old_pk,
                "new_primary_key": new_pk,
            })

    if breaking:
        severity = "breaking"
    elif additive:
        severity = "additive"
    else:
        severity = "none"

    return {
        "additive": additive,
        "breaking": breaking,
        "severity": severity,
        "renamed": [{"from": a, "to": b} for a, b in renamed_pairs],
    }


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
    sample_rows: list[dict[str, Any]] | None = None,
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
    # Redis/Mongo/Dynamo have no DDL — target fingerprints are often synthetic from
    # mapping revisions. Fingerprint churn must not block as "Target DDL incompatible".
    if schemaless:
        target_changed = False

    mapped_sources = {str(m.get("source")) for m in mappings if m.get("source")}
    mapped_targets = {str(m.get("target")).lower() for m in mappings if m.get("target")}
    unmapped_sources = [c for c in source_columns if c not in mapped_sources]
    # Engine-managed SCD2 / CDC bookkeeping columns are not mapping orphans.
    try:
        from services.scd2_engine import SCD2_COLUMNS

        system_targets = {c.lower() for c in SCD2_COLUMNS}
    except Exception:
        system_targets = {"valid_from", "valid_to", "is_current", "row_hash"}
    system_targets |= {"_df_lsn", "df_lsn"}
    orphan_targets = [
        c
        for c in target_columns
        if c.lower() not in mapped_targets and c.lower() not in system_targets
    ]

    type_mismatches: list[dict[str, str]] = []
    if not schemaless:
        from services.coercion_probe import samples_coerce_mapping

        for m in mappings:
            src = str(m.get("source") or "")
            tgt = str(m.get("target") or "")
            if not src or not tgt:
                continue
            src_type = source_schema.get(src) or "VARCHAR"
            tgt_type = ci_get(target_schema, tgt) or "VARCHAR"
            if not (target_schema and is_lossy_coercion(src_type, tgt_type)):
                continue
            # Declared VARCHAR→NUMBER with clean numeric samples is not breaking drift.
            if sample_rows and samples_coerce_mapping(
                m,
                source_types=source_schema,
                target_types=target_schema,
                rows=sample_rows,
            ):
                continue
            type_mismatches.append({
                "source": src,
                "target": tgt,
                "source_type": src_type.upper(),
                "target_type": tgt_type.upper(),
            })

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
