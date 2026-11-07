"""Structural array intelligence — Array<Primitive|Object> → migration strategies.

Enterprise migration SSOT (document → relational):

* Detect element shape from samples (never invent typed ARRAY<STRING> from bare
  Mongo ARRAY).
* Recommend operator strategies: JSON document wire (default / lossless),
  normalize to child table, or hybrid (parent JSON + child rows).
* Build ``child_table_spec`` from profiled object keys only — operator-approved.

Writers consume ``struct_policy`` + ``child_table_spec``; invent SSOT for the
parent column stays dialect document wire (MySQL JSON / PG JSONB / …).
"""

from __future__ import annotations

import json
import re
from typing import Any

from services.json_intelligence import (
    ARRAY_POLICY_EXPLODE,
    ARRAY_POLICY_HYBRID,
    ARRAY_POLICY_NORMALIZE_CHILD,
    STRUCT_POLICY_STORE_AS_JSON,
    _parse_array_sample,
    normalize_struct_policy,
)

# Fail-closed logical types allowed in child_table_spec.columns[].type
_CHILD_LOGICAL_TYPES = frozenset({
    "VARCHAR", "TEXT", "STRING", "INTEGER", "BIGINT", "DECIMAL", "FLOAT",
    "DOUBLE", "BOOLEAN", "JSON", "JSONB", "DATE", "DATETIME", "TIMESTAMP",
    "TIMESTAMPTZ", "UUID", "BINARY",
})

STRUCTURAL_ARRAY_OF_PRIMITIVE = "array_of_primitive"
STRUCTURAL_ARRAY_OF_OBJECT = "array_of_object"
STRUCTURAL_ARRAY_MIXED = "array_mixed"
STRUCTURAL_ARRAY_EMPTY = "array_empty"
STRUCTURAL_NOT_ARRAY = "not_array"

_MAX_CHILD_FIELDS = 32
_MAX_PROFILE_SAMPLES = 64
_MAX_CHILD_ROWS_PER_PARENT = 256

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def _infer_scalar_logical(value: Any) -> str:
    if value is None:
        return "VARCHAR"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int) and not isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, float):
        return "DECIMAL"
    if isinstance(value, (dict, list)):
        return "JSON"
    return "VARCHAR"


def classify_array_samples(samples: list[Any] | None) -> dict[str, Any]:
    """Profile array samples into a structural class + optional child fields.

    Returns keys: ``structural_class``, ``element_logical``, ``child_fields``,
    ``sample_count``, ``element_count``, ``confidence``.
    """
    rows = list(samples or [])[:_MAX_PROFILE_SAMPLES]
    arrays: list[list[Any]] = []
    for raw in rows:
        arr = _parse_array_sample(raw)
        if arr is not None:
            arrays.append(arr)
    if not arrays:
        return {
            "structural_class": STRUCTURAL_NOT_ARRAY,
            "element_logical": "",
            "child_fields": [],
            "sample_count": 0,
            "element_count": 0,
            "confidence": 0.0,
        }

    primitives = 0
    objects = 0
    nested = 0
    empty_arrays = 0
    element_count = 0
    field_types: dict[str, dict[str, int]] = {}
    scalar_types: dict[str, int] = {}

    for arr in arrays:
        if not arr:
            empty_arrays += 1
            continue
        for elem in arr[:_MAX_CHILD_ROWS_PER_PARENT]:
            element_count += 1
            if isinstance(elem, dict):
                objects += 1
                for key, val in list(elem.items())[:_MAX_CHILD_FIELDS]:
                    name = str(key).strip()
                    if not name or not _SAFE_IDENT.match(name.replace("-", "_")):
                        continue
                    safe = name.replace("-", "_")
                    bucket = field_types.setdefault(safe, {})
                    logical = _infer_scalar_logical(val)
                    bucket[logical] = bucket.get(logical, 0) + 1
            elif isinstance(elem, list):
                nested += 1
            else:
                primitives += 1
                logical = _infer_scalar_logical(elem)
                scalar_types[logical] = scalar_types.get(logical, 0) + 1

    if element_count == 0:
        return {
            "structural_class": STRUCTURAL_ARRAY_EMPTY,
            "element_logical": "VARCHAR",
            "child_fields": [],
            "sample_count": len(arrays),
            "element_count": 0,
            "confidence": 0.7,
        }

    # Majority class with mixed detection.
    if objects and primitives:
        structural = STRUCTURAL_ARRAY_MIXED
        confidence = 0.75
    elif objects and not primitives:
        structural = STRUCTURAL_ARRAY_OF_OBJECT
        confidence = 0.92 if nested == 0 else 0.8
    elif primitives and not objects:
        structural = STRUCTURAL_ARRAY_OF_PRIMITIVE
        confidence = 0.9 if nested == 0 else 0.78
    elif nested and not objects and not primitives:
        structural = STRUCTURAL_ARRAY_MIXED
        confidence = 0.7
    else:
        structural = STRUCTURAL_ARRAY_EMPTY
        confidence = 0.7

    element_logical = ""
    if structural == STRUCTURAL_ARRAY_OF_PRIMITIVE and scalar_types:
        element_logical = max(scalar_types.items(), key=lambda kv: kv[1])[0]
    elif structural == STRUCTURAL_ARRAY_OF_OBJECT:
        element_logical = "JSON"

    child_fields: list[dict[str, str]] = []
    if structural in {STRUCTURAL_ARRAY_OF_OBJECT, STRUCTURAL_ARRAY_MIXED}:
        for name, votes in list(field_types.items())[:_MAX_CHILD_FIELDS]:
            best = max(votes.items(), key=lambda kv: kv[1])[0]
            child_fields.append({"name": name, "type": best})

    return {
        "structural_class": structural,
        "element_logical": element_logical,
        "child_fields": child_fields,
        "sample_count": len(arrays),
        "element_count": element_count,
        "confidence": confidence,
        "empty_arrays": empty_arrays,
        "nested_elements": nested,
    }


def recommend_array_strategies(
    profile: dict[str, Any],
    *,
    dest_db: str = "",
    parent_column: str = "",
    parent_key_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Ranked strategy recommendations for Map (operator picks one)."""
    structural = str(profile.get("structural_class") or STRUCTURAL_NOT_ARRAY)
    db = (dest_db or "").strip().lower()
    col = str(parent_column or "array_col").strip() or "array_col"
    pk_hint = list(parent_key_columns or [])

    strategies: list[dict[str, Any]] = []

    # JSON document wire — always valid when dest has JSON/document sink.
    strategies.append({
        "id": STRUCT_POLICY_STORE_AS_JSON,
        "label": "JSON column",
        "fidelity": "lossless",
        "rank": 5 if structural != STRUCTURAL_ARRAY_OF_OBJECT else 5,
        "recommended": True,
        "detail": (
            "Store the array as a native JSON/JSONB/VARIANT document wire. "
            "No schema redesign; structure preserved. Best default for application "
            "compatibility and fast migration."
        ),
        "dest_db": db,
    })

    if structural in {STRUCTURAL_ARRAY_OF_OBJECT, STRUCTURAL_ARRAY_MIXED}:
        strategies.append({
            "id": ARRAY_POLICY_HYBRID,
            "label": "Hybrid (JSON + child table)",
            "fidelity": "lossless",
            "rank": 5,
            "recommended": structural == STRUCTURAL_ARRAY_OF_OBJECT,
            "detail": (
                "Keep parent JSON for fidelity and write a normalized child table "
                "for SQL analytics. Requires CREATE on the child table and known "
                "parent key columns."
            ),
            "child_table_spec": propose_child_table_spec(
                profile, parent_column=col, parent_key_columns=pk_hint or None
            ),
        })
        strategies.append({
            "id": ARRAY_POLICY_NORMALIZE_CHILD,
            "label": "Normalize child table",
            "fidelity": "lossless",
            "rank": 4,
            "recommended": False,
            "detail": (
                "Relational child table only (parent column kept as JSON for "
                "fail-closed fidelity unless operator clears keep_parent_json). "
                "Best for warehouse analytics; changes query shape."
            ),
            "child_table_spec": propose_child_table_spec(
                profile,
                parent_column=col,
                keep_parent_json=True,
                parent_key_columns=pk_hint or None,
            ),
        })

    if structural == STRUCTURAL_ARRAY_OF_PRIMITIVE:
        strategies.append({
            "id": ARRAY_POLICY_EXPLODE,
            "label": "Explode rows",
            "fidelity": "compatible",
            "rank": 3,
            "recommended": False,
            "detail": (
                "Duplicate parent row per element (capped). Same table — not a "
                "child table. Useful for primitive tags/skills analytics."
            ),
        })
        strategies.append({
            "id": ARRAY_POLICY_NORMALIZE_CHILD,
            "label": "Normalize value child table",
            "fidelity": "lossless",
            "rank": 4,
            "recommended": False,
            "detail": (
                "Child table with parent_fk + ordinal + value. Prefer JSON unless "
                "SQL joins on individual values are required."
            ),
            "child_table_spec": propose_child_table_spec(
                profile,
                parent_column=col,
                primitive_value=True,
                parent_key_columns=pk_hint or None,
            ),
        })

    # Sort: recommended first, then rank desc.
    strategies.sort(key=lambda s: (not s.get("recommended"), -int(s.get("rank") or 0)))
    # Ensure exactly one recommended flag for UI primary.
    seen_rec = False
    for s in strategies:
        if s.get("recommended") and not seen_rec:
            seen_rec = True
        else:
            s["recommended"] = False if seen_rec else s.get("recommended")
    if not seen_rec and strategies:
        strategies[0]["recommended"] = True
    return strategies


def propose_child_table_spec(
    profile: dict[str, Any],
    *,
    parent_column: str,
    keep_parent_json: bool = True,
    primitive_value: bool = False,
    parent_key_columns: list[str] | None = None,
) -> dict[str, Any]:
    """Build operator-editable child table contract from a profile."""
    col = re.sub(r"[^A-Za-z0-9_]", "_", str(parent_column or "items").strip()) or "items"
    child_table = f"{col}__norm"
    # Never invent parent keys — empty means Map/Validate must supply identity.
    pk_cols = [str(c).strip() for c in (parent_key_columns or []) if str(c).strip()]

    columns: list[dict[str, str]] = []
    if primitive_value or str(profile.get("structural_class")) == STRUCTURAL_ARRAY_OF_PRIMITIVE:
        el = str(profile.get("element_logical") or "VARCHAR").upper()
        if el not in _CHILD_LOGICAL_TYPES:
            el = "VARCHAR"
        columns = [{"name": "value", "type": el}]
    else:
        columns = list(profile.get("child_fields") or [])
        if not columns:
            columns = [{"name": "payload", "type": "JSON"}]

    return {
        "child_table": child_table,
        "parent_key_columns": pk_cols,
        "ordinal_column": "_df_ord",
        "columns": columns[:_MAX_CHILD_FIELDS],
        "keep_parent_json": bool(keep_parent_json),
        "needs_parent_keys": not bool(pk_cols),
    }


def validate_child_table_spec(
    spec: Any,
    *,
    known_source_columns: set[str] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Return (normalized_spec, error)."""
    if not isinstance(spec, dict):
        return None, "child_table_spec must be an object"
    child_table = str(spec.get("child_table") or "").strip()
    if not child_table or not _SAFE_IDENT.match(child_table):
        return None, "child_table must be a safe SQL identifier"
    pk = [str(c).strip() for c in (spec.get("parent_key_columns") or []) if str(c).strip()]
    if not pk:
        return None, "parent_key_columns required (no invented id)"
    for c in pk:
        if not _SAFE_IDENT.match(c):
            return None, f"parent key {c!r} is not a safe identifier"
    if known_source_columns is not None:
        known_ci = {k.lower() for k in known_source_columns}
        for c in pk:
            if c not in known_source_columns and c.lower() not in known_ci:
                return None, (
                    f"parent key {c!r} is not in source columns "
                    f"{sorted(known_source_columns)[:12]}"
                )
    ordinal = str(spec.get("ordinal_column") or "_df_ord").strip() or "_df_ord"
    if not _SAFE_IDENT.match(ordinal):
        return None, "ordinal_column must be a safe identifier"
    columns_in = list(spec.get("columns") or [])
    columns: list[dict[str, str]] = []
    for item in columns_in[:_MAX_CHILD_FIELDS]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip().replace("-", "_")
        typ = str(item.get("type") or "VARCHAR").strip().upper() or "VARCHAR"
        # Strip params — DECIMAL(10,2) → DECIMAL — then allowlist base.
        base = typ.split("(", 1)[0].strip()
        if base not in _CHILD_LOGICAL_TYPES:
            return None, f"child column type {typ!r} is not an allowlisted logical type"
        if not name or not _SAFE_IDENT.match(name):
            continue
        columns.append({"name": name, "type": base})
    if not columns:
        return None, "child_table_spec.columns must list at least one field"
    return {
        "child_table": child_table,
        "parent_key_columns": pk,
        "ordinal_column": ordinal,
        "columns": columns,
        "keep_parent_json": bool(spec.get("keep_parent_json", True)),
    }, None


def stamp_mapping_array_strategies(
    mappings: list[dict[str, Any]],
    *,
    source_samples: dict[str, list[Any]] | None = None,
    dest_db: str = "",
    parent_key_hint: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Attach structural_class + strategy recommendations on ARRAY mappings."""
    samples = source_samples or {}
    out: list[dict[str, Any]] = []
    for m in mappings:
        row = dict(m)
        src = str(row.get("source") or "").strip()
        src_type = str(row.get("source_type") or row.get("inferred_type") or "").upper()
        is_array = "ARRAY" in src_type or "LIST" in src_type or "REPEATED" in src_type
        if not is_array or not src:
            out.append(row)
            continue
        profile = classify_array_samples(samples.get(src) or row.get("samples") or [])
        row["structural_class"] = profile.get("structural_class")
        row["array_profile"] = {
            "element_logical": profile.get("element_logical"),
            "child_fields": profile.get("child_fields"),
            "sample_count": profile.get("sample_count"),
            "element_count": profile.get("element_count"),
            "confidence": profile.get("confidence"),
        }
        strategies = recommend_array_strategies(
            profile,
            dest_db=dest_db,
            parent_column=src,
            parent_key_columns=parent_key_hint,
        )
        # Inject parent key hint into child specs (never invent when missing).
        for s in strategies:
            spec = s.get("child_table_spec")
            if isinstance(spec, dict):
                if parent_key_hint:
                    spec["parent_key_columns"] = list(parent_key_hint)
                    spec["needs_parent_keys"] = False
                elif not spec.get("parent_key_columns"):
                    spec["needs_parent_keys"] = True
        row["array_strategies"] = strategies
        # Default policy: JSON unless operator already chose.
        existing = str(row.get("struct_policy") or row.get("structPolicy") or "").strip()
        if not existing:
            # Always JSON default — never silent normalize/hybrid invent.
            row["struct_policy"] = STRUCT_POLICY_STORE_AS_JSON
            proposed = next(
                (
                    s.get("child_table_spec")
                    for s in strategies
                    if s.get("id") in {
                        ARRAY_POLICY_NORMALIZE_CHILD,
                        ARRAY_POLICY_HYBRID,
                    }
                    and s.get("child_table_spec")
                ),
                None,
            )
            if proposed:
                row["proposed_child_table_spec"] = proposed
            row.setdefault(
                "reasoning",
                (row.get("reasoning") or "")
                + f" · structural {profile.get('structural_class')} → JSON default",
            )
        elif existing in {ARRAY_POLICY_NORMALIZE_CHILD, ARRAY_POLICY_HYBRID}:
            spec = row.get("child_table_spec") or row.get("proposed_child_table_spec")
            if not spec:
                for s in strategies:
                    if s.get("id") == existing and s.get("child_table_spec"):
                        row["child_table_spec"] = s["child_table_spec"]
                        break
        out.append(row)
    return out


def build_normalized_child_batches(
    headers: list[str],
    data_rows: list[list[Any]],
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str = "",
) -> tuple[list[dict[str, Any]], list[str]]:
    """Extract child-table insert batches for normalize/hybrid array policies.

    Returns ``(batches, errors)``. Missing parent keys or truncated arrays are
    errors — never silent drop.
    """
    from services.type_system import ddl_type

    batches: list[dict[str, Any]] = []
    errors: list[str] = []
    if not mappings or not headers:
        return batches, errors
    header_idx = {h: i for i, h in enumerate(headers)}
    header_idx_ci = {str(h).lower(): i for i, h in enumerate(headers)}
    known = set(headers)

    def _cell(row: list[Any], name: str) -> Any:
        i = header_idx.get(name)
        if i is None:
            i = header_idx_ci.get(str(name).lower())
        if i is None or i >= len(row):
            return None
        return row[i]

    db = (dest_db or "").strip().lower() or "mysql"

    for m in mappings:
        policy = normalize_struct_policy(
            m.get("struct_policy") or m.get("structPolicy")
        )
        if policy not in {ARRAY_POLICY_NORMALIZE_CHILD, ARRAY_POLICY_HYBRID}:
            continue
        spec_raw = m.get("child_table_spec") or m.get("childTableSpec")
        spec, err = validate_child_table_spec(
            spec_raw, known_source_columns=known
        )
        if err or not spec:
            errors.append(
                f"ARRAY {policy} on {m.get('source')!r}: {err or 'invalid child_table_spec'}"
            )
            continue
        src = str(m.get("source") or "").strip()
        if not src:
            continue
        pk_cols = list(spec["parent_key_columns"])
        ordinal = spec["ordinal_column"]
        value_cols = [c["name"] for c in spec["columns"]]
        out_cols = pk_cols + [ordinal] + value_cols
        ddl_types = []
        for _c in pk_cols:
            ddl_types.append(ddl_type(db, "VARCHAR") if db else "VARCHAR")
        ddl_types.append(ddl_type(db, "INTEGER") if db else "INTEGER")
        for c in spec["columns"]:
            ddl_types.append(ddl_type(db, c["type"]) if db else c["type"])

        child_rows: list[list[Any]] = []
        skipped_pk = 0
        truncated = 0
        for row in data_rows:
            arr = _parse_array_sample(_cell(row, src))
            if not arr:
                continue
            if len(arr) > _MAX_CHILD_ROWS_PER_PARENT:
                truncated += 1
            pk_vals = [_cell(row, k) for k in pk_cols]
            if any(v is None or v == "" for v in pk_vals):
                skipped_pk += 1
                continue
            for ord_i, elem in enumerate(arr[:_MAX_CHILD_ROWS_PER_PARENT]):
                values: list[Any] = list(pk_vals) + [ord_i]
                if isinstance(elem, dict):
                    for c in spec["columns"]:
                        values.append(elem.get(c["name"]))
                elif len(spec["columns"]) == 1:
                    values.append(elem)
                else:
                    values.append(
                        json.dumps(elem, ensure_ascii=False, separators=(",", ":"))
                        if not isinstance(elem, str)
                        else elem
                    )
                    for _ in spec["columns"][1:]:
                        values.append(None)
                child_rows.append(values)

        if skipped_pk:
            errors.append(
                f"ARRAY {policy} on {src!r}: {skipped_pk} parent row(s) missing "
                f"parent_key_columns {pk_cols} — refuse silent child drop"
            )
        if truncated:
            errors.append(
                f"ARRAY {policy} on {src!r}: {truncated} parent array(s) exceed "
                f"{_MAX_CHILD_ROWS_PER_PARENT} elements — raise cap or split; "
                f"refuse silent truncate"
            )
        if child_rows and not skipped_pk and not truncated:
            batches.append({
                "child_table": spec["child_table"],
                "columns": out_cols,
                "ddl_types": ddl_types,
                "rows": child_rows,
                "parent_source": src,
                "struct_policy": policy,
                "keep_parent_json": spec.get("keep_parent_json", True),
                "parent_key_count": len(pk_cols),
                "ordinal_column": ordinal,
            })
        elif child_rows and (skipped_pk or truncated):
            # Partial emit is forbidden — surface as error, no batch.
            pass
    return batches, errors


def array_strategy_gate_issues(
    mappings: list[dict[str, Any]] | None,
    *,
    known_source_columns: set[str] | None = None,
) -> list[str]:
    """Fail-closed preflight: normalize/hybrid require a valid child_table_spec."""
    issues: list[str] = []
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        policy = normalize_struct_policy(
            m.get("struct_policy") or m.get("structPolicy")
        )
        if policy not in {ARRAY_POLICY_NORMALIZE_CHILD, ARRAY_POLICY_HYBRID}:
            continue
        src = str(m.get("source") or "").strip() or "?"
        spec_raw = m.get("child_table_spec") or m.get("childTableSpec")
        _spec, err = validate_child_table_spec(
            spec_raw, known_source_columns=known_source_columns
        )
        if err:
            issues.append(
                f"ARRAY strategy {policy} on {src!r} needs child_table_spec: {err}. "
                f"Pick JSON column, or approve a child table contract in Map."
            )
    return issues


def parent_key_hint_from_schemas(
    source_schemas: list[dict[str, Any]] | None,
    source_columns: list[str] | None = None,
) -> list[str] | None:
    """Best-effort parent FK columns for child-table proposals (never invent)."""
    schemas = list(source_schemas or [])
    pks: list[str] = []
    for s in schemas:
        name = str(s.get("name") or "").strip()
        if not name:
            continue
        if s.get("is_primary_key") or s.get("primary_key") or s.get("is_pk"):
            pks.append(name)
    if pks:
        return pks
    names = {str(s.get("name") or "").strip() for s in schemas}
    names.update(str(c).strip() for c in (source_columns or []) if str(c).strip())
    for cand in ("_id", "id", "uuid", "guid", "pk"):
        if cand in names:
            return [cand]
    return None
