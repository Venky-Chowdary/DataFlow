"""Value-aware coercion probe for preflight validation.

The declared-type coercion check (preflight gate G3) can only say "TEXT → NUMBER
looks lossy". For schemaless sources such as MongoDB — where a field is widened
to ``TEXT`` the moment two documents disagree — that produces a wall of hard
blocks even when every *actual* sampled value would coerce cleanly.

This module closes that gap by predicting the real write outcome: it resolves
the exact transform the write path would use (:func:`resolve_transform`) and
applies it to the sampled values (:func:`apply_transform`), so the preflight
verdict matches what the engine would actually do at write time. Findings are
per-column and per-value so the UI (and the AI assistant) can tell the user
*which* value in *which* row fails, why, and how to fix it.

The report is intentionally plain dicts (JSON-serializable, typed shape
documented below) so it can flow straight into the API response.

CoercionColumn shape::

    {
      "source": str, "target": str,
      "source_type": str, "target_type": str, "target_logical": str,
      "transform": str,
      "sampled": int, "ok": int, "nulls": int, "sentinel_nulls": int, "failed": int,
      "sample_failures": [{"row": int, "value": str, "reason": str}],
      "sentinel_examples": [{"row": int, "value": str}],
      "severity": "block" | "warn" | "ok",
      "suggested_fix": str,
      "suggested_target_type": str | None,
      "suggested_transform": str | None,
    }
"""

from __future__ import annotations

from typing import Any

from services.transform_engine import NULL_SENTINELS, apply_transform
from services.transform_resolver import resolve_transform
from services.type_system import ddl_type, normalize_logical_type
from services.value_serializer import cell_to_string

_TEXTUAL_LOGICALS = {"string", "text"}
_STRUCTURAL_LOGICALS = {"json", "array"}
SAMPLE_FAILURE_LIMIT = 5
# Must match preflight_service sample cap so G3/G5/G6 see the same rows.
DEFAULT_SAMPLE_LIMIT = 500
PREFLIGHT_SAMPLE_LIMIT = DEFAULT_SAMPLE_LIMIT


def samples_coerce_mapping(
    mapping: dict,
    *,
    source_types: dict[str, str],
    target_types: dict[str, str],
    rows: list[dict[str, Any]],
) -> bool:
    """True when every non-empty sample coerces via the write-path transform.

    Used by G5 integrity, G6 DDL, and schema-drift so declared VARCHAR→NUMBER
    (JSON/CSV numeric strings) does not false-block when values cast cleanly.
    """
    src = str(mapping.get("source") or "")
    if not src or not rows:
        return False
    item = dict(mapping)
    tgt = item.get("target")
    if not item.get("target_type") and tgt and target_types:
        item["target_type"] = target_types.get(str(tgt))
    transform = resolve_transform(item, column_types=source_types, dest_types=target_types)
    checked = 0
    for row in rows[:DEFAULT_SAMPLE_LIMIT]:
        raw = cell_to_string(row.get(src, ""))
        if not str(raw).strip():
            continue
        checked += 1
        _val, err = apply_transform(raw, transform)
        if err:
            return False
    return checked > 0


def samples_coerce_mapping(
    mapping: dict,
    *,
    source_types: dict[str, str],
    target_types: dict[str, str],
    rows: list[dict[str, Any]],
) -> bool:
    """True when every non-empty sample coerces via the write-path transform.

    Used by G5 integrity, G6 DDL, and schema-drift so declared VARCHAR→NUMBER
    (JSON/CSV numeric strings) does not false-block when values cast cleanly.
    """
    src = str(mapping.get("source") or "")
    if not src or not rows:
        return False
    item = dict(mapping)
    tgt = item.get("target")
    if not item.get("target_type") and tgt and target_types:
        item["target_type"] = target_types.get(str(tgt))
    transform = resolve_transform(item, column_types=source_types, dest_types=target_types)
    checked = 0
    for row in rows[:200]:
        raw = cell_to_string(row.get(src, ""))
        if not str(raw).strip():
            continue
        checked += 1
        _val, err = apply_transform(raw, transform)
        if err:
            return False
    return checked > 0


def _target_type_for(mapping: dict, dest_types: dict[str, str], source_types: dict[str, str]) -> str:
    tgt = mapping.get("target", "")
    return (
        dest_types.get(tgt)
        or mapping.get("target_type")
        or source_types.get(mapping.get("source", ""))
        or "VARCHAR"
    )


def _safe_target_type(dest_db_type: str, prefer_structural: bool) -> str | None:
    """A destination-native type that would accept the offending values.

    For a mixed/dirty column the safe move on a typed warehouse is to widen the
    column to text (or VARIANT when the values are structural) so no value is
    rejected — the user can always cast downstream.
    """
    db = (dest_db_type or "").strip().lower()
    if not db:
        return "VARCHAR" if not prefer_structural else "JSON"
    logical = "json" if prefer_structural else "text"
    try:
        return ddl_type(db, logical)
    except Exception:
        return "VARCHAR"


def _looks_structural(values: list[str]) -> bool:
    for v in values:
        s = v.strip()
        if s[:1] in ("{", "[") and s[-1:] in ("}", "]"):
            return True
    return False


def _build_suggestion(
    *,
    source: str,
    source_type: str,
    target_type: str,
    target_logical: str,
    failed: int,
    sentinel_nulls: int,
    sampled: int,
    failure_examples: list[str],
    dest_db_type: str,
    structural: bool,
) -> tuple[str, str | None, str | None]:
    """Return (human_fix, suggested_target_type, suggested_transform)."""
    if failed:
        examples = ", ".join(repr(v) for v in failure_examples[:3]) or "some values"
        if target_logical in _STRUCTURAL_LOGICALS:
            # A VARIANT/JSON column only accepts valid JSON. Mixed Mongo fields
            # (array in one doc, bare scalar in another) fail on the scalar. The
            # safe move is a text column that keeps every raw value.
            safe_type = _safe_target_type(dest_db_type, prefer_structural=False)
            fix = (
                f"Column '{source}' → {target_type}: {failed} of {sampled} sampled "
                f"value(s) are not valid JSON (e.g. {examples}). Map the column to "
                f"{safe_type or 'VARCHAR'} to keep every raw value, or normalize the "
                f"source so this field is always a JSON object/array before loading."
            )
            return fix, safe_type, None
        safe_type = _safe_target_type(dest_db_type, prefer_structural=structural)
        fix = (
            f"Column '{source}' → {target_type}: {failed} of {sampled} sampled "
            f"value(s) cannot be cast to {target_logical} (e.g. {examples}). "
            f"For a new table, create as {safe_type or 'VARCHAR'}. "
            f"For an existing typed column, remap to a text column or ALTER the "
            f"destination (mapping Widen alone does not change DDL). "
            f"Quarantine only applies after Validate passes for write-time rejects "
            f"— it never silently drops rows."
        )
        return fix, safe_type, None
    if sentinel_nulls:
        fix = (
            f"Column '{source}' → {target_type}: {sentinel_nulls} of {sampled} sampled "
            f"value(s) are placeholder/empty text and will be stored as NULL. This is "
            f"safe for a typed column; map to a text type if you need to keep the "
            f"literal placeholder text."
        )
        return fix, None, None
    return "", None, None


def analyze_coercion(
    *,
    sample_rows: list[dict[str, Any]] | None,
    mappings: list[dict[str, Any]],
    source_types: dict[str, str],
    dest_types: dict[str, str] | None = None,
    dest_db_type: str = "",
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    table_exists: bool = False,
) -> dict[str, Any]:
    """Predict per-value write coercion for each mapping against sampled rows.

    Returns a JSON-serializable report (see module docstring). When there are no
    sample rows the report is empty and callers should fall back to the
    declared-type check.
    """
    dest_types = dest_types or {}
    rows = list(sample_rows or [])[:sample_limit]
    columns: list[dict[str, Any]] = []
    by_source: dict[str, dict[str, Any]] = {}

    if not rows:
        return {
            "checked": 0,
            "sampled_rows": 0,
            "has_blocking_failures": False,
            "columns": [],
            "by_source": {},
        }

    for m in mappings:
        src = m.get("source", "")
        if not src:
            continue
        src_type = source_types.get(src, "VARCHAR")
        tgt_type = _target_type_for(m, dest_types, source_types)
        src_logical = normalize_logical_type(src_type)
        tgt_logical = normalize_logical_type(tgt_type)

        # Text/varchar targets accept any serialized value — no coercion risk.
        if tgt_logical in _TEXTUAL_LOGICALS:
            continue

        transform = resolve_transform(m, column_types=source_types, dest_types=dest_types)

        ok = nulls = sentinel_nulls = failed = 0
        wire_normalize = 0
        wire_failures = 0
        sample_failures: list[dict[str, Any]] = []
        sentinel_examples: list[dict[str, Any]] = []
        wire_examples: list[dict[str, Any]] = []
        raw_failure_values: list[str] = []
        observed_values: list[str] = []
        sample_wire_form: str | None = None

        use_wire = False
        wire_check_fn = None
        try:
            from connectors.sql_temporal import (
                dest_uses_sql_wire_probe,
                is_temporal_ddl,
                wire_check_temporal,
            )

            use_wire = dest_uses_sql_wire_probe(dest_db_type) and is_temporal_ddl(tgt_type)
            dest_l = (dest_db_type or "").strip().lower()
            if use_wire and dest_l in {"snowflake", "bigquery"}:
                from connectors.warehouse_temporal import wire_check_warehouse

                wire_check_fn = lambda val, typ, _d=dest_l: wire_check_warehouse(  # noqa: E731
                    val, typ, engine=_d
                )
            else:
                wire_check_fn = wire_check_temporal
        except ImportError:
            wire_check_fn = None

        for idx, row in enumerate(rows):
            cell = cell_to_string(row.get(src))
            observed_values.append(cell)
            converted, err = apply_transform(cell, transform)
            if err:
                failed += 1
                if len(sample_failures) < SAMPLE_FAILURE_LIMIT:
                    sample_failures.append({"row": idx, "value": cell[:120], "reason": err})
                    raw_failure_values.append(cell[:120])
            elif converted is None:
                if cell.strip() == "":
                    nulls += 1
                else:
                    sentinel_nulls += 1
                    if len(sentinel_examples) < SAMPLE_FAILURE_LIMIT:
                        sentinel_examples.append({"row": idx, "value": cell[:120]})
            else:
                # Destination-wire probe: transform-engine ISO-Z ≠ SQL/warehouse bind.
                if use_wire and wire_check_fn is not None:
                    probe_val = converted if converted is not None else cell
                    wire = wire_check_fn(probe_val, tgt_type)
                    if not wire.get("ok"):
                        wire_failures += 1
                        failed += 1
                        if len(sample_failures) < SAMPLE_FAILURE_LIMIT:
                            sample_failures.append({
                                "row": idx,
                                "value": cell[:120],
                                "reason": wire.get("reason") or "Destination wire bind failed",
                                "wire_form": wire.get("wire_value"),
                            })
                            raw_failure_values.append(cell[:120])
                        continue
                    if wire.get("wire_value") and sample_wire_form is None:
                        sample_wire_form = str(wire["wire_value"])
                    if wire.get("needs_normalize"):
                        wire_normalize += 1
                        if len(wire_examples) < SAMPLE_FAILURE_LIMIT:
                            wire_examples.append({
                                "row": idx,
                                "value": cell[:120],
                                "wire_form": wire.get("wire_value"),
                                "reason": wire.get("reason") or "Will normalize for destination",
                            })
                ok += 1

        # Only report columns that carry real coercion risk: a typed target with
        # values that fail or get placeholder-nulled, or where source disagrees
        # with the destination logical type (genuine coercion happening).
        coercion_required = src_logical != tgt_logical
        if (
            failed == 0
            and sentinel_nulls == 0
            and wire_normalize == 0
            and not coercion_required
        ):
            continue

        if failed:
            severity = "block"
        elif sentinel_nulls or wire_normalize:
            severity = "warn"
        else:
            severity = "ok"

        structural = tgt_logical in _STRUCTURAL_LOGICALS or _looks_structural(observed_values)
        fix, suggested_type, suggested_transform = _build_suggestion(
            source=src,
            source_type=src_type,
            target_type=tgt_type,
            target_logical=tgt_logical,
            failed=failed,
            sentinel_nulls=sentinel_nulls,
            sampled=len(rows),
            failure_examples=raw_failure_values,
            dest_db_type=dest_db_type,
            structural=structural,
        )
        if wire_normalize and not fix:
            example = wire_examples[0] if wire_examples else {}
            fix = (
                f"Column '{src}' → {tgt_type}: {wire_normalize} of {len(rows)} sampled "
                f"value(s) use ISO timestamps (e.g. {example.get('value', '…')!r}). "
                f"DataFlow will normalize to {example.get('wire_form') or 'YYYY-MM-DD HH:MM:SS'} "
                f"at write time for destination SQL/warehouse temporal bind."
            )

        tgt_name = str(m.get("target", src) or src)
        dest_col_exists = bool(
            table_exists and (
                tgt_name in dest_types
                or tgt_name.lower() in {k.lower() for k in dest_types}
            )
        )
        entry = {
            "source": src,
            "target": tgt_name,
            "source_type": src_type,
            "target_type": tgt_type,
            "target_logical": tgt_logical,
            "transform": transform,
            "sampled": len(rows),
            "ok": ok,
            "nulls": nulls,
            "sentinel_nulls": sentinel_nulls,
            "failed": failed,
            "wire_normalize": wire_normalize,
            "wire_failures": wire_failures,
            "sample_failures": sample_failures,
            "sentinel_examples": sentinel_examples,
            "wire_examples": wire_examples,
            "sample_wire_form": sample_wire_form,
            "severity": severity,
            "suggested_fix": fix,
            "suggested_target_type": suggested_type,
            "suggested_transform": suggested_transform,
            "destination_exists": dest_col_exists,
            "table_exists": bool(table_exists),
        }
        columns.append(entry)
        by_source[src] = entry

    return {
        "checked": len(columns),
        "sampled_rows": len(rows),
        "has_blocking_failures": any(c["severity"] == "block" for c in columns),
        "columns": columns,
        "by_source": by_source,
    }
