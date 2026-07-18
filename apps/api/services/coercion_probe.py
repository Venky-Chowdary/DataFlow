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
DEFAULT_SAMPLE_LIMIT = 200


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
            f"value(s) cannot be cast to {target_logical} (e.g. {examples}). Widen the "
            f"destination column to {safe_type or 'VARCHAR'} to preserve every value, "
            f"or keep the type and quarantine non-castable rows (they are surfaced, "
            f"never silently dropped)."
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
        sample_failures: list[dict[str, Any]] = []
        sentinel_examples: list[dict[str, Any]] = []
        raw_failure_values: list[str] = []
        observed_values: list[str] = []

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
                ok += 1

        # Only report columns that carry real coercion risk: a typed target with
        # values that fail or get placeholder-nulled, or where source disagrees
        # with the destination logical type (genuine coercion happening).
        coercion_required = src_logical != tgt_logical
        if failed == 0 and sentinel_nulls == 0 and not coercion_required:
            continue

        if failed:
            severity = "block"
        elif sentinel_nulls:
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

        entry = {
            "source": src,
            "target": m.get("target", src),
            "source_type": src_type,
            "target_type": tgt_type,
            "target_logical": tgt_logical,
            "transform": transform,
            "sampled": len(rows),
            "ok": ok,
            "nulls": nulls,
            "sentinel_nulls": sentinel_nulls,
            "failed": failed,
            "sample_failures": sample_failures,
            "sentinel_examples": sentinel_examples,
            "severity": severity,
            "suggested_fix": fix,
            "suggested_target_type": suggested_type,
            "suggested_transform": suggested_transform,
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
