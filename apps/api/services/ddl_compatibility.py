"""Target DDL compatibility — real G6 validation beyond bool(mappings)."""

from __future__ import annotations

import re
from typing import Any

from services.db_type_utils import SCHEMALESS_DESTS, ci_get, normalize_dest_kind
from services.type_system import (
    ddl_type,
    is_lossy_coercion,
    normalize_logical_type,
    vector_dim_mismatch,
)

_VARCHAR_WIDTH = re.compile(r"(?:varchar|char|character\s+varying)\s*\(\s*(\d+)\s*\)", re.I)
_DECIMAL_PRECISION = re.compile(r"(?:decimal|numeric|number)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)", re.I)
_NUMBERISH = re.compile(r"^(?:decimal|numeric|number|float|double|real|int|bigint|smallint)", re.I)


def _max_string_len(values: list[str]) -> int:
    return max((len(v) for v in values if v), default=0)


def _parse_varchar_width(ddl: str) -> int | None:
    m = _VARCHAR_WIDTH.search(ddl or "")
    return int(m.group(1)) if m else None


def _parse_decimal_capacity(ddl: str) -> tuple[int, int] | None:
    """Return (precision, scale) for DECIMAL/NUMERIC/NUMBER DDL, if present."""
    text = (ddl or "").strip()
    if not text:
        return None
    m = _DECIMAL_PRECISION.search(text)
    if m:
        precision = int(m.group(1))
        scale = int(m.group(2) or 0)
        return precision, scale
    # Bare NUMBER / DECIMAL without precision: capacity is unknown — do not
    # invent scale=0 (that falsely blocks "10.00" → DECIMAL on SQLite re-runs).
    return None


def _decimal_overflow_issue(samples: list[str], tgt: str, tgt_type: str) -> str | None:
    capacity = _parse_decimal_capacity(tgt_type)
    if not capacity or not samples:
        return None
    precision, scale = capacity
    max_int_digits = max(0, precision - scale)
    try:
        from decimal import Decimal, InvalidOperation
    except ImportError:
        return None
    for raw in samples[:50]:
        text = (raw or "").strip().replace(",", "")
        if not text or text.lower() in {"null", "none", "nan"}:
            continue
        try:
            value = Decimal(text)
        except (InvalidOperation, ValueError):
            # Non-numeric into DECIMAL is a type/coercion problem handled elsewhere.
            continue
        sign, digits, exp = value.as_tuple()
        del sign
        scale_digits = -exp if exp < 0 else 0
        int_digits = len(digits) - scale_digits if exp < 0 else len(digits) + max(exp, 0)
        if int_digits > max_int_digits or scale_digits > scale:
            return (
                f"Decimal capacity overflow: {tgt} ({tgt_type}) cannot hold sample value "
                f"'{raw[:40]}' (needs ~{int_digits},{scale_digits} vs {precision},{scale})"
            )
    return None


def _sample_values(sample_rows: list[dict] | None, column: str) -> list[str]:
    if not sample_rows:
        return []
    out: list[str] = []
    for row in sample_rows:
        val = row.get(column)
        if val is None:
            continue
        out.append(str(val).strip())
    return out


_OVERWRITE_SYNC = {
    "full_refresh_overwrite",
    "overwrite",
    "full_refresh",
    "replace",
}


def _primary_key_target(
    mappings: list[dict],
    dest_kind: str,
) -> str | None:
    """Return the target column for the identity uniqueness contract.

    Delegates to the canonical helper so G6/G8/G9 never disagree on ``*_id``.
    """
    from services.primary_key import resolve_primary_key_target

    return resolve_primary_key_target(mappings, dest_kind)


def _duplicate_pk_in_source(
    sample_rows: list[dict] | None,
    mappings: list[dict],
    *,
    dest_kind: str,
) -> list[str]:
    if not sample_rows:
        return []
    issues: list[str] = []
    src_by_tgt = {str(m.get("target") or ""): str(m.get("source") or "") for m in mappings if m.get("target")}

    pk_tgt = _primary_key_target(mappings, dest_kind)
    if not pk_tgt:
        return issues
    src = src_by_tgt.get(pk_tgt, pk_tgt)

    seen: dict[str, int] = {}
    for row in sample_rows:
        val = str(row.get(src, "")).strip()
        if not val:
            continue
        seen[val] = seen.get(val, 0) + 1
    dupes = [v for v, n in seen.items() if n > 1]
    if dupes:
        issues.append(
            f"Primary key candidate '{pk_tgt}' has {len(dupes)} duplicate value(s) in source sample"
        )
    return issues


def evaluate_ddl_compatibility(
    *,
    mappings: list[dict[str, Any]],
    source_schema: dict[str, str] | None = None,
    target_schema: dict[str, str] | None = None,
    sample_rows: list[dict] | None = None,
    table_exists: bool = False,
    dest_connected: bool = False,
    dest_db_type: str = "postgresql",
    allow_create: bool = False,
    backfill_new_fields: bool = False,
    schema_policy: str | None = None,
    sync_mode: str | None = None,
    destination_table: str | None = None,
) -> tuple[bool, list[str]]:
    """
    Evaluate whether mapped columns can land in the destination DDL.
    Returns (compatible, issues).
    """
    source_schema = source_schema or {}
    target_schema = target_schema or {}
    issues: list[str] = []
    dest_kind = normalize_dest_kind(dest_db_type, default="postgresql")
    schemaless = dest_kind in SCHEMALESS_DESTS

    if not mappings:
        return False, ["No column mappings defined"]

    from services.batch_progress import effective_backfill_new_fields

    # CREATE TABLE permission ≠ ALTER ADD COLUMN. Missing columns on an existing
    # table are only safe when writers will ADD COLUMN (create_new / backfill /
    # propagate_*). Otherwise preflight must fail-fast — same class as Snowflake
    # 000904 invalid identifier across every SQL destination.
    will_add_columns = effective_backfill_new_fields(
        backfill_new_fields=backfill_new_fields,
        schema_policy=schema_policy,
        mappings=mappings,
    )

    sync = (sync_mode or "").strip().lower()
    overwrite = sync in _OVERWRITE_SYNC
    named_target = bool((destination_table or "").strip())

    # Honest schema gate: empty live schema on an *existing* non-overwrite target
    # means Validate cannot prove columns exist — do not pretend the table is new.
    # When table_exists is False (create-new), empty target_schema is expected —
    # SCD2 / upsert / incremental first runs must not be blocked here.
    if (
        dest_connected
        and not schemaless
        and named_target
        and not overwrite
        and not target_schema
        and table_exists
    ):
        issues.append(
            "Could not load destination schema for existing target — "
            "Validate cannot prove mapped columns exist. Re-check table/schema name "
            "and credentials, refresh destination columns on Map, or use "
            "full_refresh_overwrite to recreate the table."
        )

    seen_targets: set[str] = set()
    for m in mappings:
        src = str(m.get("source") or "")
        tgt = str(m.get("target") or "")
        if not src or not tgt:
            issues.append("Mapping missing source or target column")
            continue
        tgt_key = tgt.lower()
        if tgt_key in seen_targets:
            issues.append(f"Duplicate target column in mapping contract: {tgt}")
        seen_targets.add(tgt_key)

        src_type = ci_get(source_schema, src) or "VARCHAR"
        tgt_type = ci_get(target_schema, tgt)

        if not schemaless and table_exists and target_schema and tgt_type is None:
            if will_add_columns:
                continue
            issues.append(
                f"Target column '{tgt}' does not exist in destination table; "
                "enable backfill new fields / create-new mapping so DataFlow can ADD COLUMN, "
                "or remap onto an existing column"
            )
            continue

        if not schemaless and tgt_type and vector_dim_mismatch(src_type, tgt_type):
            issues.append(
                f"Vector dimension mismatch: {src} ({src_type}) → {tgt} ({tgt_type})"
            )
        if not schemaless and tgt_type and is_lossy_coercion(src_type, tgt_type):
            # Sample-aware: JSON/CSV numeric strings onto warehouse NUMBER are
            # declared-lossy but write-safe when values coerce. Only hard-block
            # when samples are missing or fail the write-path transform.
            sample_ok = False
            if sample_rows:
                from services.coercion_probe import samples_coerce_mapping

                sample_ok = samples_coerce_mapping(
                    m,
                    source_types=source_schema or {},
                    target_types=target_schema or {},
                    rows=sample_rows,
                )
            if not sample_ok:
                issues.append(
                    f"Lossy type coercion: {src} ({src_type}) → {tgt} ({tgt_type})"
                )

        if not schemaless and sample_rows and tgt_type:
            samples = _sample_values(sample_rows, src)
            if samples:
                width = _parse_varchar_width(tgt_type)
                if width is not None:
                    max_len = _max_string_len(samples)
                    if max_len > width:
                        issues.append(
                            f"Value width overflow: {src} sample max {max_len} chars "
                            f"exceeds {tgt} ({tgt_type})"
                        )

                overflow = _decimal_overflow_issue(samples, tgt, tgt_type)
                if overflow:
                    issues.append(overflow)

                src_logical = normalize_logical_type(src_type)
                tgt_logical = normalize_logical_type(tgt_type)
                if src_logical in {"integer", "decimal"} and tgt_logical == "integer":
                    for s in samples[:20]:
                        if "." in s and s.replace(".", "", 1).replace("-", "", 1).isdigit():
                            issues.append(
                                f"Fractional source values for {src} cannot fit integer target {tgt}"
                            )
                            break

        if not schemaless and not table_exists and allow_create:
            inferred_ddl = ddl_type(dest_db_type, src_type)
            width = _parse_varchar_width(inferred_ddl)
            if width is not None and sample_rows:
                max_len = _max_string_len(_sample_values(sample_rows, src))
                if max_len > width:
                    issues.append(
                        f"Proposed DDL {inferred_ddl} for {tgt} may truncate values (max {max_len} chars)"
                    )

    issues.extend(_duplicate_pk_in_source(sample_rows, mappings, dest_kind=dest_kind))

    if not schemaless and table_exists and target_schema:
        mapped_targets = {str(m.get("target")).lower() for m in mappings if m.get("target")}
        required_unmapped = [
            c
            for c in target_schema
            if c.lower().endswith("_id") and c.lower() not in mapped_targets and c.lower() not in {"id", "_id"}
        ]
        if required_unmapped[:3]:
            issues.append(
                f"{len(required_unmapped)} identifier column(s) in destination are unmapped: "
                f"{', '.join(required_unmapped[:3])}"
            )

    # Keep issues visible even when dest is disconnected — G2 still blocks connectivity,
    # but operators must see schema hazards immediately rather than a false clean G6.
    if not dest_connected:
        return len(issues) == 0, issues

    return len(issues) == 0, issues
