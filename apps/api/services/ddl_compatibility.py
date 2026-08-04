"""Target DDL compatibility — real G6 validation beyond bool(mappings)."""

from __future__ import annotations

import re
from typing import Any

from services.db_type_utils import (
    NO_RELATIONAL_DDL_DESTS,
    SCHEMALESS_DESTS,
    ci_get,
    normalize_dest_kind,
)
from services.type_system import (
    ddl_type,
    decimal_precision_would_truncate,
    decimal_scale_would_truncate,
    is_lossy_coercion,
    is_precision_collapse_coercion,
    normalize_logical_type,
    specialty_carrier_would_collapse,
    string_width_would_narrow,
    vector_dim_mismatch,
    vector_dim_unknown_for_native,
)

_VARCHAR_WIDTH = re.compile(
    r"(?:n?varchar2?|n?char|character\s+varying|character)"
    r"\s*\(\s*(\d+)\s*(?:BYTE|CHAR)?\s*\)",
    re.I,
)
_UNBOUNDED_STRING = re.compile(
    r"(?:n?varchar2?|n?char|character\s+varying)\s*\(\s*max\s*\)",
    re.I,
)
_UNBOUNDED_TEXT_TYPES = re.compile(
    r"^(?:n?text|clob|nclob|longtext|mediumtext|tinytext|long\s+varchar|"
    r"string|bytes|json|jsonb|xml|super|variant)\b",
    re.I,
)
_DECIMAL_PRECISION = re.compile(r"(?:decimal|numeric|number)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)", re.I)
_NUMBERISH = re.compile(r"^(?:decimal|numeric|number|float|double|real|int|bigint|smallint)", re.I)


def _max_string_len(values: list[str]) -> int:
    return max((len(v) for v in values if v), default=0)


def parse_varchar_width(ddl: str) -> int | None:
    """Return bounded VARCHAR/CHAR/NVARCHAR width, or None if unlimited/unknown.

    ``NVARCHAR(MAX)``, ``TEXT``, ``STRING``, and bare ``VARCHAR`` are unlimited
    for write-path quarantine — only parameterized widths are enforced.
    """
    text = (ddl or "").strip()
    if not text:
        return None
    if _UNBOUNDED_STRING.search(text):
        return None
    if _UNBOUNDED_TEXT_TYPES.match(text):
        return None
    m = _VARCHAR_WIDTH.search(text)
    if not m:
        return None
    width = int(m.group(1))
    return width if width > 0 else None


def _parse_varchar_width(ddl: str) -> int | None:
    return parse_varchar_width(ddl)


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

def evaluate_ddl_compatibility(
    *,
    mappings: list[dict[str, Any]],
    source_schema: dict[str, str] | None = None,
    target_schema: dict[str, str] | None = None,
    sample_rows: list[dict] | None = None,
    table_exists: bool | None = False,
    dest_connected: bool = False,
    dest_db_type: str = "postgresql",
    allow_create: bool = False,
    backfill_new_fields: bool = False,
    schema_policy: str | None = None,
    sync_mode: str | None = None,
    destination_table: str | None = None,
    destination_pk_columns: list[str] | None = None,
    contract_primary_key: str | None = None,
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
    # Object/file sinks have no CREATE TABLE contract — sticky None must not
    # block Validate the way a SQL warehouse probe failure does.
    relational_ddl = (
        dest_kind not in SCHEMALESS_DESTS and dest_kind not in NO_RELATIONAL_DDL_DESTS
    )

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
    # table_exists=None means probe unknown — never coerce to create-new.
    if (
        dest_connected
        and relational_ddl
        and named_target
        and not overwrite
        and not target_schema
        and table_exists is True
    ):
        issues.append(
            "Could not load destination schema for existing target — "
            "Validate cannot prove mapped columns exist. Re-check table/schema name "
            "and credentials, refresh destination columns on Map, or use "
            "full_refresh_overwrite to recreate the table."
        )
    if (
        dest_connected
        and relational_ddl
        and named_target
        and not overwrite
        and table_exists is None
    ):
        issues.append(
            "Destination table existence is unknown (introspect probe failed) — "
            "re-check credentials/schema/table before Validate can approve create-new "
            "or existing-table DDL. Do not assume the table is missing."
        )
    if (
        dest_connected
        and relational_ddl
        and named_target
        and overwrite
        and table_exists is None
    ):
        # Overwrite still needs a proven existence probe — type hints alone are
        # not enough to unlock Execute against an unknown table.
        issues.append(
            "Destination table existence is unknown — Validate cannot prove "
            "overwrite DDL. Re-check credentials/schema/table or refresh "
            "destination columns on Map."
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

        if not schemaless and table_exists is True and target_schema and tgt_type is None:
            if will_add_columns:
                continue
            issues.append(
                f"Target column '{tgt}' does not exist in destination table; "
                "enable backfill new fields / create-new mapping so Datawrap can ADD COLUMN, "
                "or remap onto an existing column"
            )
            continue

        if not schemaless and tgt_type and vector_dim_mismatch(src_type, tgt_type):
            issues.append(
                f"Vector dimension mismatch: {src} ({src_type}) → {tgt} ({tgt_type})"
            )
        if (
            not schemaless
            and vector_dim_unknown_for_native(src_type, dest_kind)
            and normalize_logical_type(tgt_type or src_type) == "vector"
        ):
            issues.append(
                f"Vector dimension unknown: {src} ({src_type}) → {tgt} "
                f"— native VECTOR requires VECTOR(n); refuse invented default width"
            )
        if (
            not schemaless
            and decimal_scale_would_truncate(src_type, dest_kind)
            and normalize_logical_type(tgt_type or "") not in {"string", "text", "json"}
        ):
            issues.append(
                f"Lossy type coercion: {src} ({src_type}) → {tgt} ({tgt_type or 'proposed'}) "
                f"— scale truncates on {dest_kind}"
            )
        if (
            not schemaless
            and decimal_precision_would_truncate(src_type, dest_kind)
            and normalize_logical_type(tgt_type or "") not in {"string", "text", "json"}
        ):
            issues.append(
                f"Lossy type coercion: {src} ({src_type}) → {tgt} ({tgt_type or 'proposed'}) "
                f"— precision clamps on {dest_kind}"
            )
        risk_ack = bool(
            m.get("risk_acknowledged") or m.get("riskAcknowledged")
        )
        if not schemaless and tgt_type and is_lossy_coercion(src_type, tgt_type, dest_db=dest_kind):
            # Align with G3: declared lossy never soft-passes on head samples
            # without explicit Map risk_acknowledged. Accept risk clears the DDL
            # gate (still warn via G3) so Map CTA and Validate agree.
            src_logical = normalize_logical_type(src_type)
            tgt_logical = normalize_logical_type(tgt_type)
            # Accept risk clears declared lossy / fidelity collapse (G3/G4 parity).
            # Without ack: never sample soft-pass precision collapses.
            if not risk_ack:
                note = ""
                if src_logical == "float" and tgt_logical == "decimal":
                    note = " — float→decimal (IEEE precision risk; accept risk or remap)"
                elif src_logical == "float" and tgt_logical == "integer":
                    note = " — float→integer (fractional truncation risk)"
                elif src_logical == "decimal" and tgt_logical == "integer":
                    note = " — decimal→integer (scale truncation risk)"
                elif (
                    src_logical == "decimal"
                    and tgt_logical == "decimal"
                    and is_precision_collapse_coercion(src_type, tgt_type, dest_db=dest_kind)
                ):
                    note = " — DECIMAL(p,s) narrowing (scale/capacity shrink; accept risk or remap)"
                elif src_logical == "datetime" and tgt_logical == "date":
                    note = " — datetime→date (time-of-day truncation; accept risk or remap)"
                elif src_logical == "array" and tgt_logical == "array":
                    note = " — ARRAY element type contract mismatch"
                elif "unsigned" in src_type.lower():
                    note = " — UNSIGNED→signed overflow risk (widen to BIGINT/DECIMAL)"
                elif specialty_carrier_would_collapse(src_type, tgt_type):
                    note = (
                        " — specialty polarity collapse "
                        "(prefer VARCHAR(24)/BINARY(12) for ObjectId; bare TEXT/VARCHAR drops carrier domain)"
                    )
                elif (
                    src_logical in {"string", "text"}
                    and tgt_logical in {"string", "text"}
                    and string_width_would_narrow(src_type, tgt_type)
                ):
                    note = " — VARCHAR/CHAR width narrowing (declared capacity; accept risk or remap)"
                issues.append(
                    f"Lossy type coercion: {src} ({src_type}) → {tgt} ({tgt_type}){note}"
                )

        # Declared width / specialty collapse — skip when Accept risk already cleared.
        if (
            not schemaless
            and tgt_type
            and is_precision_collapse_coercion(src_type, tgt_type, dest_db=dest_kind)
            and not risk_ack
        ):
            if specialty_carrier_would_collapse(src_type, tgt_type):
                msg = (
                    f"Lossy type coercion: {src} ({src_type}) → {tgt} ({tgt_type}) "
                    "— specialty polarity collapse "
                    "(prefer VARCHAR(24)/BINARY(12) for ObjectId; bare TEXT/VARCHAR drops carrier domain)"
                )
            elif (
                normalize_logical_type(src_type) in {"string", "text"}
                and normalize_logical_type(tgt_type) in {"string", "text"}
                and string_width_would_narrow(src_type, tgt_type)
            ):
                msg = (
                    f"Lossy type coercion: {src} ({src_type}) → {tgt} ({tgt_type}) "
                    "— VARCHAR/CHAR width narrowing (declared capacity; accept risk or remap)"
                )
            else:
                msg = ""
            if msg and msg not in issues:
                issues.append(msg)

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

        if not schemaless and table_exists is False and allow_create:
            inferred_ddl = ddl_type(dest_db_type, src_type)
            width = _parse_varchar_width(inferred_ddl)
            if width is not None and sample_rows:
                max_len = _max_string_len(_sample_values(sample_rows, src))
                if max_len > width:
                    issues.append(
                        f"Proposed DDL {inferred_ddl} for {tgt} may truncate values (max {max_len} chars)"
                    )

    # Duplicate source-PK detection is owned by G9 data_integrity so the
    # check is not duplicated here. G6 focuses on DDL shape and target
    # compatibility; G9 audits source values (duplicates, nulls, precision).

    if not schemaless and table_exists is True and target_schema:
        # Only enforced identity columns — optional FK *_id must not false-block
        # partial maps on wide warehouse schemas (client deploy confusion).
        mapped_targets = {str(m.get("target")).lower() for m in mappings if m.get("target")}
        pk_set = {
            str(c).strip().lower()
            for c in (destination_pk_columns or [])
            if str(c).strip()
        }
        if contract_primary_key and str(contract_primary_key).strip():
            pk_set.add(str(contract_primary_key).strip().lower())
        if pk_set:
            required_unmapped = [
                c
                for c in target_schema
                if c.lower() in pk_set
                and c.lower() not in mapped_targets
                and c.lower() not in {"id", "_id"}
            ]
            if required_unmapped[:3]:
                issues.append(
                    f"{len(required_unmapped)} primary-key column(s) in destination are unmapped: "
                    f"{', '.join(required_unmapped[:3])}"
                )

    # Keep issues visible even when dest is disconnected — G2 still blocks connectivity,
    # but operators must see schema hazards immediately rather than a false clean G6.
    if not dest_connected:
        return len(issues) == 0, issues

    return len(issues) == 0, issues
