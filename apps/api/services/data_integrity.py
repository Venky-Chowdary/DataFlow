"""
Unified data integrity audit — single orchestrator for all critical checks.

Delegates to existing modules (no duplicate logic):
  sample_quality, type_coercion_validator, transform_engine, mapping_quality, csv_validator
"""

from __future__ import annotations

import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Any

from services.column_case import column_type_or_none, lookup_row_value
from services.db_type_utils import SCHEMALESS_DESTS, normalize_dest_kind
from services.mapping_constraints import write_mappings
from services.validation_coverage import stamp_validation_coverage
from services.value_serializer import (
    is_reader_null_cell,
    project_row_cells,
    wire_cell_text,
)

# Validation mode → minimum confidence / null tolerance
_MODE_THRESHOLDS = {
    "maximum": {"confidence": 0.95, "null_rate_max": 0.0, "parse_fail_max": 0.0},
    "strict": {"confidence": 0.85, "null_rate_max": 0.05, "parse_fail_max": 0.02},
    "audit": {"confidence": 0.85, "null_rate_max": 0.05, "parse_fail_max": 0.02},
    "balanced": {"confidence": 0.75, "null_rate_max": 0.15, "parse_fail_max": 0.05},
    "migration": {"confidence": 0.75, "null_rate_max": 0.15, "parse_fail_max": 0.05},
    "discovery": {"confidence": 0.0, "null_rate_max": 1.0, "parse_fail_max": 1.0},
}


_FINANCIAL_NAME_PATTERNS = re.compile(
    r"(amount|amt|price|cost|total|balance|payment|revenue|salary|premium|fee)",
    re.IGNORECASE,
)


def _mode_config(validation_mode: str | None) -> dict[str, float]:
    mode = (validation_mode or "strict").strip().lower()
    return _MODE_THRESHOLDS.get(mode, _MODE_THRESHOLDS["strict"])


def _rows_from_samples(
    source_columns: list[str],
    source_samples: dict[str, list[str]] | None,
    sample_rows: list[dict] | None,
) -> list[dict[str, Any]]:
    if sample_rows:
        return sample_rows
    if not source_samples:
        return []
    max_len = max((len(v) for v in source_samples.values()), default=0)
    return [
        {col: (vals[i] if i < len(vals) else None) for col, vals in source_samples.items()}
        for i in range(min(max_len, 500))
    ]


def _lenient_object_store_copy(source_db_type: str, dest_kind: str) -> bool:
    """True when both ends create keys by PUT, so cell DDL is not the contract."""
    try:
        from preflight.constants import object_store_put_creates_key
    except ImportError:
        return False
    return object_store_put_creates_key(source_db_type) and object_store_put_creates_key(
        dest_kind
    )


def _check_coercion_safety(
    mappings: list[dict],
    source_types: dict[str, str],
    target_types: dict[str, str],
    *,
    dest_kind: str = "",
    source_db_type: str = "",
    schema_policy: str = "manual_review",
    validation_mode: str = "strict",
    rows: list[dict[str, Any]] | None = None,
    dest_table_exists: bool | None = None,
    proven_width_narrowings: dict | None = None,
) -> dict[str, Any]:
    from services.coercion_probe import samples_coerce_mapping
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )

    from services.timezone_policy import declared_source_column_types

    source_types = declared_source_column_types(source_types, mappings)
    floor = float(_mode_config(validation_mode)["confidence"])
    issues = validate_mapping_coercions(
        mappings,
        source_types=source_types,
        target_types=target_types,
        schema_policy=schema_policy,
        confidence_floor=floor,
        validation_mode=validation_mode,
        dest_db_type=dest_kind or "",
        dest_table_exists=dest_table_exists,
        proven_width_narrowings=proven_width_narrowings,
        # The measured cells decide questions a declared type cannot (a
        # nanosecond column whose rows hold microseconds loses nothing).
        sample_rows=rows,
    )
    mode = (validation_mode or "strict").strip().lower()
    if mode == "lenient" and _lenient_object_store_copy(source_db_type, dest_kind):
        # An object copy stores bytes. Inferred cell types are not a DDL
        # contract, and a lenient S3→S3 run must not stop on them (R8-003).
        return {
            "check": "coercion_safety",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": [
                i.get("message") or i.get("reason") or ""
                for i in issues
                if i.get("severity") in {"warn", "block"}
            ][:10],
            "object_store_copy": True,
        }

    schemaless = dest_kind in SCHEMALESS_DESTS
    if schemaless:
        # Schemaless stores values as-is — skip ordinary type-coercion blockers.
        # Pending Studio/Map stamp invent still blocks (Dynamo/ES/Redis writers
        # refuse Map logical invent under partial Studio).
        invent_blocks = [
            i
            for i in issues
            if i.get("severity") == "block"
            and (
                "pending" in str(i.get("reason") or "").lower()
                or not str(i.get("target_type") or "").strip()
            )
        ]
        if invent_blocks:
            return {
                "check": "coercion_safety",
                "passed": False,
                "blocks_transfer": True,
                "issues": invent_blocks[:20],
                "warnings": [],
            }
        return {
            "check": "coercion_safety",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": [
                i.get("message") or i.get("reason") or ""
                for i in issues
                if i.get("severity") in {"warn", "block"}
            ][:10],
        }

    # Declared VARCHAR→NUMBER is "lossy" on paper, but JSON/CSV files often store
    # numeric columns as strings. When samples coerce via the write transform
    # (same path as G3 / dry-run), do not hard-block — otherwise Validate
    # contradicts itself (G3 pass + G5/G6 block) and operators click Strip wrongly.
    sample_rows = rows or []
    type_locked = (schema_policy or "").lower() == "type_locked"
    hardened: list[dict[str, Any]] = []
    sample_cleared: list[str] = []
    for issue in issues:
        if issue.get("severity") != "block":
            hardened.append(issue)
            continue
        if type_locked or not sample_rows:
            hardened.append(issue)
            continue
        src = str(issue.get("source") or "")
        mapping = next((m for m in mappings if str(m.get("source") or "") == src), None)
        # Never sample-clear IEEE/precision collapses — head rows can look clean.
        from services.decision_kernel import (
            is_lossy_coercion,
            is_precision_collapse_coercion,
        )
        from services.column_case import column_population
        from services.type_system import resolve_mapping_target_type

        src_t = str(column_type_or_none(source_types, src) or source_types.get(src) or "")
        tgt_t = resolve_mapping_target_type(
            mapping or {"target": issue.get("target")},
            target_types=target_types,
            source_type=src_t,
            dest_db_type=dest_kind,
        )
        population = column_population(sample_rows, src)
        if mapping and is_precision_collapse_coercion(
            src_t,
            tgt_t,
            dest_db=dest_kind,
            dest_table_exists=dest_table_exists,
            population=population,
            source=str(mapping.get("source") or src),
            target=str(mapping.get("target") or ""),
            proven=proven_width_narrowings,
        ):
            hardened.append(issue)
            continue
        from services.migration_risk_contract import mapping_has_clearing_risk_contract

        risk_cleared = bool(mapping and mapping_has_clearing_risk_contract(mapping))
        # Match G3/G6: declared lossy cannot be sample-cleared without Risk Contract.
        if (
            mapping
            and is_lossy_coercion(
                src_t,
                tgt_t,
                dest_db=dest_kind,
                dest_table_exists=dest_table_exists,
                population=population,
                source=str(mapping.get("source") or src),
                target=str(mapping.get("target") or ""),
                proven=proven_width_narrowings,
            )
            and not risk_cleared
        ):
            hardened.append(issue)
            continue
        if mapping and samples_coerce_mapping(
            mapping,
            source_types=source_types,
            target_types=target_types,
            rows=sample_rows,
        ):
            # Sample coerce still requires a continue-policy contract to demote.
            if not risk_cleared:
                hardened.append(issue)
                continue
            sample_cleared.append(issue.get("message") or src)
            hardened.append({**issue, "severity": "warn", "sample_cleared": True})
            continue
        hardened.append(issue)

    blocks = [i for i in hardened if i.get("severity") == "block"]
    warnings = [i["message"] for i in hardened if i.get("severity") == "warn"][:10]
    for msg in sample_cleared[:5]:
        warnings.append(f"Sample-cleared declared coercion: {msg}")
    return {
        "check": "coercion_safety",
        "passed": len(blocks) == 0,
        "blocks_transfer": coerce_blocks_transfer(hardened),
        "issues": [i["message"] for i in blocks[:15]],
        "warnings": warnings,
    }


def _check_transform_dry_run(
    mappings: list[dict],
    source_columns: list[str],
    source_types: dict[str, str],
    rows: list[dict[str, Any]],
    *,
    dest_kind: str = "",
    target_types: dict[str, str] | None = None,
    empty_cells_as_null: bool = False,
    dest_nullability: dict[str, bool] | None = None,
    database_extract: bool = False,
    file_row_quarantine: bool = False,
) -> dict[str, Any]:
    if not rows or not mappings:
        return {"check": "transform_dry_run", "passed": True, "blocks_transfer": False, "issues": []}

    if source_columns:
        headers = source_columns
    else:
        from connectors.header_union import union_attribute_keys

        # Sparse documents omit keys: row 0 alone is not the header (D150).
        headers = union_attribute_keys(*(list(r) for r in rows if isinstance(r, dict)))
    sample_rows = [
        project_row_cells(row, headers, preserve_sql_null=database_extract)
        for row in rows[:200]
    ]
    from services.transform_engine import dry_run_sample

    # Ensure each mapping carries target_type so name heuristics (e.g. "date" in
    # posted_date_estimated) cannot override an explicit BOOLEAN/DECIMAL target.
    enriched = []
    for m in mappings:
        item = dict(m)
        tgt = item.get("target")
        if not item.get("target_type") and tgt and target_types:
            item["target_type"] = target_types.get(str(tgt))
        enriched.append(item)

    ok, errors = dry_run_sample(
        headers=headers,
        sample_rows=sample_rows,
        mappings=enriched,
        column_types=source_types,
        empty_cells_as_null=empty_cells_as_null,
        dest_nullability=dest_nullability,
        database_extract=database_extract,
        file_row_quarantine=file_row_quarantine,
    )
    # Parity with G8 / G5: continue-policy contracts demote cast failures to
    # holdouts — they must not keep G9 Data integrity blocked after Accept risk.
    try:
        from preflight.risk_contract import partition_transform_dry_run_errors
    except Exception:  # pragma: no cover — package always available in GA
        partition_transform_dry_run_errors = None  # type: ignore[assignment]

    hard_errors = list(errors or [])
    contracted: list[str] = []
    if partition_transform_dry_run_errors is not None:
        hard_errors, contracted = partition_transform_dry_run_errors(errors, enriched)
    missing_col_errors = [e for e in hard_errors if "Source column missing" in e]
    schemaless = dest_kind in SCHEMALESS_DESTS
    if schemaless and not missing_col_errors:
        # Schemaless stores values as-is; transform failures (e.g. typed casts
        # inferred from an unknown target schema) should not block preflight.
        return {
            "check": "transform_dry_run",
            "passed": True,
            "blocks_transfer": False,
            "issues": (hard_errors + contracted)[:20],
            "contracted_holdouts": contracted[:20],
            "kind": "transform_errors" if hard_errors else None,
        }
    # Contracted-only → pass (holdouts audited). Hard errors → block.
    ok = len(hard_errors) == 0
    issues = list(hard_errors[:20])
    note = None
    if not ok and issues:
        # Keep prose out of issues[] — root-cause column extractors must not
        # invent "Preflight blocked the transfer" as a fake column name.
        note = (
            "Preflight blocked the transfer (0 rows written). "
            "Findings below are for inspection — fix Map types/targets, then re-Validate. "
            "Write-time quarantine only applies after preflight passes."
        )
    result: dict[str, Any] = {
        "check": "transform_dry_run",
        "passed": ok,
        "blocks_transfer": not ok,
        "issues": issues,
        "contracted_holdouts": contracted[:20],
        "contracted_holdout_count": len(contracted),
    }
    if note:
        result["note"] = note
    if hard_errors:
        result["kind"] = "transform_errors"
    elif contracted:
        result["transform_status"] = "completed_with_contracted_holdouts"
        result["warnings"] = [
            f"Contracted holdout (continue-policy): {c}" for c in contracted[:5]
        ]
        result["note"] = (
            (result.get("note") + " ") if result.get("note") else ""
        ) + (
            f"Sample transform completed with {len(contracted)} contracted holdout(s) "
            "— not a clean pass; holdouts quarantine at write."
        )
    return result


def _column_has_numeric_value(rows: list[dict[str, Any]], column: str) -> bool:
    """True when at least one sample cell on ``column`` is a real number."""
    from services.transform_engine import decimal_wire_value

    for row in rows:
        if not isinstance(row, dict):
            continue
        if decimal_wire_value(lookup_row_value(row, column, None)) is not None:
            return True
    return False


def _demote_mixed_file_numeric_expectations(
    exp: dict[str, Any],
    rows: list[dict[str, Any]],
    mappings: list[dict],
) -> dict[str, Any]:
    """A minority non-numeric cell in a file is a quarantine, not a failed load.

    ``expect_column_values_between`` hard-blocks an ``amt`` column when one
    sample cell does not parse. The file writer quarantines that row and loads
    the rest, so Validate must not lock Execute. An all-bad column, and any
    mapping that already carries a Risk Contract, stay blocking.
    """
    blocking = list(exp.get("blocking_failures") or [])
    if not blocking:
        return exp
    quarantine_columns: set[str] = set()
    for mapping in mappings:
        raw = mapping.get("risk_contract") or mapping.get("riskContract")
        if isinstance(raw, dict):
            continue
        source = str(mapping.get("source") or "")
        target = str(mapping.get("target") or "")
        if _column_has_numeric_value(rows, source) or (
            target and _column_has_numeric_value(rows, target)
        ):
            if source:
                quarantine_columns.add(source)
            if target:
                quarantine_columns.add(target)
    kept: list[dict[str, Any]] = []
    demoted: list[dict[str, Any]] = []
    for failure in blocking:
        column = str(failure.get("column") or "")
        if (
            failure.get("expectation") == "expect_column_values_between"
            and column in quarantine_columns
        ):
            demoted.append(failure)
        else:
            kept.append(failure)
    if not demoted:
        return exp
    out = dict(exp)
    out["blocking_failures"] = kept
    out["warnings"] = list(exp.get("warnings") or []) + demoted
    out["blocks_transfer"] = len(kept) > 0
    out["passed"] = len(kept) == 0
    return out


def _check_financial_precision(
    mappings: list[dict],
    source_types: dict[str, str],
    rows: list[dict[str, Any]],
    *,
    file_row_quarantine: bool = False,
) -> dict[str, Any]:
    """Detect values that could silently lose magnitude (e.g. comma/currency parsing failures)."""
    from services.migration_risk_contract import mapping_has_clearing_risk_contract
    from services.transform_engine import apply_transform, infer_transform_for_mapping

    issues: list[str] = []
    contracted: list[str] = []
    file_notes: list[str] = []
    for m in mappings:
        src = m.get("source", "")
        tgt = m.get("target", "")
        if not _FINANCIAL_NAME_PATTERNS.search(src) and not _FINANCIAL_NAME_PATTERNS.search(tgt):
            continue
        # A signed continue-policy Migration Risk Contract on this column names
        # the write-time disposition for a value the destination will reject, so
        # the rejection is a contracted holdout, not a Validate block. A
        # magnitude shift is a silent misparse with no rejection, so it stays
        # blocking whatever the contract says.
        holdouts_contracted = mapping_has_clearing_risk_contract(m)
        src_type = column_type_or_none(source_types, src) or source_types.get(src, "VARCHAR")
        transform = m.get("transform") or infer_transform_for_mapping(
            src, tgt, src_type, m.get("target_type"),
        )
        if transform not in {"decimal", "integer", "currency", "percentage"}:
            continue
        values = [
            wire_cell_text(lookup_row_value(row, src, "")).strip()
            for row in rows
            if lookup_row_value(row, src, None) not in (None, "")
        ]
        unparseable: list[str] = []
        parsed_ok = 0
        for raw in values[:100]:
            if not raw or raw in {"0", "0.0", "0.00"}:
                if raw in {"0", "0.0", "0.00"}:
                    parsed_ok += 1
                continue
            # Schemaless missing and SQL NULL sentinels are not financial values.
            if raw in {"__DF_MISSING__", "__df_sql_null__"}:
                continue
            converted, err = apply_transform(raw, transform)
            if err:
                unparseable.append(f"{src}: unparseable financial value {raw!r}")
                continue
            # Null/missing sentinel coerced to None is a valid absence, not a parse failure.
            if converted is None:
                continue
            try:
                original_parsed, original_err = apply_transform(raw, "decimal")
                if original_err:
                    unparseable.append(f"{src}: unparseable financial value {raw!r}")
                    continue
                if original_parsed is None:
                    continue
                original = Decimal(str(original_parsed))
                result = Decimal(str(converted))
                if original != 0 and result != 0:
                    ratio = abs(result / original)
                    # Catch cents↔units (~100x) and similar scale bugs; 10x is the
                    # industry-safe floor (Airbyte/Fivetran-class precision guards).
                    if ratio < 0.1 or ratio > 10:
                        issues.append(
                            f"{src}: magnitude shift {raw!r} → {converted} (ratio {ratio:.4f})"
                        )
            except (InvalidOperation, ZeroDivisionError):
                pass
            parsed_ok += 1
        contract_present = isinstance(
            m.get("risk_contract") or m.get("riskContract"), dict
        )
        # No contract + a file + at least one good amount: quarantine the bad
        # cells. A present contract (FAIL_JOB, tampered) stays fail-closed.
        # Magnitude shifts above stay in ``issues`` either way.
        if holdouts_contracted:
            contracted.extend(unparseable)
        elif file_row_quarantine and parsed_ok > 0 and not contract_present:
            file_notes.extend(unparseable)
        else:
            issues.extend(unparseable)
    blocks = len(issues) > 0
    report: dict[str, Any] = {
        "check": "financial_precision",
        "passed": not blocks,
        "blocks_transfer": blocks,
        "issues": issues[:15],
    }
    holdout_lines = contracted + file_notes
    if holdout_lines:
        report["contracted_holdouts"] = holdout_lines[:15]
        report["contracted_holdout_count"] = len(holdout_lines)
        report["warnings"] = [
            f"Contracted holdout (continue-policy): {c}" for c in contracted[:5]
        ] + [f"File row quarantine: {c}" for c in file_notes[:5]]
    return report


def _route_stores_blank_key_as_null(
    mapping: dict[str, Any],
    *,
    source_type: str,
    empty_cells_as_null: bool,
    database_extract: bool,
) -> bool:
    """True when this route writes a ``''`` key cell as SQL NULL (D53).

    The same rules the writer applies (``services.blank_cell_contract``): a
    file blank becomes NULL in a non-text target, and a database carrier that
    cannot store ``''`` (numeric, temporal, uuid, ...) reached the wire as a
    flattened NULL. A text key keeps ``''`` as its value.
    """
    from services.blank_cell_contract import (
        _target_is_text,
        source_carrier_cannot_store_empty,
    )

    if empty_cells_as_null and not _target_is_text(mapping):
        return True
    return bool(
        database_extract and source_type and source_carrier_cannot_store_empty(source_type)
    )


def _check_required_nulls(
    mappings: list[dict],
    rows: list[dict[str, Any]],
    *,
    null_rate_max: float,
    dest_kind: str = "",
    primary_key: str | None = None,
    validation_mode: str = "strict",
    source_types: dict[str, str] | None = None,
    empty_cells_as_null: bool = False,
    database_extract: bool = False,
) -> dict[str, Any]:
    """Enforce nullability on the resolved identity key and exact canonical keys only.

    A key is missing when it is SQL NULL, or when it is ``''`` and *this route*
    stores that ``''`` as NULL (D53): a destination that folds ``''`` to NULL
    (Oracle VARCHAR2), a file blank landing in a non-text column (the blank-cell
    contract writes it as NULL), or a database carrier that cannot hold ``''``.
    Anywhere else ``''`` is a key value and the write lands it as one — counting
    it here refused keys the destination stores, while a key the write would
    turn into NULL passed Validate and died on NOT NULL at Execute.

    Optional FK / OAuth fields (``googleId`` → ``google_id``, ``providerId``,
    ``user_id``, …) are often sparse on Mongo/NoSQL sources. Treating every
    ``*_id`` target as required falsely blocked Validate while the real PK
    (``_id`` / ``id``) was populated. Strict mode still requires the resolved
    primary-key source (including a sole ``*_id`` when that is the identity).
    """
    issues: list[str] = []
    schemaless = dest_kind in SCHEMALESS_DESTS
    _ = (validation_mode or "strict").strip().lower()
    from services.reconciliation import destination_empty_string_is_null

    empty_is_null = destination_empty_string_is_null(dest_kind)

    # A column the operator declared Omit is never written, so no destination
    # nullability contract applies to it. Judging it would block Validate on a
    # value the run will not carry (Mongo's implicit `_id` omitted by hand).
    mappings = write_mappings(mappings)

    # Resolve the source column that maps to the primary key target.
    pk_source = ""
    if primary_key:
        for m in mappings:
            if (m.get("target") or "").lower() == primary_key.lower():
                pk_source = m.get("source", "")
                break
        if not pk_source:
            pk_source = primary_key

    for m in mappings:
        src = m.get("source", "")
        tgt = m.get("target", "")
        src_lower = src.lower()
        tgt_lower = tgt.lower()

        if schemaless and src_lower != "_id":
            # Schemaless documents generate `_id` and do not require every FK.
            continue

        # The inferred primary key is always required.
        is_pk = bool(pk_source and src == pk_source)
        # Exact canonical key names only — not every snake_case ``*_id`` FK.
        reserved_exact = {"id", "_id", "uuid", "pk", "key"}
        is_reserved_key = src_lower in reserved_exact or tgt_lower in reserved_exact
        if not is_pk and not is_reserved_key:
            continue

        values = [lookup_row_value(row, src, None) for row in rows]
        if not values:
            continue
        # A NULL cell serialises to the wire sentinel, not "" — count it (and
        # the reader's NULL/Missing spellings) as empty, or a key column that
        # is entirely NULL sails through as populated. An empty string is a
        # key value, not a missing one (D53): it counts only where the
        # destination itself stores '' as NULL (Oracle VARCHAR2).
        blank_is_null = empty_is_null or _route_stores_blank_key_as_null(
            m,
            source_type=str((source_types or {}).get(src) or ""),
            empty_cells_as_null=empty_cells_as_null,
            database_extract=database_extract,
        )
        empty = sum(
            1
            for v in values
            if is_reader_null_cell(v)
            or (blank_is_null and wire_cell_text(v) == "")
        )
        rate = empty / len(values)
        if rate > null_rate_max:
            blank = (
                sum(
                    1
                    for v in values
                    if not is_reader_null_cell(v) and wire_cell_text(v) == ""
                )
                if blank_is_null
                else 0
            )
            where = (
                f"{dest_kind or 'the destination'} stores '' as NULL"
                if empty_is_null
                else "this route writes '' into that column as NULL"
            )
            why = (
                f"; {blank} of them are '' and {where}, so they cannot identify a row"
                if blank
                else ""
            )
            issues.append(
                f"{src}: {rate:.0%} null/empty (max {null_rate_max:.0%} for required field){why}"
            )
    blocks = len(issues) > 0
    return {
        "check": "required_nulls",
        "passed": not blocks,
        "blocks_transfer": blocks,
        "issues": issues[:15],
    }


def _is_append_like(sync_mode: str) -> bool:
    """Align with ``sync_cursor.APPEND_SYNC_MODES`` (insert/full_append aliases)."""
    try:
        from services.sync_cursor import is_append_sync, normalize_sync_mode

        return is_append_sync(normalize_sync_mode(sync_mode))
    except Exception:
        return (sync_mode or "").strip().lower() in {
            "full_refresh_append",
            "incremental_append",
            "append",
            "append_only",
            "insert",
            "full_append",
        }


def _is_overwrite_like(sync_mode: str) -> bool:
    """Align with ``sync_cursor.OVERWRITE_SYNC_MODES``."""
    try:
        from services.sync_cursor import is_overwrite_sync, normalize_sync_mode

        return is_overwrite_sync(normalize_sync_mode(sync_mode))
    except Exception:
        return (sync_mode or "").strip().lower() in {
            "full_refresh_overwrite",
            "overwrite",
            "full_refresh",
            "replace",
            "truncate",
            "full_overwrite",
        }


def _target_for_source(source: str, mappings: list[dict]) -> str:
    for m in (mappings or []):
        if (m.get("source") or "").lower() == (source or "").lower():
            return str(m.get("target") or source)
    return source


def _source_for_target(target: str, mappings: list[dict]) -> str:
    for m in (mappings or []):
        if (m.get("target") or "").lower() == (target or "").lower():
            return str(m.get("source") or target)
    return target


def _unique_key_column_list(uk: dict[str, Any]) -> list[str]:
    cols: list[str] = []
    for c in list(uk.get("columns") or []) + list(uk.get("expression_columns") or []):
        name = str(c or "").strip()
        if name and name.lower() not in {x.lower() for x in cols}:
            cols.append(name)
    return cols


def _lookup_target_ddl(target_col: str, target_types: dict[str, str] | None) -> str:
    if not target_types:
        return ""
    if target_col in target_types:
        return str(target_types.get(target_col) or "")
    lower_map = {str(k).lower(): v for k, v in target_types.items()}
    return str(lower_map.get(target_col.lower(), "") or "")


def _sample_unique_constraint_dupes(
    mappings: list[dict],
    rows: list[dict[str, Any]],
    *,
    columns: list[str],
    unique_meta: dict[str, Any] | None,
    target_types: dict[str, str] | None,
    label: str,
    dest_kind: str = "",
) -> list[str]:
    """Detect sample duplicates for a (possibly composite) destination UNIQUE/PK.

    Airbyte-class composite keys: uniqueness is on the combination, not each
    column alone — ``(org=1,code=A)`` and ``(org=2,code=A)`` must not false-fail.
    """
    if not columns or not rows:
        return []
    from services.type_system import (
        unique_equality_key,
        unique_key_forces_casefold,
        unique_key_nulls_collide,
        unique_key_row_in_scope,
    )

    uk_list = [unique_meta] if unique_meta else []
    col_specs: list[tuple[str, str, str, bool, str | None]] = []
    for dest_col in columns:
        src = _source_for_target(dest_col, mappings)
        ddl = _lookup_target_ddl(dest_col, target_types)
        casefold = unique_key_forces_casefold(
            dest_col, ddl_type=ddl, unique_keys=uk_list
        )
        nulls_collide = unique_key_nulls_collide(dest_col, unique_keys=uk_list)
        null_sentinel = "\x00NULL\x00" if nulls_collide else None
        col_specs.append((dest_col, src, ddl, casefold, null_sentinel))

    seen: dict[str, int] = {}
    examples: dict[str, str] = {}
    scope_col = columns[0]
    for row in rows:
        scope_row = dict(row)
        for dest_col, src, *_rest in col_specs:
            if dest_col not in scope_row and src in scope_row:
                scope_row[dest_col] = scope_row.get(src)
        if unique_meta and not unique_key_row_in_scope(
            scope_row, scope_col, unique_keys=uk_list
        ):
            continue
        parts: list[str] = []
        display: list[str] = []
        skip = False
        for dest_col, src, ddl, casefold, null_sentinel in col_specs:
            # Preserve trailing spaces for NO PAD engines — cell_to_string only.
            # An absent cell is Missing (no key), never the '' key value (D53).
            raw_cell = row.get(src, row.get(dest_col))
            raw = wire_cell_text(raw_cell) if raw_cell is not None else ""
            key_part = unique_equality_key(
                None if raw_cell is None else raw,
                ddl,
                force_casefold=casefold,
                null_sentinel=null_sentinel,
                dest_kind=dest_kind,
            )
            if not key_part and null_sentinel is None:
                # Default UNIQUE: NULL / empty key column → not colliding.
                skip = True
                break
            parts.append(key_part)
            display.append(raw if raw else "<NULL>")
        if skip or not parts:
            continue
        key = "\x1f".join(parts)
        seen[key] = seen.get(key, 0) + 1
        examples.setdefault(key, "(" + ", ".join(display) + ")")

    dupes = [(examples.get(v, v), c) for v, c in seen.items() if c > 1]
    if not dupes:
        return []
    sample = ", ".join(f"{v}×{c}" for v, c in dupes[:3])
    return [f"{label}: duplicate key values ({sample})"]


def _key_table_kind(uk: dict[str, Any] | None) -> str:
    """Table kind stamped on one catalog key, or empty when it was not measured."""
    if not isinstance(uk, dict) or "table_kind" not in uk:
        return ""
    from services.foreign_key_metadata import normalize_snowflake_table_kind

    return normalize_snowflake_table_kind(uk.get("table_kind"))


def _destination_constraints_advisory(
    dest_kind: str,
    destination_unique_keys: list[dict[str, Any]] | None = None,
) -> bool:
    """True when dest PK/UNIQUE are optimizer metadata, not a write rule.

    Redshift, BigQuery, Databricks, and Snowflake standard tables share
    :func:`services.foreign_key_metadata.uniqueness_proof_gap`. A Snowflake
    key becomes a write rule only when its measured table kind is hybrid
    and ``enforced`` is true. A stamped ``enforced`` flag without that kind
    stays advisory. A key dict that says ``enforced`` false on any other
    engine is the same answer.
    """
    from services.foreign_key_metadata import uniqueness_proof_gap

    keys = [uk for uk in (destination_unique_keys or []) if isinstance(uk, dict)]
    if not keys:
        return uniqueness_proof_gap(dest_kind) == "unenforced"
    return all(
        not _unique_constraint_enforced(uk, dest_kind=dest_kind) for uk in keys
    )


def _unique_constraint_enforced(
    uk: dict[str, Any] | None,
    *,
    dest_kind: str = "",
) -> bool:
    from services.foreign_key_metadata import _dialect_key, uniqueness_proof_gap

    kind = _key_table_kind(uk)
    if uk is not None and uk.get("index_ready") is False:
        # PostgreSQL indisready is false. Inserts ignore the index.
        return False
    if uk is not None and uk.get("disabled") is True:
        # SQL Server DISABLE. The index does not reject a new duplicate.
        return False
    if uniqueness_proof_gap(dest_kind, table_kind=kind) == "unenforced":
        return False
    if _dialect_key(dest_kind) == "snowflake":
        # Hybrid tables enforce the key. The constraint row still has to say so.
        return uk is not None and uk.get("enforced") is True
    if uk is not None and uk.get("enforced") is False:
        return False
    if uk is not None and uk.get("enforced") is True:
        return True
    # An enforcing engine with no separate bit treats the constraint as the check.
    return True


def _advisory_unique_key_warnings(
    *,
    dest_kind: str,
    destination_unique_keys: list[dict[str, Any]] | None,
    mappings: list[dict] | None = None,
    rows: list[dict[str, Any]] | None = None,
    destination_pk_columns: list[str] | None = None,
    target_types: dict[str, str] | None = None,
) -> list[str]:
    """Warn-only honesty for BQ / Redshift / Snowflake NOT ENFORCED keys.

    Never invent blockers — operators must prove uniqueness in pipeline tests
    (dbt unique / Gate-9 sample) before trusting merges.
    """
    warnings: list[str] = []
    keys = list(destination_unique_keys or [])
    advisory = [
        uk for uk in keys if not _unique_constraint_enforced(uk, dest_kind=dest_kind)
    ]
    kind = normalize_dest_kind(dest_kind or "") or "destination"
    if any(
        uk.get("index_valid") is False and uk.get("index_ready") is not False
        for uk in keys
    ):
        warnings.append(
            f"{kind} unique index is invalid (pg_index.indisvalid is false). "
            "New duplicates are still rejected while the index is ready for "
            "inserts. Existing rows were not checked."
        )
    if any(uk.get("ignore_dup_key") is True for uk in keys):
        warnings.append(
            f"{kind} unique index has IGNORE_DUP_KEY. A duplicate insert "
            "is dropped with a warning instead of failing the statement. "
            "Validate still quarantines a duplicate it can see."
        )
    if not advisory and not _destination_constraints_advisory(dest_kind, keys):
        return warnings
    if advisory:
        labels: list[str] = []
        for uk in advisory[:6]:
            name = str(uk.get("name") or ("PRIMARY" if uk.get("primary") else "UNIQUE"))
            cols = _unique_key_column_list(uk)
            labels.append(f"{name}({', '.join(cols)})" if cols else name)
        warnings.append(
            f"{kind} PRIMARY KEY / UNIQUE is NOT ENFORCED (advisory/optimizer "
            f"metadata): {', '.join(labels)} — Validate will not invent write "
            "blockers; prove uniqueness with pipeline tests before trusting merges."
        )
        if any(uk.get("rely") is True for uk in advisory):
            warnings.append(
                f"{kind} RELY is an optimizer hint. It does not prove the "
                "loaded rows are unique."
            )
        if any(uk.get("disabled") is True for uk in advisory):
            from services.foreign_key_metadata import _dialect_key

            if _dialect_key(dest_kind) == "oracle":
                warnings.append(
                    f"{kind} primary key or unique constraint has STATUS "
                    "DISABLED. It does not reject a new duplicate and does "
                    "not prove the loaded rows are unique."
                )
            else:
                warnings.append(
                    f"{kind} unique index is disabled. It does not reject a "
                    "new duplicate and does not prove the loaded rows are unique."
                )
        if any(uk.get("index_ready") is False for uk in advisory):
            warnings.append(
                f"{kind} unique index is not ready for inserts "
                "(pg_index.indisready is false). It does not reject a new "
                "duplicate and does not prove the loaded rows are unique."
            )
    elif _destination_constraints_advisory(dest_kind, keys):
        warnings.append(
            f"{kind} PRIMARY KEY / UNIQUE constraints are informational "
            "(not enforced at write) — Validate will not invent duplicate blockers."
        )

    # Soft-probe sample collisions under advisory keys (warn, never block).
    if rows and mappings and advisory:
        mapped_targets = {
            str(m.get("target") or "").lower()
            for m in mappings
            if m.get("target")
        }
        for uk in advisory:
            cols = _unique_key_column_list(uk)
            if not cols or not all(c.lower() in mapped_targets for c in cols):
                continue
            name = str(uk.get("name") or ("PRIMARY" if uk.get("primary") else "UNIQUE"))
            soft = _sample_unique_constraint_dupes(
                mappings,
                rows,
                columns=cols,
                unique_meta=uk,
                target_types=target_types,
                label=f"advisory {name} (" + ", ".join(cols) + ")",
                dest_kind=dest_kind,
            )
            for msg in soft:
                warnings.append(
                    f"{msg} — destination will accept duplicates (NOT ENFORCED)"
                )
        # Composite PK columns listed without unique_keys primary bucket.
        pk_cols = [str(c) for c in (destination_pk_columns or []) if c]
        if len(pk_cols) >= 2 and all(c.lower() in mapped_targets for c in pk_cols):
            if not any(_unique_key_column_list(uk) == pk_cols for uk in advisory):
                soft = _sample_unique_constraint_dupes(
                    mappings,
                    rows,
                    columns=pk_cols,
                    unique_meta={
                        "name": "PRIMARY",
                        "columns": pk_cols,
                        "primary": True,
                        "enforced": False,
                    },
                    target_types=target_types,
                    label="advisory PRIMARY KEY (" + ", ".join(pk_cols) + ")",
                    dest_kind=dest_kind,
                )
                for msg in soft:
                    warnings.append(
                        f"{msg} — destination will accept duplicates (NOT ENFORCED)"
                    )
    return warnings[:12]


def _check_destination_unique_constraints(
    mappings: list[dict],
    rows: list[dict[str, Any]],
    *,
    destination_pk_columns: list[str] | None,
    destination_unique_keys: list[dict[str, Any]] | None,
    target_types: dict[str, str] | None,
    dest_kind: str = "",
) -> list[str]:
    """Probe all fully-mapped destination PK/UNIQUE constraints (incl. composites)."""
    issues: list[str] = []
    mapped_targets = {
        str(m.get("target") or "").lower()
        for m in (mappings or [])
        if m.get("target")
    }

    def _fully_mapped(cols: list[str]) -> bool:
        return bool(cols) and all(c.lower() in mapped_targets for c in cols)

    pk_cols = [str(c) for c in (destination_pk_columns or []) if c]
    pk_meta = next(
        (
            uk
            for uk in (destination_unique_keys or [])
            if uk.get("primary")
        ),
        {
            "name": "PRIMARY",
            "columns": pk_cols,
            "primary": True,
            "enforced": not _destination_constraints_advisory(
                dest_kind, destination_unique_keys
            ),
        },
    )
    if (
        len(pk_cols) >= 2
        and _fully_mapped(pk_cols)
        and _unique_constraint_enforced(pk_meta, dest_kind=dest_kind)
    ):
        issues.extend(
            _sample_unique_constraint_dupes(
                mappings,
                rows,
                columns=pk_cols,
                unique_meta=pk_meta,
                target_types=target_types,
                label="PRIMARY KEY (" + ", ".join(pk_cols) + ")",
                dest_kind=dest_kind,
            )
        )

    for uk in destination_unique_keys or []:
        # BQ / Redshift / Snowflake NOT ENFORCED — advisory only, do not invent blockers.
        if not _unique_constraint_enforced(uk, dest_kind=dest_kind):
            continue
        if uk.get("primary") and len(_unique_key_column_list(uk)) >= 2:
            # Already handled via pk_cols path above.
            continue
        cols = _unique_key_column_list(uk)
        if len(cols) < 2:
            # Single-column UNIQUE still matters when identity path skipped it
            # (e.g. append onto hybrid UNIQUE that is not the Studio PK).
            if len(cols) == 1 and _fully_mapped(cols):
                name = str(uk.get("name") or "UNIQUE")
                issues.extend(
                    _sample_unique_constraint_dupes(
                        mappings,
                        rows,
                        columns=cols,
                        unique_meta=uk,
                        target_types=target_types,
                        label=f"UNIQUE {name} (" + ", ".join(cols) + ")",
                        dest_kind=dest_kind,
                    )
                )
            continue
        if not _fully_mapped(cols):
            continue
        name = str(uk.get("name") or "UNIQUE")
        issues.extend(
            _sample_unique_constraint_dupes(
                mappings,
                rows,
                columns=cols,
                unique_meta=uk,
                target_types=target_types,
                label=f"UNIQUE {name} (" + ", ".join(cols) + ")",
                dest_kind=dest_kind,
            )
        )
    return issues


def _inferred_identity_dup_warnings(
    rows: list[dict[str, Any]], identity_cols: list[str], pk_label: str
) -> list[str]:
    """Non-blocking note for duplicates of a heuristically inferred identity."""
    counts: dict[tuple[str, ...], int] = {}
    for row in rows:
        tup = tuple(wire_cell_text(row.get(c)) for c in identity_cols)
        if all(v == "" for v in tup):
            continue
        counts[tup] = counts.get(tup, 0) + 1
    dupes = [(",".join(t), n) for t, n in counts.items() if n > 1]
    if not dupes:
        return []
    sample = ", ".join(f"{v}×{n}" for v, n in dupes[:3])
    return [
        f"{pk_label}: duplicate values of an inferred identity ({sample}) — the "
        "overwrite recreates a table without a declared key, so rows load as-is. "
        "Declare a primary key to enforce uniqueness."
    ]


def _stream_contracts_declare_identity(stream_contracts: list[dict] | None) -> bool:
    for contract in stream_contracts or []:
        if not isinstance(contract, dict):
            continue
        for field in ("primary_key", "primary_keys", "upsert_key", "cursor_primary_key"):
            if contract.get(field):
                return True
    return False


def _business_identity_columns(identity_cols: list[str], sync: str) -> list[str]:
    """Drop SCD2 engine-owned history columns from a source identity key.

    An SCD2 destination key is ``(business_key, valid_from)``; the source carries
    one row per business key and never the history columns, so including them
    would make every probe tuple incomplete (false green) or compare history
    versions as duplicates (false block).
    """
    if sync != "scd2" or not identity_cols:
        return identity_cols
    from services.scd2_engine import SCD2_COLUMNS

    history = {c.lower() for c in SCD2_COLUMNS}
    business = [c for c in identity_cols if c.lower() not in history]
    return business or identity_cols


def _rows_by_file(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    tagged = any(isinstance(row, dict) and "_df_file" in row for row in rows)
    if not tagged:
        return [list(rows)]
    groups: dict[str, list[dict[str, Any]]] = {}
    order: list[str] = []
    for row in rows:
        tag = str(row.get("_df_file") or "") if isinstance(row, dict) else ""
        if tag not in groups:
            order.append(tag)
            groups[tag] = []
        groups[tag].append(row)
    return [groups[tag] for tag in order]


def _file_scope_key(row: dict[str, Any], key: str) -> str:
    if isinstance(row, dict) and "_df_file" in row:
        return str(row.get("_df_file") or "") + "\x1e" + str(key)
    return str(key)


def _in_file_collapse_message(
    pk: str,
    extra: int,
    example: str,
    *,
    sample: bool = False,
    unique: int | None = None,
    scanned: int | None = None,
) -> str:
    """Operator text for an in-file upsert collapse.

    A finished scan names the population. A partial scan says the number is
    from the rows that were read. Neither path invents a count it did not make.
    """
    if sample:
        counted = f"{extra} extra row(s) in the scanned sample"
    else:
        counted = f"{extra} extra row(s)"
    shown = f"({example}) " if example else ""
    scope = ""
    if not sample and unique is not None and scanned is not None:
        scope = f"({unique} unique, {scanned} scanned) "
    return (
        f"In-file duplicate keys on {pk}: "
        f"{counted} collapsed to one per key "
        f"{shown}{scope}"
        "(last wins). Counted once. Not quarantined and not skipped."
    )


def _check_duplicate_keys(
    mappings: list[dict],
    rows: list[dict[str, Any]],
    validation_mode: str = "strict",
    *,
    dest_kind: str = "",
    primary_key: str | None = None,
    primary_key_columns: list[str] | None = None,
    source_duplicate_findings: list[dict[str, Any]] | None = None,
    sync_mode: str = "",
    destination_pk_columns: list[str] | None = None,
    destination_unique_keys: list[dict[str, Any]] | None = None,
    target_types: dict[str, str] | None = None,
    source_duplicate_probe_status: str = "",
    source_duplicate_probe_message: str = "",
    source_duplicate_probe_expected: bool = False,
    identity_declared: bool | None = None,
    in_file_dedupe: str = "",
    duplicate_policy: str = "",
) -> dict[str, Any]:
    """Duplicate check on the resolved identity key (sample + source-side probe).

    - Schemaless destinations (Mongo/Redis/Dynamo) and sync modes that require a
      unique identity (upsert/CDC/mirror/SCD2) always enforce duplicates.
    - Overwrite modes block on duplicates of a *declared* identity (operator /
      contract / composite key, or a destination key the recreate enforces). A
      heuristically inferred key only warns — a heap may legally repeat it.
      ``identity_declared=None`` means only a destination key counts as declared.
    - SCD2 dedupes on the business key; engine-owned history columns
      (valid_from/valid_to/is_current/row_hash) are never part of source identity.
    - Append-like modes only enforce duplicates when the destination primary key or
      a UNIQUE index is known to include the mapped target column.
    - Destination CI/AI collations / CITEXT / ``UNIQUE (lower(col))`` equate
      case/accents so Validate does not false-green engine-equal keys.
    - Composite UNIQUE/PK constraints are probed as tuples (Airbyte composite
      primary key class) — never invent single-column uniqueness from a multi-col index.
    """
    issues: list[str] = []
    from services.primary_key import sync_requires_unique_identity

    schemaless = dest_kind in SCHEMALESS_DESTS
    try:
        from services.sync_cursor import normalize_sync_mode

        sync = normalize_sync_mode(sync_mode)
    except Exception:
        sync = (sync_mode or "").strip().lower()
    # ACC-03 — full identity columns (composite-aware). A (region,id) contract
    # must dedupe on the tuple, never on `id` alone: components legitimately
    # repeat across composite keys, and checking one member false-blocks every
    # composite-key upsert.
    identity_cols = [
        str(c).strip() for c in (primary_key_columns or []) if str(c or "").strip()
    ]
    if not identity_cols and primary_key:
        identity_cols = [primary_key]
    identity_cols = _business_identity_columns(identity_cols, sync)
    destination_pk_columns = _business_identity_columns(
        [str(c) for c in (destination_pk_columns or []) if c], sync
    )
    composite_identity = len(identity_cols) > 1
    pk_label = " + ".join(identity_cols) if identity_cols else (primary_key or "")
    target_cols = [
        (_target_for_source(c, mappings) or c) for c in identity_cols
    ]
    from services.primary_key import operator_collapses_in_file_duplicates

    keep_last = str(in_file_dedupe or "").strip().lower().replace("-", "_") == "keep_last"
    collapses_in_file = operator_collapses_in_file_duplicates(sync, in_file_dedupe)
    collapsed_extra = 0
    collapse_example = ""
    probe_collapsed_extra = 0
    probe_example = ""
    population_extra: int | None = None
    population_unique: int | None = None
    population_scanned: int | None = None
    sample_only = False
    sample_extra: int | None = None
    if collapses_in_file:
        # Explicit keep-last clears the source probe: the operator chose it.
        # Upsert collapses in-file copies the payload scan found. A SQL
        # source-table probe that found real duplicate keys still blocks,
        # and so does an explicit block policy (collapses_in_file is false).
        if keep_last:
            source_duplicate_findings = []
        elif identity_cols:
            kept_findings: list[dict[str, Any]] = []
            for finding in source_duplicate_findings or []:
                if isinstance(finding, dict) and finding.get("in_file"):
                    if (
                        finding.get("population_complete") is True
                        and finding.get("population_extra") is not None
                    ):
                        population_extra = int(finding["population_extra"])
                        if finding.get("population_unique") is not None:
                            population_unique = int(finding["population_unique"])
                        if finding.get("population_scanned") is not None:
                            population_scanned = int(finding["population_scanned"])
                    elif finding.get("sample_only"):
                        sample_only = True
                        if finding.get("sample_extra") is not None:
                            sample_extra = int(finding["sample_extra"])
                    count = int(finding.get("count") or 0)
                    if count > 1:
                        probe_collapsed_extra += count - 1
                        if not probe_example:
                            probe_example = str(finding.get("value") or "")
                    continue
                kept_findings.append(finding)
            source_duplicate_findings = kept_findings
        if identity_cols:
            from services.duplicate_policy import collapse_keep_last
            from services.type_system import unique_equality_key

            def _equality_identity(row: dict[str, Any]) -> tuple:
                parts = []
                for col in identity_cols:
                    ddl = _lookup_target_ddl(col, target_types) if target_types else ""
                    parts.append(
                        unique_equality_key(
                            row.get(col),
                            ddl or "",
                            dest_kind=dest_kind,
                        )
                    )
                return tuple(parts)

            collapsed_groups = []
            for group in _rows_by_file(rows):
                kept, dropped = collapse_keep_last(
                    group, identity_cols, key_fn=_equality_identity
                )
                collapsed_extra += int(dropped or 0)
                if dropped and not collapse_example and group:
                    sample = group[-1]
                    collapse_example = (
                        "(" + ", ".join(str(sample.get(col)) for col in identity_cols) + ")"
                    )
                collapsed_groups.append(kept)
            rows = [row for group in collapsed_groups for row in group]
    target_col = _target_for_source(primary_key, mappings) if primary_key else ""
    advisory_warnings = _advisory_unique_key_warnings(
        dest_kind=dest_kind,
        destination_unique_keys=destination_unique_keys,
        mappings=mappings,
        rows=rows,
        destination_pk_columns=destination_pk_columns,
        target_types=target_types,
    )
    if population_extra:
        # A finished payload scan counted every extra row. The example list is
        # capped, so summing those examples under-counts the file.
        advisory_warnings.append(
            _in_file_collapse_message(
                pk_label or primary_key or "",
                population_extra,
                probe_example,
                unique=population_unique,
                scanned=population_scanned,
            )
        )
    elif collapsed_extra:
        advisory_warnings.append(
            _in_file_collapse_message(
                pk_label or primary_key or "",
                collapsed_extra,
                collapse_example.lstrip("(").rstrip(")") if collapse_example else "",
            )
        )
    elif sample_only and (sample_extra or probe_collapsed_extra):
        # The scan stopped before the rest of the file. Say so. Do not promote
        # the rows that were read into a population.
        advisory_warnings.append(
            _in_file_collapse_message(
                pk_label or primary_key or "",
                sample_extra if sample_extra is not None else probe_collapsed_extra,
                probe_example,
                sample=True,
            )
        )
    elif probe_collapsed_extra:
        # The Validate sample missed the repeated keys (it is capped). The
        # file scan found them. Upsert still keeps one row per key.
        advisory_warnings.append(
            _in_file_collapse_message(
                pk_label or primary_key or "",
                probe_collapsed_extra,
                probe_example,
            )
        )

    dest_has_enforced_key = bool(destination_pk_columns) or any(
        _unique_constraint_enforced(uk, dest_kind=dest_kind)
        for uk in (destination_unique_keys or [])
    )
    if identity_declared is None:
        identity_declared = dest_has_enforced_key
    # A declared identity that repeats in the source cannot become the recreated
    # table's key, so overwrite blocks. An inferred key on a heap only warns.
    # Explicit append is the operator saying duplicate keys are the load.
    from services.duplicate_policy import normalize_duplicate_policy

    append_allows_duplicates = normalize_duplicate_policy(duplicate_policy) == "append"
    overwrite_enforces_uniqueness = (
        _is_overwrite_like(sync)
        and bool(identity_cols)
        and bool(identity_declared or dest_has_enforced_key)
        and not append_allows_duplicates
    )
    # Single-column identity enforcement (upsert/CDC/PK/single UNIQUE).
    enforce_identity = bool(identity_cols) and (
        schemaless
        or (
            sync_requires_unique_identity(sync, dest_kind=dest_kind)
            and not collapses_in_file
        )
        or overwrite_enforces_uniqueness
        or (
            not _is_append_like(sync)
            and not _is_overwrite_like(sync)
            and not collapses_in_file
        )
    )
    covering_single = False
    covering_composite_only = False
    if primary_key and _is_append_like(sync) and (
        destination_pk_columns or destination_unique_keys
    ):
        dest_pk = [str(c) for c in (destination_pk_columns or []) if c]
        pk_enforced = not _destination_constraints_advisory(
            dest_kind, destination_unique_keys
        )
        # Prefer explicit primary bucket flag when present.
        for uk in destination_unique_keys or []:
            if uk.get("primary"):
                pk_enforced = _unique_constraint_enforced(uk, dest_kind=dest_kind)
                break
        if (
            pk_enforced
            and len(dest_pk) == 1
            and dest_pk[0].lower() == target_col.lower()
        ):
            covering_single = True
            enforce_identity = True
        elif (
            pk_enforced
            and len(dest_pk) > 1
            and target_col.lower() in {c.lower() for c in dest_pk}
        ):
            covering_composite_only = True
        for uk in destination_unique_keys or []:
            if not _unique_constraint_enforced(uk, dest_kind=dest_kind):
                continue
            cols = _unique_key_column_list(uk)
            if target_col.lower() not in {c.lower() for c in cols}:
                continue
            if len(cols) == 1:
                covering_single = True
                enforce_identity = True
            else:
                covering_composite_only = True

    # Composite UNIQUE/PK probes run whenever catalog keys are known and mapped —
    # not only when the Studio identity column participates.
    composite_issues: list[str] = []
    for group in _rows_by_file(rows):
        composite_issues.extend(
            _check_destination_unique_constraints(
                mappings,
                group,
                destination_pk_columns=destination_pk_columns,
                destination_unique_keys=destination_unique_keys,
                target_types=target_types,
                dest_kind=dest_kind,
            )
        )
    issues.extend(composite_issues)

    if not identity_cols and not issues:
        return {
            "check": "duplicate_keys",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": advisory_warnings,
            "primary_key": None,
        }

    # Source-side probe is authoritative: it scans the full table, not just the
    # preview sample. Must run BEFORE the append/no-enforce early return —
    # Quarantine→balanced must not green Validate when write-time DQ will fail.
    probe_authoritative = False
    probe_status = (source_duplicate_probe_status or "").strip().lower()
    if identity_cols:
        findings = source_duplicate_findings or []
        if findings:
            sample = ", ".join(
                f"{f.get('value')}×{f.get('count', 1)}" for f in findings[:3]
            )
            issues.append(
                f"{pk_label}: duplicate key values from source probe ({sample})"
            )
            probe_authoritative = True

    # Expected but not "ran" is unavailable — including a status nobody set.
    # Requiring a non-empty status let a probe that never reported pass as
    # if it had proven uniqueness (QA MX3-22 fail-open).
    probe_unavailable = bool(
        source_duplicate_probe_expected
        and probe_status != "ran"
        and not probe_authoritative
    )

    if not enforce_identity and not issues and not probe_unavailable:
        if _is_overwrite_like(sync) and identity_cols:
            advisory_warnings = advisory_warnings + _inferred_identity_dup_warnings(
                rows, identity_cols, pk_label
            )
        return {
            "check": "duplicate_keys",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": advisory_warnings,
            "primary_key": primary_key,
            "dest_kind": dest_kind,
        }

    # Single-column sample identity — skip when the only covering constraint is
    # composite (same code under different orgs must not false-fail).
    # Also skip when the destination has a composite PK that includes the identity
    # column: checking single-column duplicates on a composite PK component is a
    # false positive (ACC-03: (region,id) composite key has duplicate 'id' values).
    dest_has_composite_pk = len(destination_pk_columns or []) > 1
    identity_in_composite_pk = (
        not composite_identity
        and dest_has_composite_pk
        and target_col
        and target_col.lower() in {str(c).lower() for c in (destination_pk_columns or [])}
    )
    run_identity = bool(
        enforce_identity
        and identity_cols
        and not (covering_composite_only and not covering_single)
        and not identity_in_composite_pk
    )
    if run_identity and composite_identity:
        # ACC-03 — dedupe on the whole tuple. ``composite_unique_equality_key``
        # applies the same per-cell engine semantics (CI/AI collation, PAD
        # rules) and joins components with a unit separator so ("ab","c") can
        # never equal ("a","bc"). A NULL component is SQL-NULL: not a key, so
        # that row cannot collide and is skipped — MATCH SIMPLE semantics.
        from services.type_system import (
            composite_unique_equality_key,
            unique_key_forces_casefold,
        )

        comp_ddls = [
            _lookup_target_ddl(tc, target_types) or _lookup_target_ddl(sc, target_types)
            for sc, tc in zip(identity_cols, target_cols)
        ]
        comp_casefold = [
            unique_key_forces_casefold(
                tc, ddl_type=comp_ddls[i], unique_keys=destination_unique_keys or []
            )
            for i, tc in enumerate(target_cols)
        ]
        seen_t: dict[str, int] = {}
        examples_t: dict[str, str] = {}
        for row in rows:
            key = composite_unique_equality_key(
                [
                    (
                        row.get(col),
                        comp_ddls[i],
                        comp_casefold[i],
                        None,
                    )
                    for i, col in enumerate(identity_cols)
                ],
                dest_kind=dest_kind,
            )
            if not key:
                continue
            scoped = _file_scope_key(row, key)
            seen_t[scoped] = seen_t.get(scoped, 0) + 1
            examples_t.setdefault(
                scoped,
                "(" + ", ".join(str(row.get(c)) for c in identity_cols) + ")",
            )
        dupes = [(examples_t.get(v, v), c) for v, c in seen_t.items() if c > 1]
        if dupes:
            sample = ", ".join(f"{v}×{c}" for v, c in dupes[:3])
            issues.append(
                f"{pk_label}: duplicate composite key values ({sample})"
            )
    elif run_identity and primary_key:
        from services.type_system import (
            unique_equality_key,
            unique_key_forces_casefold,
            unique_key_nulls_collide,
            unique_key_row_in_scope,
        )

        dest_ddl = _lookup_target_ddl(target_col, target_types) or _lookup_target_ddl(
            primary_key, target_types
        )
        # Prefer single-column uniques covering the target for casefold/nulls flags.
        single_uks = [
            uk
            for uk in (destination_unique_keys or [])
            if len(_unique_key_column_list(uk)) == 1
            and target_col.lower()
            in {c.lower() for c in _unique_key_column_list(uk)}
        ] or (destination_unique_keys or [])
        casefold = unique_key_forces_casefold(
            target_col,
            ddl_type=dest_ddl,
            unique_keys=single_uks,
        )
        nulls_collide = unique_key_nulls_collide(
            target_col, unique_keys=single_uks
        )
        null_sentinel = "\x00NULL\x00" if nulls_collide else None
        seen: dict[str, int] = {}
        examples: dict[str, str] = {}
        for row in rows:
            scope_row = dict(row)
            if target_col and target_col not in scope_row and primary_key in scope_row:
                scope_row[target_col] = scope_row.get(primary_key)
            if not unique_key_row_in_scope(
                scope_row, target_col, unique_keys=single_uks
            ):
                continue
            # An absent cell is Missing (no key), never the '' key value (D53).
            raw_cell = row.get(primary_key)
            raw = wire_cell_text(raw_cell) if raw_cell is not None else ""
            key = unique_equality_key(
                None if raw_cell is None else raw,
                dest_ddl,
                force_casefold=casefold,
                null_sentinel=null_sentinel,
                dest_kind=dest_kind,
            )
            if not key and not nulls_collide:
                continue
            if not key:
                continue
            scoped = _file_scope_key(row, key)
            seen[scoped] = seen.get(scoped, 0) + 1
            examples.setdefault(scoped, raw if raw else "<NULL>")
        dupes = [(examples.get(v, v), c) for v, c in seen.items() if c > 1]
        if dupes:
            sample = ", ".join(f"{v}×{c}" for v, c in dupes[:3])
            issues.append(f"{primary_key}: duplicate key values ({sample})")

    # Dedupe issue strings while preserving order.
    deduped: list[str] = []
    for item in issues:
        if item not in deduped:
            deduped.append(item)
    issues = deduped
    blocks = len(issues) > 0
    mode = (validation_mode or "").strip().lower()
    # Full-table probe found duplicates. Hard-block only when write-time uniqueness
    # is actually required (upsert/CDC/overwrite/schemaless/dest PK|UNIQUE).
    # Append / create-new without a covering unique constraint may legally carry
    # duplicate source keys — warn, do not invent a Validate-green / Execute-red
    # asymmetry (operator saw "Ready" then failed at Run on append).
    uniqueness_required = bool(
        enforce_identity
        or covering_single
        or covering_composite_only
        or schemaless
        or sync_requires_unique_identity(sync, dest_kind=dest_kind)
        or overwrite_enforces_uniqueness
    )
    # Probe expected but did not complete → never invent population uniqueness.
    # Upsert/CDC/overwrite/dest-PK routes fail closed; append/create-new warns.
    if probe_unavailable and uniqueness_required:
        detail = (source_duplicate_probe_message or probe_status).strip()
        issues.append(
            f"{primary_key}: source uniqueness probe unavailable ({detail}) — "
            "cannot approve uniqueness-required sync on sample alone"
        )
        return {
            "check": "duplicate_keys",
            "passed": False,
            "blocks_transfer": True,
            "issues": issues[:15],
            "warnings": advisory_warnings,
            "primary_key": primary_key,
            "dest_kind": dest_kind,
            "note": (
                "Source uniqueness probe did not complete. Upsert/CDC/overwrite/"
                "destination-PK routes fail closed until the probe runs, a unique "
                "primary key is proven, or sync mode is switched to append without "
                "a covering UNIQUE constraint."
            ),
        }
    if probe_unavailable and not uniqueness_required:
        advisory_warnings.append(
            f"{primary_key}: source uniqueness probe unavailable ({probe_status}) — "
            "sample-only uniqueness; append may still write duplicate keys"
        )
    if probe_authoritative and blocks:
        # In-file copies collapse. A full-table probe that found duplicate
        # keys is still a block: the source itself is not unique.
        if uniqueness_required or collapses_in_file:
            return {
                "check": "duplicate_keys",
                "passed": False,
                "blocks_transfer": True,
                "issues": issues[:15],
                "warnings": advisory_warnings,
                "primary_key": primary_key,
                "dest_kind": dest_kind,
                "note": (
                    "Source-table probe found duplicate identity keys — Validate cannot "
                    "pass until Primary key is a unique column, sync mode allows non-unique "
                    "rows without that PK, or the source is deduped. Strip/Quarantine cannot fix this."
                ),
            }
        return {
            "check": "duplicate_keys",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": (advisory_warnings + issues)[:15],
            "primary_key": primary_key,
            "dest_kind": dest_kind,
            "note": (
                f"Source probe found duplicate values on {primary_key}, but sync mode "
                f"{sync or 'append'} does not require unique identity and destination has "
                "no UNIQUE/PK covering that column — rows will append. Switch to upsert "
                "with a unique primary key if identity must be enforced."
            ),
        }
    if blocks and mode == "balanced":
        # Balanced may warn-only for append-like routes where sample-only
        # duplicates can be legal. Upsert/CDC/mirror/SCD2/overwrite/schemaless
        # still fail-closed — Studio "strip+rerun balanced" must not green-light
        # PK collisions that write-time DQ will refuse.
        must_block = (
            schemaless
            or sync_requires_unique_identity(sync, dest_kind=dest_kind)
            or overwrite_enforces_uniqueness
            or (not _is_append_like(sync) and not _is_overwrite_like(sync))
        )
        if must_block:
            return {
                "check": "duplicate_keys",
                "passed": False,
                "blocks_transfer": True,
                "issues": issues[:15],
                "warnings": (advisory_warnings + issues)[:15],
                "primary_key": primary_key,
                "dest_kind": dest_kind,
                "note": (
                    "Balanced still blocks duplicate identity for upsert/CDC/"
                    "overwrite/schemaless — remediations: dedupe source, composite key, "
                    "or switch sync mode"
                ),
            }
        return {
            "check": "duplicate_keys",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": (advisory_warnings + issues)[:15],
            "primary_key": primary_key,
            "dest_kind": dest_kind,
        }
    if blocks and append_allows_duplicates:
        # Explicit append on overwrite reloads every source row. A repeated
        # key is the data, including when the full-table probe found it.
        return {
            "check": "duplicate_keys",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": (advisory_warnings + issues)[:15],
            "primary_key": primary_key,
            "dest_kind": dest_kind,
            "note": (
                "Append keeps every source row, including repeated identity keys."
            ),
        }
    return {
        "check": "duplicate_keys",
        "passed": not blocks,
        "blocks_transfer": blocks,
        "issues": issues[:15],
        "warnings": advisory_warnings,
        "primary_key": primary_key,
        "dest_kind": dest_kind,
    }


def _check_sample_quality(
    source_columns: list[str],
    rows: list[dict[str, Any]],
    source_types: dict[str, str],
    validation_mode: str,
    *,
    dest_kind: str = "",
) -> dict[str, Any]:
    if not rows:
        return {"check": "sample_quality", "passed": True, "blocks_transfer": False, "issues": []}

    from services.sample_quality import analyze_dataset_quality

    report = analyze_dataset_quality(
        source_columns,
        rows,
        schema=source_types,
        dest_kind=dest_kind,
    )
    return {
        "check": "sample_quality",
        "passed": not report.get("blocks_transfer"),
        "blocks_transfer": bool(report.get("blocks_transfer")),
        "issues": report.get("issues", [])[:20],
        "score": report.get("quality_score"),
    }


def _check_mapping_confidence(
    mappings: list[dict],
    *,
    confidence_min: float,
    validation_mode: str = "strict",
) -> dict[str, Any]:
    """Report mapping confidence for explainability — G4 owns hard blocks.

    Module 3: never ``blocks_transfer`` on confidence alone here. G9 previously
    duplicated ``g4_mapping_confidence`` and confused Validate with two
    confidence faces for one threshold.
    """
    # Keep G9 aligned with the published/preflight mode floors for *reporting*:
    # maximum=0.95, strict=0.85, balanced=0.75.
    mode = (validation_mode or "strict").strip().lower()
    floor = confidence_min
    warnings: list[str] = []
    for m in mappings:
        # Align with G4: operator override / risk ack already cleared Map confidence.
        if m.get("user_override") or m.get("risk_acknowledged") or m.get("riskAcknowledged"):
            continue
        conf = float(m.get("confidence", 0))
        if conf < floor:
            warnings.append(
                f"{m.get('source')}→{m.get('target')}: confidence {conf:.0%} < {floor:.0%} "
                "(G4 is hard authority)"
            )
        elif m.get("requires_review"):
            warnings.append(
                f"{m.get('source')}→{m.get('target')}: ambiguous mapping requires review "
                "(G4 is hard authority)"
            )
    return {
        "check": "mapping_confidence",
        "passed": True,
        "blocks_transfer": False,
        "issues": [],
        "warnings": warnings[:20],
        "authority": "g4_mapping_confidence",
        "validation_mode": mode,
        "note": (
            "Informational only — hard confidence / review blocks are owned by "
            "g4_mapping_confidence (Module 3 confidence SSOT)."
        ),
    }


def _format_control_chars(text: str) -> tuple[list[str], list[str]]:
    """U+XXXX codes of ``(control, format)`` characters in ``text``.

    Controls (Cc, minus tab/newline/CR — NUL, BEL, ESC …) are rejected or
    corrupted by destinations. Format characters (Cf — U+200B, ZWJ/ZWNJ,
    soft hyphen, BOM, bidi marks) are valid Unicode every UTF-8 destination
    stores verbatim; they only make visually equal text compare unequal.
    """
    controls: list[str] = []
    formats: list[str] = []
    for ch in text:
        cat = unicodedata.category(ch)
        if cat == "Cc" and ch not in "\t\n\r":
            bucket = controls
        elif cat == "Cf":
            bucket = formats
        else:
            continue
        code = f"U+{ord(ch):04X}"
        if code not in bucket:
            bucket.append(code)
        if len(controls) + len(formats) >= 6:
            break
    return controls, formats


def _check_encoding_anomalies(
    rows: list[dict[str, Any]],
    *,
    validation_mode: str = "strict",
    mappings: list[dict] | None = None,
) -> dict[str, Any]:
    """Flag replacement / format-control chars that break warehouse loads.

    Strict/maximum: U+FFFD and control characters block at Validate; format
    characters alone (U+200B …) warn, because they are valid text the
    destination keeps (D69). Balanced: every finding warns with the
    strip_controls path (never silent-pass). Columns already mapped with
    ``strip_controls`` / ``normalize_unicode`` are skipped.
    """
    sanitized_cols = {
        str(m.get("source") or "").lower()
        for m in (mappings or [])
        if str(m.get("transform") or "").lower() in {"strip_controls", "normalize_unicode"}
    } | {
        str(m.get("target") or "").lower()
        for m in (mappings or [])
        if str(m.get("transform") or "").lower() in {"strip_controls", "normalize_unicode"}
    }
    findings: list[dict[str, Any]] = []
    checked = 0
    for row_idx, row in enumerate(rows[:200], start=1):
        if not isinstance(row, dict):
            continue
        for col, val in row.items():
            if val is None:
                continue
            if str(col).lower() in sanitized_cols:
                continue
            text = wire_cell_text(val)
            checked += 1
            if "\ufffd" in text:
                findings.append({
                    "column": str(col),
                    "row": row_idx,
                    "message": "replacement character (U+FFFD) detected — encoding mismatch",
                    "chars": ["U+FFFD"],
                    "sample": text[:500],
                    "suggested_fix": "Re-encode the source as UTF-8, or apply strip_controls and quarantine remaining bad cells.",
                    "suggested_transform": "strip_controls",
                    "blocking": True,
                })
                continue
            controls, formats = _format_control_chars(text)
            bad = controls + formats
            if bad:
                findings.append({
                    "column": str(col),
                    "row": row_idx,
                    "message": f"format-control character detected ({', '.join(bad)}) — normalize before transfer",
                    "chars": bad,
                    "sample": text[:500],
                    "suggested_fix": (
                        f"Column '{col}' contains invisible format/control characters "
                        f"({', '.join(bad)}). Apply strip_controls to sanitize (warehouse-safe) "
                        "or quarantine affected rows — never drop silently."
                    ),
                    "suggested_transform": "strip_controls",
                    "blocking": bool(controls),
                })
        if len(findings) >= 12:
            break

    mode = (validation_mode or "strict").strip().lower()
    # Strict/maximum: U+FFFD and control chars block — they break Snowflake/PG/
    # MySQL loads. Format characters only warn: they load and read back intact.
    # Balanced: every finding warns + strip_controls fix path so a single bad
    # cell in legacy data does not hard-stop Validate; never silent-pass.
    issue_payload: list[Any] = findings[:12] if findings else []
    blocking = [] if mode == "balanced" else [f for f in issue_payload if f["blocking"]]
    advisory = [f for f in issue_payload if f not in blocking]
    blocks = bool(blocking)
    return {
        "check": "encoding_anomalies",
        "passed": not blocks,
        "blocks_transfer": blocks,
        "issues": blocking,
        "warnings": [
            (
                f"{f.get('column')}: {f.get('message')} — apply strip_controls "
                "or quarantine before Run"
            )
            for f in advisory
        ],
        "values_checked": checked,
        "affected_columns": sorted({f["column"] for f in findings}),
        "suggested_transform": "strip_controls" if findings else None,
        "encoding_findings": issue_payload,
    }


def _check_null_keys(
    key_columns: list[str],
    null_rows: int,
    *,
    sync_mode: str,
    dest_kind: str,
) -> dict[str, Any]:
    """D46: population rows whose identity key is NULL, from the source probe.

    A keyed write matches on the key, and a NULL matches nothing: the row can
    neither update nor insert. Found only at write time, it surfaced as an
    unrelated "column not on the destination" failure after rows had landed.
    """
    from services.primary_key import sync_requires_unique_identity
    from services.sync_cursor import normalize_sync_mode

    label = " + ".join(key_columns) or "the key"
    keyed = dest_kind in SCHEMALESS_DESTS or sync_requires_unique_identity(
        normalize_sync_mode(sync_mode), dest_kind=dest_kind
    )
    message = (
        f"{label}: {null_rows:,} source row(s) have a NULL upsert key — a keyed "
        "write cannot match or insert a row without its key. Fill the key, "
        "filter those rows out, or upsert on a key that is never NULL."
    )
    if not keyed:
        return {
            "check": "null_keys",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": [message],
        }
    return {
        "check": "null_keys",
        "passed": False,
        "blocks_transfer": True,
        "issues": [message],
        "null_key_rows": int(null_rows),
        "primary_key_columns": list(key_columns),
    }


def _check_transformed_key_collisions(
    probe: dict[str, Any],
    *,
    sync_mode: str,
    dest_kind: str,
    identity_declared: bool = False,
) -> dict[str, Any]:
    """D39: distinct source keys that a key-column transform writes as one key."""
    from services.primary_key import sync_requires_unique_identity
    from services.sync_cursor import is_overwrite_sync, normalize_sync_mode

    count = int(probe.get("collision_count") or 0)
    label = " + ".join(probe.get("key_columns") or []) or "the key"
    transforms = ", ".join(
        f"{col}: {xform}" for col, xform in (probe.get("transforms") or {}).items()
    )
    sample = "; ".join(
        f"{' vs '.join(repr(k) for k in c.get('source_keys') or [])} "
        f"-> {c.get('written_key')!r}"
        for c in (probe.get("collisions") or [])[:3]
    )
    message = (
        f"{label}: {count:,} source row(s) share a key only after the key's "
        f"transform ({transforms}) - {sample}. The destination key would treat them as one "
        "row and keep one of them. Drop the transform from the key, dedupe the "
        "source, or choose a key the transform leaves distinct."
    )
    # An overwrite recreates the table with the declared key as its PK, so the
    # same collision is a unique violation there.
    keyed = (
        dest_kind in SCHEMALESS_DESTS
        or sync_requires_unique_identity(
            normalize_sync_mode(sync_mode), dest_kind=dest_kind
        )
        or (identity_declared and is_overwrite_sync(sync_mode))
    )
    if not keyed:
        return {
            "check": "transformed_key_collisions",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": [message],
        }
    return {
        "check": "transformed_key_collisions",
        "passed": False,
        "blocks_transfer": True,
        "issues": [message],
        "collision_count": count,
        "primary_key_columns": list(probe.get("key_columns") or []),
    }


def run_integrity_audit(
    *,
    source_columns: list[str],
    target_columns: list[str] | None = None,
    mappings: list[dict] | None = None,
    source_schemas: list[dict] | None = None,
    target_schemas: list[dict] | None = None,
    source_samples: dict[str, list[str]] | None = None,
    destination_db_type: str = "",
    source_db_type: str = "",
    sample_rows: list[dict] | None = None,
    validation_mode: str = "strict",
    schema_policy: str = "manual_review",
    sync_mode: str = "",
    contract_primary_key: str | None = None,
    destination_pk_columns: list[str] | None = None,
    destination_unique_keys: list[dict[str, Any]] | None = None,
    source_duplicate_findings: list[dict[str, Any]] | None = None,
    source_duplicate_probe_ran: bool = False,
    source_duplicate_probe_pk: str = "",
    source_duplicate_probe_status: str = "",
    source_duplicate_probe_message: str = "",
    source_duplicate_probe_expected: bool = False,
    source_null_key_rows: int | None = None,
    source_key_collisions: dict[str, Any] | None = None,
    dest_table_exists: bool | None = None,
    empty_cells_as_null: bool = False,
    dest_nullability: dict[str, bool] | None = None,
    database_extract: bool = False,
    file_row_quarantine: bool = False,
    source_measured_empty: bool = False,
    stream_contracts: list[dict] | None = None,
    proven_width_narrowings: dict | None = None,
    in_file_dedupe: str = "",
    duplicate_policy: str = "",
) -> dict[str, Any]:
    """
    Run all critical data integrity checks in one pass.
    Returns a structured report used by mapping pipeline and preflight G9.
    """
    cfg = _mode_config(validation_mode)
    dest_kind = normalize_dest_kind(destination_db_type)

    mappings = mappings or []
    source_schemas = source_schemas or []
    target_schemas = target_schemas or []

    source_types = {s["name"]: s.get("inferred_type", "VARCHAR") for s in source_schemas}
    if not source_types and source_columns:
        source_types = {c: "VARCHAR" for c in source_columns}
    target_types = {s["name"]: s.get("inferred_type", "VARCHAR") for s in target_schemas}
    if not target_types and target_columns:
        target_types = {c: "VARCHAR" for c in target_columns}

    rows = _rows_from_samples(source_columns, source_samples, sample_rows)

    # Canonical identity key — same helper as G6/G8. Schemaless → `_id` only;
    # SQL required-nulls include `*_id` only in strict/maximum.
    from services.primary_key import resolve_primary_key_source

    mode = (validation_mode or "strict").strip().lower()
    pk_nulls = resolve_primary_key_source(
        mappings,
        source_columns,
        dest_kind,
        validation_mode=mode,
        purpose="required_nulls",
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
    )
    # Duplicate checks must use a key that is actually expected to be unique.
    # Required-nulls may fall back to the first `*_id` FK in strict mode, which
    # legitimately repeats; uniqueness resolution stays conservative.
    pk_uniqueness = resolve_primary_key_source(
        mappings,
        source_columns,
        dest_kind,
        validation_mode=mode,
        purpose="uniqueness",
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
    )
    # Composite-aware identity (ACC-03): Studio contracts / destination PKs can
    # name a multi-column key. The sample/probe dedupe must run on the whole
    # tuple — checking any one component alone reports false duplicates for
    # values that legitimately repeat inside a composite key.
    from services.primary_key import resolve_primary_key_source_columns

    pk_columns_uniqueness = resolve_primary_key_source_columns(
        mappings,
        source_columns,
        dest_kind,
        validation_mode=mode,
        purpose="uniqueness",
        destination_pk_columns=destination_pk_columns,
        contract_primary_key=contract_primary_key,
        stream_contracts=stream_contracts,
    )
    if len(pk_columns_uniqueness) > 1 and not pk_uniqueness:
        # Label for operator output; the check itself reads the column list.
        pk_uniqueness = pk_columns_uniqueness[0]
    if len(pk_columns_uniqueness) <= 1:
        pk_columns_uniqueness = [pk_uniqueness] if pk_uniqueness else []
    identity_declared = bool(
        str(contract_primary_key or "").strip()
        or len(pk_columns_uniqueness) > 1
        or destination_pk_columns
        or _stream_contracts_declare_identity(stream_contracts)
    )

    checks: list[dict[str, Any]] = []

    # Validation without a schema or a representative sample proves nothing.
    # Fail closed instead of returning a green "No integrity checks run" report.
    if not source_columns:
        checks.append({
            "check": "source_columns_available",
            "passed": False,
            "blocks_transfer": True,
            "issues": ["No source columns available for integrity validation"],
        })
    if not rows and source_measured_empty:
        checks.append({
            "check": "sample_available",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": ["Source holds 0 rows (read, not assumed) — no value to audit"],
        })
    elif not rows:
        checks.append({
            "check": "sample_available",
            "passed": False,
            "blocks_transfer": True,
            "issues": ["No sample rows available for integrity validation"],
        })

    if mappings:
        checks.append(
            _check_coercion_safety(
                mappings,
                source_types,
                target_types,
                dest_kind=dest_kind,
                source_db_type=source_db_type,
                schema_policy=schema_policy,
                validation_mode=validation_mode,
                rows=rows,
                dest_table_exists=dest_table_exists,
                proven_width_narrowings=proven_width_narrowings,
            )
        )
        checks.append(
            _check_transform_dry_run(
                mappings,
                source_columns,
                source_types,
                rows,
                dest_kind=dest_kind,
                target_types=target_types,
                empty_cells_as_null=empty_cells_as_null,
                database_extract=database_extract,
                dest_nullability=dest_nullability,
                file_row_quarantine=file_row_quarantine,
            )
        )
        checks.append(
            _check_financial_precision(
                mappings,
                source_types,
                rows,
                file_row_quarantine=file_row_quarantine,
            )
        )
        checks.append(
            _check_required_nulls(
                mappings,
                rows,
                null_rate_max=cfg["null_rate_max"],
                dest_kind=dest_kind,
                primary_key=pk_nulls,
                validation_mode=validation_mode,
                source_types=source_types,
                empty_cells_as_null=empty_cells_as_null,
                database_extract=database_extract,
            )
        )
        # Duplicate identity keys block when the write path will enforce uniqueness:
        # schemaless destinations, upsert/CDC/mirror/SCD2, overwrite (table recreated),
        # or append when the destination PK is known to include the mapped target.
        checks.append(
            _check_duplicate_keys(
                mappings,
                rows,
                validation_mode,
                dest_kind=dest_kind,
                primary_key=pk_uniqueness,
                primary_key_columns=pk_columns_uniqueness,
            identity_declared=identity_declared,
                source_duplicate_findings=source_duplicate_findings,
                sync_mode=sync_mode,
                destination_pk_columns=destination_pk_columns,
                destination_unique_keys=destination_unique_keys,
                target_types=target_types,
                source_duplicate_probe_status=source_duplicate_probe_status,
                source_duplicate_probe_message=source_duplicate_probe_message,
                source_duplicate_probe_expected=source_duplicate_probe_expected,
                in_file_dedupe=in_file_dedupe,
                duplicate_policy=duplicate_policy,
            )
        )
        if source_null_key_rows:
            checks.append(
                _check_null_keys(
                    pk_columns_uniqueness,
                    int(source_null_key_rows),
                    sync_mode=sync_mode,
                    dest_kind=dest_kind,
                )
            )
        if source_key_collisions and source_key_collisions.get("collision_count"):
            checks.append(
                _check_transformed_key_collisions(
                    source_key_collisions,
                    sync_mode=sync_mode,
                    dest_kind=dest_kind,
                    identity_declared=identity_declared,
                )
            )
        checks.append(
            _check_mapping_confidence(mappings, confidence_min=cfg["confidence"], validation_mode=validation_mode)
        )

    # Mongo majority typing can keep INTEGER while a few TEXT sentinels remain —
    # warn (never silent) so Validate honesty matches write-time quarantine risk.
    mix_warnings = [
        f"{s.get('name')}: {s.get('type_mix_warning')}"
        for s in source_schemas
        if s.get("type_mix_warning") and s.get("name")
    ]
    if mix_warnings:
        checks.append({
            "check": "mongo_type_mix",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": mix_warnings[:12],
        })

    if rows and source_columns:
        checks.append(_check_sample_quality(source_columns, rows, source_types, validation_mode, dest_kind=dest_kind))

    if rows:
        checks.append(
            _check_encoding_anomalies(rows, validation_mode=validation_mode, mappings=mappings)
        )

    # Industry-standard expectation suite (dbt/GX patterns)
    if rows and source_columns:
        from services.expectations_engine import run_auto_expectations

        exp = run_auto_expectations(
            rows,
            source_columns,
            source_types,
            primary_key=pk_uniqueness,
            primary_key_columns=pk_columns_uniqueness,
            dest_kind=dest_kind,
            validation_mode=validation_mode,
            sync_mode=sync_mode,
            in_file_dedupe=in_file_dedupe,
        )
        if file_row_quarantine:
            exp = _demote_mixed_file_numeric_expectations(exp, rows, mappings)
        checks.append({
            "check": "expectations_suite",
            "passed": exp.get("passed", True),
            "blocks_transfer": exp.get("blocks_transfer", False),
            "issues": [
                f"{f['expectation']}:{f['column']}: {f['failing_count']} failures"
                for f in exp.get("blocking_failures", [])
            ][:15],
            "details": {
                "expectations_run": exp.get("expectations_run", 0),
                "expectations_passed": exp.get("expectations_passed", 0),
            },
        })

    # Module 3: mapping_confidence never blocks_transfer (G4 is SSOT), so the
    # former balanced-mode "downgrade confidence blockers" path is gone.

    if mode == "lenient":
        # Cell and sample findings quarantine at write. They must not refuse
        # the load. A missing schema or an unreadable sample still blocks:
        # there is nothing to quarantine against.
        structural = {"source_columns_available", "sample_available"}
        for check in checks:
            if check.get("check") in structural:
                continue
            if check.get("blocks_transfer"):
                check["blocks_transfer"] = False
                check["lenient_quarantine"] = True
                note = "bad rows will quarantine; good rows still load"
                warnings = list(check.get("warnings") or [])
                warnings.append(note)
                check["warnings"] = warnings

    passed_checks = [c for c in checks if c.get("passed")]
    failed_checks = [c for c in checks if not c.get("passed")]
    blocks = any(c.get("blocks_transfer") for c in checks)
    all_issues = [issue for c in failed_checks for issue in c.get("issues", [])]
    all_warnings: list[str] = []
    for c in checks:
        for w in c.get("warnings") or []:
            all_warnings.append(str(w))

    return {
        "passed": not blocks,
        "blocks_transfer": blocks,
        "validation_mode": validation_mode,
        "checks_run": len(checks),
        "checks_passed": len(passed_checks),
        "checks_failed": len(failed_checks),
        "checks": checks,
        "issues": all_issues[:30],
        "warnings": all_warnings[:20],
        "validation_coverage": stamp_validation_coverage(
            layer="sample",
            rows_examined=len(rows) if rows else 0,
            note=(
                "G9 integrity audit examines Validate sample rows only — "
                "not population proof."
            ),
        ),
        "source_uniqueness_probe": {
            "ran": bool(source_duplicate_probe_ran),
            "primary_key": str(source_duplicate_probe_pk or "") or None,
            "finding_count": len(source_duplicate_findings or []),
            "coverage": "full_selected" if source_duplicate_probe_ran else "sample",
            "status": (source_duplicate_probe_status or (
                "ran" if source_duplicate_probe_ran else "sample"
            )),
            "message": (source_duplicate_probe_message or "")[:400] or None,
            "expected": bool(source_duplicate_probe_expected),
        },
        "summary": (
            f"{len(passed_checks)}/{len(checks)} integrity checks passed"
            + (
                " (Validate sample — population uniqueness not proven)"
                if not source_duplicate_probe_ran
                else " (full-selected uniqueness probe · other checks on sample)"
            )
            if checks
            else "No integrity checks run (missing mappings or samples)"
        ),
    }
