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

from services.db_type_utils import SCHEMALESS_DESTS, normalize_dest_kind
from services.value_serializer import cell_to_string

# Validation mode → minimum confidence / null tolerance
_MODE_THRESHOLDS = {
    "maximum": {"confidence": 0.95, "null_rate_max": 0.0, "parse_fail_max": 0.0},
    "strict": {"confidence": 0.85, "null_rate_max": 0.05, "parse_fail_max": 0.02},
    "balanced": {"confidence": 0.75, "null_rate_max": 0.15, "parse_fail_max": 0.05},
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


def _check_coercion_safety(
    mappings: list[dict],
    source_types: dict[str, str],
    target_types: dict[str, str],
    *,
    dest_kind: str = "",
    schema_policy: str = "manual_review",
    validation_mode: str = "strict",
    rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    from services.coercion_probe import samples_coerce_mapping
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )

    floor = float(_mode_config(validation_mode)["confidence"])
    issues = validate_mapping_coercions(
        mappings,
        source_types=source_types,
        target_types=target_types,
        schema_policy=schema_policy,
        confidence_floor=floor,
    )
    schemaless = dest_kind in SCHEMALESS_DESTS
    if schemaless:
        # Schemaless destinations store values as-is; strict type coercion checks
        # are not transfer blockers.
        return {
            "check": "coercion_safety",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": [i["message"] for i in issues if i.get("severity") in {"warn", "block"}][:10],
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
        from services.type_system import is_precision_collapse_coercion

        src_t = str(source_types.get(src) or "")
        from services.type_system import resolve_mapping_target_type

        tgt_t = resolve_mapping_target_type(
            mapping or {"target": issue.get("target")},
            target_types=target_types,
            source_type=src_t,
            dest_db_type=dest_kind,
        )
        if mapping and is_precision_collapse_coercion(src_t, tgt_t, dest_db=dest_kind):
            hardened.append(issue)
            continue
        from services.type_system import is_lossy_coercion

        risk_ack = bool(
            mapping
            and (mapping.get("risk_acknowledged") or mapping.get("riskAcknowledged"))
        )
        # Match G3: declared lossy cannot be sample-cleared without risk ack.
        if mapping and is_lossy_coercion(src_t, tgt_t) and not risk_ack:
            hardened.append(issue)
            continue
        if mapping and samples_coerce_mapping(
            mapping,
            source_types=source_types,
            target_types=target_types,
            rows=sample_rows,
        ):
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
) -> dict[str, Any]:
    if not rows or not mappings:
        return {"check": "transform_dry_run", "passed": True, "blocks_transfer": False, "issues": []}

    headers = source_columns or list(rows[0].keys())
    sample_rows = [[cell_to_string(row.get(h, "")) for h in headers] for row in rows[:200]]
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
    )
    missing_col_errors = [e for e in errors if "Source column missing" in e]
    schemaless = dest_kind in SCHEMALESS_DESTS
    if schemaless and not missing_col_errors:
        # Schemaless stores values as-is; transform failures (e.g. typed casts
        # inferred from an unknown target schema) should not block preflight.
        return {
            "check": "transform_dry_run",
            "passed": True,
            "blocks_transfer": False,
            "issues": errors[:20],
        }
    issues = list(errors[:20])
    if not ok and issues:
        # Preflight quarantine rows are inspect-only — the job does not continue.
        issues.insert(
            0,
            "Preflight blocked the transfer (0 rows written). "
            "Findings below are for inspection — fix Map types/targets, then re-Validate. "
            "Write-time quarantine only applies after preflight passes.",
        )
    return {
        "check": "transform_dry_run",
        "passed": ok,
        "blocks_transfer": not ok,
        "issues": issues,
    }


def _check_financial_precision(
    mappings: list[dict],
    source_types: dict[str, str],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Detect values that could silently lose magnitude (e.g. comma/currency parsing failures)."""
    from services.transform_engine import apply_transform, infer_transform_for_mapping

    issues: list[str] = []
    for m in mappings:
        src = m.get("source", "")
        tgt = m.get("target", "")
        if not _FINANCIAL_NAME_PATTERNS.search(src) and not _FINANCIAL_NAME_PATTERNS.search(tgt):
            continue
        src_type = source_types.get(src, "VARCHAR")
        transform = m.get("transform") or infer_transform_for_mapping(
            src, tgt, src_type, m.get("target_type"),
        )
        if transform not in {"decimal", "integer", "currency", "percentage"}:
            continue
        values = [cell_to_string(row.get(src, "")).strip() for row in rows if row.get(src) not in (None, "")]
        for raw in values[:100]:
            if not raw or raw in {"0", "0.0", "0.00"}:
                continue
            # Schemaless missing and SQL NULL sentinels are not financial values.
            if raw in {"__DF_MISSING__", "__df_sql_null__"}:
                continue
            converted, err = apply_transform(raw, transform)
            if err:
                issues.append(f"{src}: unparseable financial value {raw!r}")
                continue
            # Null/missing sentinel coerced to None is a valid absence, not a parse failure.
            if converted is None:
                continue
            try:
                original_parsed, original_err = apply_transform(raw, "decimal")
                if original_err:
                    issues.append(f"{src}: unparseable financial value {raw!r}")
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
    blocks = len(issues) > 0
    return {
        "check": "financial_precision",
        "passed": not blocks,
        "blocks_transfer": blocks,
        "issues": issues[:15],
    }


def _check_required_nulls(
    mappings: list[dict],
    rows: list[dict[str, Any]],
    *,
    null_rate_max: float,
    dest_kind: str = "",
    primary_key: str | None = None,
    validation_mode: str = "strict",
) -> dict[str, Any]:
    """Enforce nullability on the resolved identity key and exact canonical keys only.

    Optional FK / OAuth fields (``googleId`` → ``google_id``, ``providerId``,
    ``user_id``, …) are often sparse on Mongo/NoSQL sources. Treating every
    ``*_id`` target as required falsely blocked Validate while the real PK
    (``_id`` / ``id``) was populated. Strict mode still requires the resolved
    primary-key source (including a sole ``*_id`` when that is the identity).
    """
    issues: list[str] = []
    schemaless = dest_kind in SCHEMALESS_DESTS
    _ = (validation_mode or "strict").strip().lower()

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

        values = [row.get(src) for row in rows]
        if not values:
            continue
        empty = sum(1 for v in values if cell_to_string(v) == "")
        rate = empty / len(values)
        if rate > null_rate_max:
            issues.append(f"{src}: {rate:.0%} null/empty (max {null_rate_max:.0%} for required field)")
    blocks = len(issues) > 0
    return {
        "check": "required_nulls",
        "passed": not blocks,
        "blocks_transfer": blocks,
        "issues": issues[:15],
    }


def _is_append_like(sync_mode: str) -> bool:
    return (sync_mode or "").strip().lower() in {
        "full_refresh_append",
        "incremental_append",
        "append",
        "append_only",
    }


def _is_overwrite_like(sync_mode: str) -> bool:
    return (sync_mode or "").strip().lower() in {
        "full_refresh_overwrite",
        "overwrite",
        "full_refresh",
        "replace",
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
            raw_cell = row.get(src, row.get(dest_col, ""))
            raw = cell_to_string(raw_cell) if raw_cell is not None else ""
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


def _destination_constraints_advisory(
    dest_kind: str,
    destination_unique_keys: list[dict[str, Any]] | None = None,
) -> bool:
    """True when dest PK/UNIQUE are optimizer/metadata-only (BQ / Redshift / SF NOT ENFORCED)."""
    kind = normalize_dest_kind(dest_kind or "")
    if kind in {"bigquery", "redshift"}:
        return True
    # Snowflake hybrid may mix; only advisory when every covering key says so.
    keys = list(destination_unique_keys or [])
    if keys and all(uk.get("enforced") is False for uk in keys):
        return True
    return False


def _unique_constraint_enforced(
    uk: dict[str, Any] | None,
    *,
    dest_kind: str = "",
) -> bool:
    if uk is not None and uk.get("enforced") is False:
        return False
    if uk is not None and uk.get("enforced") is True:
        return True
    return not _destination_constraints_advisory(dest_kind, [uk] if uk else None)


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


def _check_duplicate_keys(
    mappings: list[dict],
    rows: list[dict[str, Any]],
    validation_mode: str = "strict",
    *,
    dest_kind: str = "",
    primary_key: str | None = None,
    source_duplicate_findings: list[dict[str, Any]] | None = None,
    sync_mode: str = "",
    destination_pk_columns: list[str] | None = None,
    destination_unique_keys: list[dict[str, Any]] | None = None,
    target_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Duplicate check on the resolved identity key (sample + source-side probe).

    - Schemaless destinations (Mongo/Redis/Dynamo) and sync modes that require a
      unique identity (upsert/CDC/mirror/SCD2) always enforce duplicates.
    - Overwrite modes enforce duplicates because the destination table is recreated
      and the mapped identity key is likely to become the primary key.
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
    sync = (sync_mode or "").strip().lower()
    target_col = _target_for_source(primary_key, mappings) if primary_key else ""
    advisory_warnings = _advisory_unique_key_warnings(
        dest_kind=dest_kind,
        destination_unique_keys=destination_unique_keys,
        mappings=mappings,
        rows=rows,
        destination_pk_columns=destination_pk_columns,
        target_types=target_types,
    )

    # Single-column identity enforcement (upsert/CDC/PK/single UNIQUE).
    enforce_identity = bool(primary_key) and (
        schemaless
        or sync_requires_unique_identity(sync, dest_kind=dest_kind)
        or _is_overwrite_like(sync)
        or not _is_append_like(sync)
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
    composite_issues = _check_destination_unique_constraints(
        mappings,
        rows,
        destination_pk_columns=destination_pk_columns,
        destination_unique_keys=destination_unique_keys,
        target_types=target_types,
        dest_kind=dest_kind,
    )
    issues.extend(composite_issues)

    if not primary_key and not issues:
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
    if primary_key:
        findings = source_duplicate_findings or []
        if findings:
            sample = ", ".join(
                f"{f.get('value')}×{f.get('count', 1)}" for f in findings[:3]
            )
            issues.append(
                f"{primary_key}: duplicate key values from source probe ({sample})"
            )
            probe_authoritative = True

    if not enforce_identity and not issues:
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
    run_single = bool(
        enforce_identity
        and primary_key
        and not (covering_composite_only and not covering_single)
    )
    if run_single and primary_key:
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
            raw_cell = row.get(primary_key, "")
            raw = cell_to_string(raw_cell) if raw_cell is not None else ""
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
            seen[key] = seen.get(key, 0) + 1
            examples.setdefault(key, raw if raw else "<NULL>")
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
    # Full-table probe found duplicates → always block. Quarantine/Strip/balanced
    # remediations must not enable Execute when the write batch will fail DQ.
    if probe_authoritative and blocks:
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
    if blocks and mode == "balanced":
        # Balanced may warn-only for append-like routes where sample-only
        # duplicates can be legal. Upsert/CDC/mirror/SCD2/overwrite/schemaless
        # still fail-closed — Studio "strip+rerun balanced" must not green-light
        # PK collisions that write-time DQ will refuse.
        must_block = (
            schemaless
            or sync_requires_unique_identity(sync, dest_kind=dest_kind)
            or _is_overwrite_like(sync)
            or not _is_append_like(sync)
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
    # Keep G9 aligned with the published/preflight mode floors:
    # maximum=0.95, strict=0.85, balanced=0.75.
    mode = (validation_mode or "strict").strip().lower()
    floor = confidence_min
    issues: list[str] = []
    warnings: list[str] = []
    for m in mappings:
        # Align with G4: operator override / risk ack already cleared Map confidence.
        # Re-blocking here made Validate contradict "All mappings meet confidence floor".
        if m.get("user_override") or m.get("risk_acknowledged") or m.get("riskAcknowledged"):
            continue
        conf = float(m.get("confidence", 0))
        if conf < floor:
            msg = f"{m.get('source')}→{m.get('target')}: confidence {conf:.0%} < {floor:.0%}"
            # Low confidence is a blocker by default; in balanced mode it is
            # downgraded to a warning when a more concrete issue already blocks,
            # so the operator sees the real problem (e.g. lossy coercion).
            issues.append(msg)
        elif m.get("requires_review"):
            # In balanced mode a near-threshold mapping with a small gap is a
            # warning, not a hard blocker, so the user can review without being
            # stopped entirely. In strict/maximum it stays a blocker.
            msg = f"{m.get('source')}→{m.get('target')}: ambiguous mapping requires review"
            if mode in {"strict", "maximum"}:
                issues.append(msg)
            else:
                warnings.append(msg)
    blocks = len(issues) > 0
    return {
        "check": "mapping_confidence",
        "passed": not blocks,
        "blocks_transfer": blocks,
        "issues": issues[:20],
        "warnings": warnings[:10],
    }


def _format_control_chars(text: str) -> list[str]:
    """Return U+XXXX codes for format/control chars that warehouses often reject."""
    found: list[str] = []
    for ch in text:
        cat = unicodedata.category(ch)
        if cat == "Cf" or (cat == "Cc" and ch not in "\t\n\r"):
            code = f"U+{ord(ch):04X}"
            if code not in found:
                found.append(code)
            if len(found) >= 6:
                break
    return found


def _check_encoding_anomalies(
    rows: list[dict[str, Any]],
    *,
    validation_mode: str = "strict",
    mappings: list[dict] | None = None,
) -> dict[str, Any]:
    """Flag replacement / format-control chars that break warehouse loads.

    Strict/maximum: block at Validate. Balanced: warn + strip_controls path
    (never silent-pass). Columns already mapped with ``strip_controls`` /
    ``normalize_unicode`` are skipped.
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
            text = cell_to_string(val)
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
                })
                continue
            bad = _format_control_chars(text)
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
                })
        if len(findings) >= 12:
            break

    mode = (validation_mode or "strict").strip().lower()
    # Strict/maximum: always block — control chars break Snowflake/PG/MySQL loads.
    # Balanced: surface findings as warnings + strip_controls fix path so a single
    # U+200B in legacy data does not hard-stop Validate; never silent-pass.
    issue_payload: list[Any] = findings[:12] if findings else []
    if mode == "balanced" and findings:
        return {
            "check": "encoding_anomalies",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
            "warnings": [
                (
                    f"{f.get('column')}: {f.get('message')} — apply strip_controls "
                    "or quarantine before Run"
                )
                for f in issue_payload
            ],
            "values_checked": checked,
            "affected_columns": sorted({f["column"] for f in findings}),
            "suggested_transform": "strip_controls",
            "encoding_findings": issue_payload,
        }
    blocks = bool(findings)
    return {
        "check": "encoding_anomalies",
        "passed": not blocks,
        "blocks_transfer": blocks,
        "issues": issue_payload,
        "warnings": [],
        "values_checked": checked,
        "affected_columns": sorted({f["column"] for f in findings}),
        "suggested_transform": "strip_controls" if findings else None,
        "encoding_findings": issue_payload,
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
    if not rows:
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
                schema_policy=schema_policy,
                validation_mode=validation_mode,
                rows=rows,
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
            )
        )
        checks.append(_check_financial_precision(mappings, source_types, rows))
        checks.append(_check_required_nulls(mappings, rows, null_rate_max=cfg["null_rate_max"], dest_kind=dest_kind, primary_key=pk_nulls, validation_mode=validation_mode))
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
                source_duplicate_findings=source_duplicate_findings,
                sync_mode=sync_mode,
                destination_pk_columns=destination_pk_columns,
                destination_unique_keys=destination_unique_keys,
                target_types=target_types,
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
            dest_kind=dest_kind,
            validation_mode=validation_mode,
            sync_mode=sync_mode,
        )
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

    # Balanced mode: if a more concrete blocker already exists, downgrade
    # low-confidence mapping issues from blockers to warnings so the UI
    # surfaces the root cause (lossy/wrong map) instead of a generic score.
    if mode == "balanced":
        mc = next((c for c in checks if c.get("check") == "mapping_confidence"), None)
        if mc and mc.get("blocks_transfer"):
            other_blockers = [
                c for c in checks
                if c.get("blocks_transfer") and c.get("check") != "mapping_confidence"
            ]
            if other_blockers:
                mc["blocks_transfer"] = False
                mc["passed"] = True
                mc["warnings"] = list(mc.get("warnings", [])) + list(mc.get("issues", []))
                mc["issues"] = []

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
        "source_uniqueness_probe": {
            "ran": bool(source_duplicate_probe_ran),
            "primary_key": str(source_duplicate_probe_pk or "") or None,
            "finding_count": len(source_duplicate_findings or []),
            "coverage": "full_selected" if source_duplicate_probe_ran else "sample",
        },
        "summary": (
            f"{len(passed_checks)}/{len(checks)} integrity checks passed"
            if checks
            else "No integrity checks run (missing mappings or samples)"
        ),
    }
