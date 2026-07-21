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
) -> dict[str, Any]:
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

    blocks = [i for i in issues if i.get("severity") == "block"]
    return {
        "check": "coercion_safety",
        "passed": len(blocks) == 0,
        "blocks_transfer": coerce_blocks_transfer(issues),
        "issues": [i["message"] for i in blocks[:15]],
        "warnings": [i["message"] for i in issues if i.get("severity") == "warn"][:10],
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
            converted, err = apply_transform(raw, transform)
            if err:
                issues.append(f"{src}: unparseable financial value {raw!r}")
                continue
            try:
                original_parsed, original_err = apply_transform(raw, "decimal")
                if original_parsed is None or original_err:
                    issues.append(f"{src}: unparseable financial value {raw!r}")
                    continue
                original = Decimal(str(original_parsed))
                result = Decimal(str(converted))
                if original != 0 and result != 0:
                    ratio = abs(result / original)
                    if ratio < 0.01 or ratio > 100:
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
    """Only enforce nullability on the inferred primary key and canonical key columns.

    Foreign-key / PII columns (email, phone, ssn, user_id, etc.) can legitimately
    be sparse in source data; blocking transfer because of nulls in those
    columns causes false preflight failures for schemaless/NoSQL sources.
    In strict/maximum mode we also hold `*_id` columns to the same standard.
    """
    issues: list[str] = []
    schemaless = dest_kind in SCHEMALESS_DESTS
    mode = (validation_mode or "strict").strip().lower()

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
        # Exact canonical key columns are always required; `*_id` columns are
        # also treated as required in strict/maximum validation mode.
        reserved_exact = {"id", "_id", "uuid", "pk", "key"}
        is_reserved_key = src_lower in reserved_exact or tgt_lower in reserved_exact
        if mode in {"strict", "maximum"} and not is_reserved_key:
            is_reserved_key = src_lower.endswith("_id") or tgt_lower.endswith("_id")
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


def _check_duplicate_keys(
    mappings: list[dict],
    rows: list[dict[str, Any]],
    validation_mode: str = "strict",
    *,
    dest_kind: str = "",
    primary_key: str | None = None,
) -> dict[str, Any]:
    issues: list[str] = []
    schemaless = dest_kind in SCHEMALESS_DESTS
    if not primary_key:
        return {
            "check": "duplicate_keys",
            "passed": True,
            "blocks_transfer": False,
            "issues": [],
        }
    pk_targets = set()
    for m in mappings:
        if m.get("source", "") == primary_key:
            pk_targets.add(m.get("target", "").lower())
    if not pk_targets:
        pk_targets = {primary_key.lower()}
    for m in mappings:
        tgt = m.get("target", "").lower()
        src = m.get("source", "")
        if schemaless and tgt != "_id":
            continue
        if not schemaless and tgt not in pk_targets:
            continue
        seen: dict[str, int] = {}
        for row in rows:
            val = cell_to_string(row.get(src, "")).strip()
            if not val:
                continue
            seen[val] = seen.get(val, 0) + 1
        dupes = [(v, c) for v, c in seen.items() if c > 1]
        if dupes:
            sample = ", ".join(f"{v}×{c}" for v, c in dupes[:3])
            issues.append(f"{src}: duplicate key values ({sample})")
    # Duplicate primary key values are only a hard blocker in strict/maximum modes.
    mode = (validation_mode or "strict").strip().lower()
    blocks = len(issues) > 0 and mode in {"strict", "maximum"}
    return {
        "check": "duplicate_keys",
        "passed": not blocks,
        "blocks_transfer": blocks,
        "issues": issues[:15],
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
    # In strict/maximum mode, hold mappings to the full configured threshold.
    # In balanced mode, align with the preflight G4 confidence floor so a
    # mapping that is accepted by the mapping gate is not rejected again here.
    mode = (validation_mode or "strict").strip().lower()
    floor = confidence_min if mode in {"strict", "maximum"} else max(0.55, confidence_min - 0.3)
    issues: list[str] = []
    warnings: list[str] = []
    for m in mappings:
        conf = float(m.get("confidence", 0))
        if conf < floor:
            issues.append(
                f"{m.get('source')}→{m.get('target')}: confidence {conf:.0%} < {floor:.0%}"
            )
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

    Always blocks at Validate when findings exist — operators must apply
    ``strip_controls`` (or clean the source) before Run. Columns already mapped
    with ``strip_controls`` / ``normalize_unicode`` are skipped.
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
    # Control/format characters break Snowflake/PG/MySQL loads — always block at
    # Validate with an explicit strip_controls fix path (never discover at Run).
    del mode
    blocks = bool(findings)
    issue_payload: list[Any] = findings[:12] if findings else []
    return {
        "check": "encoding_anomalies",
        "passed": not blocks,
        "blocks_transfer": blocks,
        "issues": issue_payload,
        "warnings": [],
        "values_checked": checked,
        "affected_columns": sorted({f["column"] for f in findings}),
        "suggested_transform": "strip_controls" if findings else None,
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

    # Primary-key heuristic: exact canonical key columns (`_id`, `id`, `uuid`,
    # `pk`, `key`) are always treated as required/unique. In strict/maximum mode
    # we also treat `*_id` columns as keys so high-assurance transfers enforce
    # completeness, while balanced/review modes allow sparse foreign keys such as
    # `user_id` or `account_id` in NoSQL/CRM extracts.
    mode = (validation_mode or "strict").strip().lower()
    pk = None
    preferred = ("_id", "id", "uuid", "pk", "key") if dest_kind not in SCHEMALESS_DESTS else ("_id",)
    for key in preferred:
        for m in mappings:
            if (m.get("target") or "").lower() == key:
                pk = m.get("source")
                break
        if not pk:
            pk = next((c for c in source_columns if c.lower() == key), None)
        if pk:
            break
    if not pk and mode in {"strict", "maximum"}:
        for col in source_columns:
            if col.lower().endswith("_id"):
                pk = col
                break

    checks: list[dict[str, Any]] = []

    if mappings:
        checks.append(
            _check_coercion_safety(
                mappings,
                source_types,
                target_types,
                dest_kind=dest_kind,
                schema_policy=schema_policy,
                validation_mode=validation_mode,
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
        checks.append(_check_required_nulls(mappings, rows, null_rate_max=cfg["null_rate_max"], dest_kind=dest_kind, primary_key=pk, validation_mode=validation_mode))
        checks.append(_check_duplicate_keys(mappings, rows, validation_mode, dest_kind=dest_kind, primary_key=pk))
        checks.append(
            _check_mapping_confidence(mappings, confidence_min=cfg["confidence"], validation_mode=validation_mode)
        )

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
            primary_key=pk,
            dest_kind=dest_kind,
            validation_mode=validation_mode,
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
        "summary": (
            f"{len(passed_checks)}/{len(checks)} integrity checks passed"
            if checks
            else "No integrity checks run (missing mappings or samples)"
        ),
    }
