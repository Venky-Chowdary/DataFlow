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

from services.value_serializer import cell_to_string

# Validation mode → minimum confidence / null tolerance
_MODE_THRESHOLDS = {
    "maximum": {"confidence": 0.95, "null_rate_max": 0.0, "parse_fail_max": 0.0},
    "strict": {"confidence": 0.85, "null_rate_max": 0.05, "parse_fail_max": 0.02},
    "balanced": {"confidence": 0.75, "null_rate_max": 0.15, "parse_fail_max": 0.05},
}

_REQUIRED_NAME_PATTERNS = re.compile(
    r"(^id$|_id$|^uuid$|_uuid$|_key$|^key$|_code$|account_no|acct_no|ssn|mrn)",
    re.IGNORECASE,
)
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
) -> dict[str, Any]:
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )

    issues = validate_mapping_coercions(
        mappings,
        source_types=source_types,
        target_types=target_types,
        schema_policy=schema_policy,
    )
    schemaless = (dest_kind or "").lower() in {"mongodb", "dynamodb", "redis"}
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
) -> dict[str, Any]:
    if not rows or not mappings:
        return {"check": "transform_dry_run", "passed": True, "blocks_transfer": False, "issues": []}

    headers = source_columns or list(rows[0].keys())
    sample_rows = [[cell_to_string(row.get(h, "")) for h in headers] for row in rows[:200]]
    from services.transform_engine import dry_run_sample

    ok, errors = dry_run_sample(
        headers=headers,
        sample_rows=sample_rows,
        mappings=mappings,
        column_types=source_types,
    )
    missing_col_errors = [e for e in errors if "Source column missing" in e]
    schemaless = (dest_kind or "").lower() in {"mongodb", "dynamodb", "redis"}
    if schemaless and not missing_col_errors:
        # Schemaless stores values as-is; transform failures (e.g. typed casts
        # inferred from an unknown target schema) should not block preflight.
        return {
            "check": "transform_dry_run",
            "passed": True,
            "blocks_transfer": False,
            "issues": errors[:20],
        }
    return {
        "check": "transform_dry_run",
        "passed": ok,
        "blocks_transfer": not ok,
        "issues": errors[:20],
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
) -> dict[str, Any]:
    issues: list[str] = []
    schemaless = (dest_kind or "").lower() in {"mongodb", "dynamodb", "redis"}
    for m in mappings:
        src = m.get("source", "")
        tgt = m.get("target", "")
        if schemaless and tgt.lower() != "_id" and src.lower() != "_id":
            # Schemaless documents generate `_id` and do not require every FK.
            continue
        if not (_REQUIRED_NAME_PATTERNS.search(src) or _REQUIRED_NAME_PATTERNS.search(tgt)):
            continue
        values = [row.get(src) for row in rows]
        if not values:
            continue
        empty = sum(1 for v in values if v is None or str(v).strip() == "")
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
    schemaless = (dest_kind or "").lower() in {"mongodb", "dynamodb", "redis"}
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


def _check_encoding_anomalies(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Flag replacement chars and non-normalized unicode that break downstream systems."""
    issues: list[str] = []
    checked = 0
    for row in rows[:200]:
        for val in row.values():
            if val is None:
                continue
            text = str(val)
            if "\ufffd" in text:
                issues.append("replacement character () detected — encoding mismatch")
                break
            if any(unicodedata.category(ch) == "Cf" for ch in text):
                issues.append("format-control character detected — normalize before transfer")
                break
            checked += 1
        if issues:
            break
    return {
        "check": "encoding_anomalies",
        "passed": len(issues) == 0,
        "blocks_transfer": len(issues) > 0,
        "issues": issues[:5],
        "values_checked": checked,
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
    from services.schema_drift import _normalize_dest_kind

    dest_kind = _normalize_dest_kind(destination_db_type)

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

    # Primary-key heuristic: prefer exact id/_id target (e.g. id -> _id for MongoDB),
    # then exact id/_id source, then first *_id source. Schemaless stores only enforce _id.
    pk = None
    if dest_kind in {"mongodb", "dynamodb", "redis"}:
        for m in mappings:
            if (m.get("target") or "").lower() == "_id":
                pk = m.get("source")
                break
        if not pk:
            pk = next((c for c in source_columns if c.lower() == "_id"), None)
    else:
        for m in mappings:
            if (m.get("target") or "").lower() in {"id", "_id"}:
                pk = m.get("source")
                break
        if not pk:
            for col in source_columns:
                if col.lower() in {"id", "_id"}:
                    pk = col
                    break
        if not pk:
            for col in source_columns:
                if col.lower().endswith("_id"):
                    pk = col
                    break

    checks: list[dict[str, Any]] = []

    if mappings:
        checks.append(_check_coercion_safety(mappings, source_types, target_types, dest_kind=dest_kind, schema_policy=schema_policy))
        checks.append(_check_transform_dry_run(mappings, source_columns, source_types, rows, dest_kind=dest_kind))
        checks.append(_check_financial_precision(mappings, source_types, rows))
        checks.append(_check_required_nulls(mappings, rows, null_rate_max=cfg["null_rate_max"], dest_kind=dest_kind))
        checks.append(_check_duplicate_keys(mappings, rows, validation_mode, dest_kind=dest_kind, primary_key=pk))
        checks.append(
            _check_mapping_confidence(mappings, confidence_min=cfg["confidence"], validation_mode=validation_mode)
        )

    if rows and source_columns:
        checks.append(_check_sample_quality(source_columns, rows, source_types, validation_mode, dest_kind=dest_kind))

    if rows:
        checks.append(_check_encoding_anomalies(rows))

    # Industry-standard expectation suite (dbt/GX patterns)
    if rows and source_columns:
        from services.expectations_engine import run_auto_expectations

        exp = run_auto_expectations(
            rows,
            source_columns,
            source_types,
            primary_key=pk,
            dest_kind=dest_kind,
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

    return {
        "passed": not blocks,
        "blocks_transfer": blocks,
        "validation_mode": validation_mode,
        "checks_run": len(checks),
        "checks_passed": len(passed_checks),
        "checks_failed": len(failed_checks),
        "checks": checks,
        "issues": all_issues[:30],
        "summary": (
            f"{len(passed_checks)}/{len(checks)} integrity checks passed"
            if checks
            else "No integrity checks run (missing mappings or samples)"
        ),
    }
