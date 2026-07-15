"""Data-quality and anomaly-detection audits for preflight and per-batch checks.

Designed for production-grade data movement: detect duplicate keys, required
nulls, financial precision loss, statistical outliers, future/invalid dates,
null-rate spikes, and low-cardinality anomalies before a transfer commits.
"""

from __future__ import annotations

import re
import statistics
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


@dataclass
class DataQualityReport:
    passed: bool = True
    checks_failed: int = 0
    checks_passed: int = 0
    checks_warned: int = 0
    issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    summary: str = ""
    stats: dict[str, Any] = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _to_decimal(value: Any) -> Decimal | None:
    if _is_null(value):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _to_float(value: Any) -> float | None:
    if _is_null(value):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _parse_iso_date(value: Any) -> datetime | None:
    if _is_null(value):
        return None
    text = str(value).strip()
    # Normalize ISO 8601 "Z" suffix to +0000 offset for strptime
    if text.endswith("Z"):
        text = text[:-1] + "+0000"
    formats = ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d")
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _is_required_target(target: str) -> bool:
    lowered = target.lower()
    return lowered in {
        "_id",
        "id",
        "pk",
        "key",
        "uuid",
        "email",
        "phone",
        "ssn",
        "customer_id",
        "account_id",
        "order_id",
    } or lowered.endswith("_id") or lowered.endswith("key")


def _is_amount_column(name: str) -> bool:
    lowered = name.lower()
    return any(
        keyword in lowered
        for keyword in ("amount", "price", "cost", "salary", "wage", "revenue", "fee", "tax", "balance")
    )


def _is_date_column(name: str) -> bool:
    lowered = name.lower()
    return any(
        keyword in lowered
        for keyword in ("date", "time", "timestamp", "created", "updated", "at")
    )


def _iqr_outliers(values: list[float]) -> list[float]:
    if len(values) < 4:
        return []
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    q1 = sorted_vals[n // 4] if n >= 4 else sorted_vals[0]
    q3 = sorted_vals[(3 * n) // 4] if n >= 4 else sorted_vals[-1]
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return [v for v in values if v < lower or v > upper]


# ─────────────────────────────────────────────────────────────────────────────
# Public audit API
# ─────────────────────────────────────────────────────────────────────────────


def run_integrity_audit(
    headers: list[str],
    rows: list[list[str]],
    column_types: dict[str, str] | None = None,
    mappings: list[dict[str, Any]] | None = None,
    required_targets: list[str] | None = None,
    primary_key: str | None = None,
    validation_mode: str = "strict",
) -> DataQualityReport:
    """Run a sample-based integrity and anomaly audit over raw source rows.

    Hard checks always block: duplicate primary keys, required-null values, and
    financial precision loss.  Soft checks (null spikes, outliers, future dates,
    low cardinality, encoding anomalies) produce warnings.  Warnings become
    blockers only in ``maximum`` validation mode; in ``strict`` / ``balanced``
    they are surfaced but do not stop the transfer.
    """
    report = DataQualityReport()
    if not rows or not headers:
        report.passed = False
        report.issues.append("No sample rows available for integrity audit")
        report.summary = "Audit skipped — empty sample"
        return report

    column_types = column_types or {}
    required_targets = {t.lower() for t in (required_targets or [])}
    mappings = mappings or []
    source_to_target = {m.get("source"): m.get("target") for m in mappings if m.get("source")}
    target_to_source = {m.get("target"): m.get("source") for m in mappings if m.get("target")}

    total = len(rows)
    header_index = {h: i for i, h in enumerate(headers)}
    stats: dict[str, Any] = {"total_rows": total, "columns": {}}

    # ── Identify primary-key source column ───────────────────────────────────
    pk_source = primary_key
    if not pk_source:
        for tgt in ("_id", "id", "ID"):
            if target_to_source.get(tgt):
                pk_source = target_to_source[tgt]
                break
        if not pk_source:
            for h in headers:
                if h.lower() in {"_id", "id"}:
                    pk_source = h
                    break
        if not pk_source:
            pk_source = headers[0]

    pk_idx = header_index.get(pk_source, 0)
    pk_values = [row[pk_idx] if pk_idx < len(row) else "" for row in rows]

    def _hard(msg: str) -> None:
        report.issues.append(msg)
        report.checks_failed += 1

    def _warn(msg: str) -> None:
        report.warnings.append(msg)
        report.checks_warned += 1

    # 1. Duplicate primary keys (hard)
    dup_counts = Counter(pk_values)
    duplicates = {v: c for v, c in dup_counts.items() if c > 1 and str(v).strip()}
    if duplicates:
        _hard(
            f"Duplicate primary key values in '{pk_source}': "
            f"{len(duplicates)} keys repeat (e.g. {', '.join(str(v) for v in list(duplicates)[:3])})"
        )
    else:
        report.checks_passed += 1

    # 2. Per-column checks
    null_spike_threshold = 0.90
    low_cardinality_threshold = 0.01

    for h in headers:
        idx = header_index[h]
        col_type = (column_types.get(h) or "string").upper()
        values = [row[idx] if idx < len(row) else "" for row in rows]
        non_null = [v for v in values if not _is_null(v)]
        null_rate = (total - len(non_null)) / total if total else 0.0
        unique = set(non_null)
        cardinality = len(unique)
        distinct_ratio = cardinality / len(non_null) if non_null else 0.0

        col_stats = {
            "null_rate": null_rate,
            "cardinality": cardinality,
            "distinct_ratio": distinct_ratio,
        }

        target = source_to_target.get(h, h)
        is_required = target.lower() in required_targets or _is_required_target(target)

        # Required nulls (hard)
        if is_required and null_rate > 0.0:
            _hard(
                f"Required column '{h}' (target '{target}') has {int(null_rate * total)} null/empty values"
            )
        else:
            report.checks_passed += 1

        # Null-rate spike (soft)
        if null_rate >= null_spike_threshold:
            _warn(f"Column '{h}' is {null_rate:.0%} null — likely missing data")
        else:
            report.checks_passed += 1

        # Numeric outlier / precision checks
        if col_type in {"INTEGER", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL"} or (
            col_type == "STRING" and _is_amount_column(h)
        ):
            nums = [_to_float(v) for v in non_null]
            nums = [n for n in nums if n is not None]
            if nums:
                col_stats["min"] = min(nums)
                col_stats["max"] = max(nums)
                col_stats["mean"] = statistics.mean(nums)
                if len(nums) > 1:
                    try:
                        col_stats["stdev"] = statistics.stdev(nums)
                    except statistics.StatisticsError:
                        col_stats["stdev"] = 0.0

                # Financial precision loss: integer target with fractional source (hard)
                if col_type == "INTEGER" and _is_amount_column(h):
                    fractional = any(
                        ("." in str(v) or "," in str(v)) and _to_decimal(v) is not None
                        for v in non_null
                    )
                    if fractional:
                        _hard(
                            f"Financial column '{h}' has fractional values but target type is INTEGER — precision loss risk"
                        )
                    else:
                        report.checks_passed += 1

                # Statistical outliers (IQR) — soft warning
                try:
                    outliers = _iqr_outliers(nums)
                except Exception:
                    outliers = []
                if outliers and len(outliers) <= max(1, len(nums) * 0.05):
                    _warn(
                        f"Column '{h}' has {len(outliers)} outlier value(s) outside 1.5*IQR "
                        f"(max {max(outliers):.2f}, min {min(outliers):.2f})"
                    )
                else:
                    report.checks_passed += 1

        # Date validity / future dates (soft)
        if col_type in {"DATE", "TIMESTAMP", "DATETIME"} or _is_date_column(h):
            dates = [_parse_iso_date(v) for v in non_null]
            valid_dates = [d for d in dates if d is not None]
            if valid_dates:
                now_naive = datetime.utcnow()
                future = [
                    d for d in valid_dates
                    if (d.replace(tzinfo=None) if d.tzinfo else d) > now_naive
                ]
                if future:
                    _warn(f"Date column '{h}' contains {len(future)} future timestamp(s)")
                else:
                    report.checks_passed += 1

        # Low cardinality / constant column warning (soft)
        if distinct_ratio < low_cardinality_threshold and cardinality > 1:
            _warn(
                f"Column '{h}' is nearly constant ({cardinality} distinct values over {len(non_null)} rows)"
            )
        else:
            report.checks_passed += 1

        stats["columns"][h] = col_stats

    # 3. Encoding / control-character anomalies (soft)
    bad_encoding = 0
    for row in rows:
        for v in row:
            text = str(v)
            if any(ord(ch) < 32 and ch not in {"\t", "\n", "\r"} for ch in text):
                bad_encoding += 1
                if bad_encoding > 5:
                    break
        if bad_encoding > 5:
            break
    if bad_encoding:
        _warn(
            f"Sample contains {bad_encoding} cell(s) with control characters / encoding anomalies"
        )
    else:
        report.checks_passed += 1

    report.stats = stats

    # In maximum mode warnings are treated as blockers.
    if validation_mode.lower() == "maximum" and report.warnings:
        report.issues.extend(report.warnings)
        report.checks_failed += report.checks_warned
        report.warnings = []
        report.checks_warned = 0

    report.passed = report.checks_failed == 0
    report.summary = (
        f"{report.checks_passed} check(s) passed, {report.checks_failed} blocker(s), "
        f"{report.checks_warned} warning(s)"
        if (report.issues or report.warnings)
        else f"All {report.checks_passed} integrity checks passed"
    )
    return report
