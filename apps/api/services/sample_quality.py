"""Sample quality analysis — statistical anomaly detection on source data."""

from __future__ import annotations

import re
from typing import Any

from decimal import Decimal

from services.db_type_utils import SCHEMALESS_DESTS
from services.transform_engine import _parse_boolean, _parse_date, _parse_datetime, decimal_wire_value
from services.value_serializer import (
    NULL_WIRE_SENTINELS,
    SQL_NULL_SENTINEL,
    cell_to_string,
    is_reader_null_cell,
)

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def _numeric_values(values: list[str]) -> list[Decimal]:
    out: list[Decimal] = []
    for raw in values:
        if not raw:
            continue
        parsed = decimal_wire_value(raw)
        if parsed is None:
            continue
        out.append(parsed)
    return out


def _iqr_outliers(values: list[Decimal]) -> tuple[Decimal, Decimal, int]:
    """Return (lower_fence, upper_fence, outlier_count) using 1.5×IQR rule.

    Fences use write-path Decimals. ``float(parsed)`` invented a second
    magnitude before the first quartile.
    """
    zero = Decimal("0")
    if len(values) < 4:
        return zero, zero, 0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    q1 = sorted_vals[n // 4]
    q3 = sorted_vals[(3 * n) // 4]
    iqr = q3 - q1
    if iqr <= 0:
        return sorted_vals[0], sorted_vals[-1], 0
    lower = q1 - Decimal("1.5") * iqr
    upper = q3 + Decimal("1.5") * iqr
    outliers = sum(1 for v in values if v < lower or v > upper)
    return lower, upper, outliers


def _sample_wire(value: Any) -> str:
    """One sample cell. Reader-wired SQL NULL is absence, not a token.

    A stored empty string is *not* SQL NULL — the blank-cell contract keeps
    ``''`` as a present value on text carriers. Flattening both to ``""``
    reported `1 '' + 1 NULL` as a 100% null rate (QA T19).
    """
    if is_reader_null_cell(value):
        return SQL_NULL_SENTINEL
    text = cell_to_string(value, preserve_sql_null=True)
    if text.strip() in NULL_WIRE_SENTINELS:
        return SQL_NULL_SENTINEL
    return text


def _column_values(rows: list[dict[str, Any]], column: str) -> list[str]:
    values: list[str] = []
    for row in rows:
        values.append(_sample_wire(row.get(column)))
    return values


def analyze_column_quality(
    column: str,
    values: list[str],
    *,
    inferred_type: str = "VARCHAR",
    dest_kind: str = "",
) -> dict[str, Any]:
    """Profile one column for anomalies affecting transfer quality."""
    schemaless = (dest_kind or "").lower() in SCHEMALESS_DESTS
    type_upper = (inferred_type or "VARCHAR").upper()
    # SQL NULL and stored '' are different evidence (QA T19). On text
    # carriers '' is a stored value; on numeric/temporal carriers a blank
    # wire cell is written as NULL — count it as absent there.
    textual = type_upper in {
        "VARCHAR", "TEXT", "CHAR", "NCHAR", "NVARCHAR", "STRING",
        "CLOB", "ENUM", "UUID", "JSON", "JSONB",
    }
    null_cells = sum(1 for v in values if v == SQL_NULL_SENTINEL)
    empty_cells = sum(1 for v in values if v == "")
    absent = null_cells + (0 if textual else empty_cells)
    warning_absent = null_cells + empty_cells
    non_empty = [
        v for v in values if v not in {SQL_NULL_SENTINEL, ""}
    ]
    null_rate = absent / max(len(values), 1)
    issues: list[str] = []
    severity = "none"
    if type_upper in {"INTEGER", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "NUMBER"}:
        nums = _numeric_values(non_empty)
        parse_fail = len(non_empty) - len(nums)
        if parse_fail > 0 and parse_fail / max(len(non_empty), 1) > 0.05:
            issues.append(f"{parse_fail} non-numeric value(s) in numeric column")
            severity = "warning"
        _, _, outlier_count = _iqr_outliers(nums)
        if outlier_count > 0 and len(nums) >= 8:
            rate = outlier_count / len(nums)
            if rate > 0.02:
                issues.append(f"{outlier_count} statistical outlier(s) ({rate:.0%})")
                severity = "warning" if severity != "block" else severity

    elif type_upper in {"DATE", "TIMESTAMP", "DATETIME"}:
        bad = 0
        for v in non_empty[:200]:
            if type_upper == "DATE" and not _parse_date(v):
                bad += 1
            elif not _parse_datetime(v):
                bad += 1
        if bad > 0 and bad / max(len(non_empty), 1) > 0.05:
            issues.append(f"{bad} unparseable date/time value(s)")
            severity = "warning"

    elif type_upper == "BOOLEAN":
        bad = sum(1 for v in non_empty if _parse_boolean(v) is None)
        if bad > 0 and bad / max(len(non_empty), 1) > 0.05:
            issues.append(f"{bad} non-boolean value(s)")
            severity = "warning"

    if re.search(r"email", column, re.I):
        invalid = sum(1 for v in non_empty[:100] if v and not EMAIL_RE.match(v))
        if invalid > 0 and invalid / max(len(non_empty), 1) > 0.1:
            issues.append(f"{invalid} invalid email format(s)")
            severity = "warning"

    if textual and empty_cells:
        # '' is a stored value on text — call it out as data, not as null.
        issues.append(f"{empty_cells} empty string value(s) stored (not NULL)")

    warning_null_rate = warning_absent / max(len(values), 1)
    if (
        not schemaless
        and warning_null_rate > 0.5
        and not re.search(r"optional|note|comment|description", column, re.I)
    ):
        issues.append(f"High null rate ({warning_null_rate:.0%})")
        # Sparse source columns are normal in NoSQL and should not block transfer;
        # the target DDL and required-null checks already cover key/NOT-NULL columns.
        if severity == "none":
            severity = "warning"

    distinct = len(set(non_empty))
    if len(non_empty) >= 20 and distinct == 1:
        issues.append("Constant value across all sampled rows")
        severity = "warning" if severity != "block" else severity

    return {
        "column": column,
        "inferred_type": type_upper,
        "null_rate": round(null_rate, 3),
        "sample_size": len(values),
        "distinct_count": distinct,
        "issues": issues,
        "severity": severity if issues else "none",
    }


def analyze_dataset_quality(
    columns: list[str],
    rows: list[dict[str, Any]],
    *,
    schema: dict[str, str] | None = None,
    sample_limit: int = 500,
    dest_kind: str = "",
) -> dict[str, Any]:
    """Analyze all columns; return issues suitable for mapping/preflight gates."""
    schema = schema or {}
    sample = rows[:sample_limit]
    column_reports: list[dict[str, Any]] = []
    all_issues: list[str] = []
    blocking = False

    def _hash(value: Any) -> str:
        return _sample_wire(value)

    key_signature_counts: dict[tuple[Any, ...], int] = {}
    for row in sample:
        signature = tuple(_hash(row.get(col, "")) for col in columns)
        key_signature_counts[signature] = key_signature_counts.get(signature, 0) + 1
    duplicate_row_count = sum(count - 1 for count in key_signature_counts.values() if count > 1)
    if duplicate_row_count > 0:
        all_issues.append(f"Duplicate rows detected: {duplicate_row_count} replicated sample record(s)")
        # Duplicate rows are a data-quality observation, not a transfer blocker.

    for col in columns:
        values = _column_values(sample, col)
        report = analyze_column_quality(col, values, inferred_type=schema.get(col, "VARCHAR"), dest_kind=dest_kind)
        column_reports.append(report)
        for issue in report.get("issues", []):
            msg = f"{col}: {issue}"
            all_issues.append(msg)
            if report.get("severity") == "block":
                blocking = True

    warn_count = sum(1 for r in column_reports if r.get("severity") == "warning")
    block_count = sum(1 for r in column_reports if r.get("severity") == "block")

    return {
        "columns": column_reports,
        "issues": all_issues,
        "issue_count": len(all_issues),
        "warning_columns": warn_count,
        "blocking_columns": block_count,
        "duplicate_row_count": duplicate_row_count,
        "blocks_transfer": blocking,
        "quality_score": round(
            max(0.0, 100.0 - warn_count * 5 - block_count * 20 - duplicate_row_count * 15),
            1,
        ),
    }
