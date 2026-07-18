"""
Data quality expectations engine — industry-standard validation contracts.

Implements patterns from dbt tests and Great Expectations:
  unique, not_null, accepted_values, between, relationships,
  row_count_bounds, regex_match, distribution_drift, column_pair_equality.

Each expectation returns failing rows + pass/fail — same contract as dbt/GX.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from services.db_type_utils import SCHEMALESS_DESTS

ExpectationFn = Callable[..., dict[str, Any]]


@dataclass
class ExpectationResult:
    expectation: str
    column: str
    passed: bool
    failing_count: int
    failing_samples: list[dict[str, Any]] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)
    severity: str = "block"  # block | warn

    def to_dict(self) -> dict[str, Any]:
        return {
            "expectation": self.expectation,
            "column": self.column,
            "passed": self.passed,
            "failing_count": self.failing_count,
            "failing_samples": self.failing_samples[:10],
            "details": self.details,
            "severity": self.severity,
        }


def _col_values(rows: list[dict[str, Any]], column: str) -> list[Any]:
    return [row.get(column) for row in rows]


def _non_empty(values: list[Any]) -> list[str]:
    return [str(v).strip() for v in values if v is not None and str(v).strip() != ""]


def expect_column_unique(
    rows: list[dict[str, Any]],
    column: str,
    *,
    severity: str = "block",
) -> ExpectationResult:
    """dbt: unique — no duplicate values in column."""
    seen: dict[str, int] = {}
    failures: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        val = row.get(column)
        key = "" if val is None else str(val).strip()
        if not key:
            continue
        seen[key] = seen.get(key, 0) + 1
        if seen[key] == 2:
            failures.append({"row_index": i, "value": key, "duplicate_of": key})
    dup_count = sum(1 for c in seen.values() if c > 1)
    return ExpectationResult(
        expectation="expect_column_unique",
        column=column,
        passed=dup_count == 0,
        failing_count=dup_count,
        failing_samples=failures[:10],
        details={"distinct": len(seen), "total_non_empty": sum(seen.values())},
        severity=severity,
    )


def expect_column_not_null(
    rows: list[dict[str, Any]],
    column: str,
    *,
    max_null_rate: float = 0.0,
    severity: str = "block",
) -> ExpectationResult:
    """dbt: not_null — column must not be empty."""
    null_count = 0
    failures: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        val = row.get(column)
        if val is None or str(val).strip() == "":
            null_count += 1
            if len(failures) < 10:
                failures.append({"row_index": i, "value": val})
    rate = null_count / max(len(rows), 1)
    return ExpectationResult(
        expectation="expect_column_not_null",
        column=column,
        passed=rate <= max_null_rate,
        failing_count=null_count,
        failing_samples=failures,
        details={"null_rate": round(rate, 4), "max_null_rate": max_null_rate},
        severity=severity,
    )


def expect_column_accepted_values(
    rows: list[dict[str, Any]],
    column: str,
    accepted: set[str] | list[str],
    *,
    mostly: float = 1.0,
    severity: str = "block",
) -> ExpectationResult:
    """dbt: accepted_values — values must be in allowed set."""
    allowed = {str(v).strip().lower() for v in accepted}
    failures: list[dict[str, Any]] = []
    checked = 0
    bad = 0
    for i, row in enumerate(rows):
        val = row.get(column)
        if val is None or str(val).strip() == "":
            continue
        checked += 1
        if str(val).strip().lower() not in allowed:
            bad += 1
            if len(failures) < 10:
                failures.append({"row_index": i, "value": str(val)[:80]})
    rate = 1.0 - (bad / max(checked, 1))
    return ExpectationResult(
        expectation="expect_column_accepted_values",
        column=column,
        passed=rate >= mostly,
        failing_count=bad,
        failing_samples=failures,
        details={"accepted": sorted(allowed)[:20], "match_rate": round(rate, 4)},
        severity=severity,
    )


def expect_column_values_between(
    rows: list[dict[str, Any]],
    column: str,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
    mostly: float = 1.0,
    severity: str = "block",
) -> ExpectationResult:
    """GX: expect_column_values_to_be_between."""
    failures: list[dict[str, Any]] = []
    checked = 0
    bad = 0
    for i, row in enumerate(rows):
        raw = row.get(column)
        if raw is None or str(raw).strip() == "":
            continue
        try:
            num = float(Decimal(str(raw).replace(",", "").replace("$", "")))
        except (InvalidOperation, ValueError):
            bad += 1
            if len(failures) < 10:
                failures.append({"row_index": i, "value": str(raw)[:40], "reason": "not_numeric"})
            continue
        checked += 1
        if min_value is not None and num < min_value:
            bad += 1
            if len(failures) < 10:
                failures.append({"row_index": i, "value": num, "reason": f"below_min_{min_value}"})
        elif max_value is not None and num > max_value:
            bad += 1
            if len(failures) < 10:
                failures.append({"row_index": i, "value": num, "reason": f"above_max_{max_value}"})
    rate = 1.0 - (bad / max(checked, 1))
    return ExpectationResult(
        expectation="expect_column_values_between",
        column=column,
        passed=rate >= mostly,
        failing_count=bad,
        failing_samples=failures,
        details={"min": min_value, "max": max_value, "match_rate": round(rate, 4)},
        severity=severity,
    )


def expect_column_values_match_regex(
    rows: list[dict[str, Any]],
    column: str,
    pattern: str,
    *,
    mostly: float = 0.95,
    severity: str = "warn",
) -> ExpectationResult:
    """GX: expect_column_values_to_match_regex."""
    compiled = re.compile(pattern)
    failures: list[dict[str, Any]] = []
    checked = 0
    bad = 0
    for i, row in enumerate(rows):
        val = row.get(column)
        if val is None or str(val).strip() == "":
            continue
        checked += 1
        if not compiled.match(str(val).strip()):
            bad += 1
            if len(failures) < 10:
                failures.append({"row_index": i, "value": str(val)[:60]})
    rate = 1.0 - (bad / max(checked, 1))
    return ExpectationResult(
        expectation="expect_column_values_match_regex",
        column=column,
        passed=rate >= mostly,
        failing_count=bad,
        failing_samples=failures,
        details={"pattern": pattern, "match_rate": round(rate, 4)},
        severity=severity,
    )


def expect_table_row_count_between(
    rows: list[dict[str, Any]],
    *,
    min_count: int | None = None,
    max_count: int | None = None,
    severity: str = "block",
) -> ExpectationResult:
    """GX: expect_table_row_count_to_be_between."""
    count = len(rows)
    passed = True
    if min_count is not None and count < min_count:
        passed = False
    if max_count is not None and count > max_count:
        passed = False
    return ExpectationResult(
        expectation="expect_table_row_count_between",
        column="*",
        passed=passed,
        failing_count=0 if passed else 1,
        details={"row_count": count, "min": min_count, "max": max_count},
        severity=severity,
    )


def expect_column_pair_values_equal(
    rows: list[dict[str, Any]],
    column_a: str,
    column_b: str,
    *,
    mostly: float = 1.0,
    severity: str = "block",
) -> ExpectationResult:
    """Cross-column constraint: two columns must match (e.g. amount vs total)."""
    failures: list[dict[str, Any]] = []
    compared = 0
    bad = 0
    for i, row in enumerate(rows):
        a, b = row.get(column_a), row.get(column_b)
        if a is None and b is None:
            continue
        compared += 1
        if str(a).strip() != str(b).strip():
            bad += 1
            if len(failures) < 10:
                failures.append({"row_index": i, column_a: str(a)[:40], column_b: str(b)[:40]})
    rate = 1.0 - (bad / max(compared, 1))
    return ExpectationResult(
        expectation="expect_column_pair_values_equal",
        column=f"{column_a}={column_b}",
        passed=rate >= mostly,
        failing_count=bad,
        failing_samples=failures,
        details={"match_rate": round(rate, 4)},
        severity=severity,
    )


def _numeric_histogram(values: list[float], buckets: int = 10) -> list[float]:
    if not values:
        return [0.0] * buckets
    lo, hi = min(values), max(values)
    if lo == hi:
        hist = [0.0] * buckets
        hist[0] = 1.0
        return hist
    width = (hi - lo) / buckets
    counts = [0] * buckets
    for v in values:
        idx = min(buckets - 1, int((v - lo) / width))
        counts[idx] += 1
    total = sum(counts) or 1
    return [c / total for c in counts]


def _js_divergence(p: list[float], q: list[float]) -> float:
    """Jensen-Shannon divergence — distribution drift metric (0=identical, 1=max)."""
    eps = 1e-12
    m = [(pi + qi) / 2 for pi, qi in zip(p, q)]
    kl_pm = sum(pi * math.log(pi / mi) for pi, mi in zip(p, m) if pi > eps)
    kl_qm = sum(qi * math.log(qi / mi) for qi, mi in zip(q, m) if qi > eps)
    return math.sqrt(0.5 * (kl_pm + kl_qm))


def expect_column_distribution_drift(
    current_rows: list[dict[str, Any]],
    baseline_rows: list[dict[str, Any]],
    column: str,
    *,
    threshold: float = 0.25,
    severity: str = "warn",
) -> ExpectationResult:
    """
    GX Checkpoint C — compare current distribution to baseline.
    Uses histogram + Jensen-Shannon divergence for numeric columns,
    categorical frequency divergence for string columns.
    """
    cur_vals = _non_empty(_col_values(current_rows, column))
    base_vals = _non_empty(_col_values(baseline_rows, column))
    if not cur_vals or not base_vals:
        return ExpectationResult(
            expectation="expect_column_distribution_drift",
            column=column,
            passed=True,
            failing_count=0,
            details={"skipped": "insufficient_data"},
            severity=severity,
        )

    nums_cur: list[float] = []
    nums_base: list[float] = []
    for v in cur_vals:
        try:
            nums_cur.append(float(Decimal(v.replace(",", ""))))
        except (InvalidOperation, ValueError):
            pass
    for v in base_vals:
        try:
            nums_base.append(float(Decimal(v.replace(",", ""))))
        except (InvalidOperation, ValueError):
            pass

    if len(nums_cur) >= 8 and len(nums_base) >= 8:
        hist_cur = _numeric_histogram(nums_cur)
        hist_base = _numeric_histogram(nums_base)
        drift = _js_divergence(hist_cur, hist_base)
        return ExpectationResult(
            expectation="expect_column_distribution_drift",
            column=column,
            passed=drift <= threshold,
            failing_count=0 if drift <= threshold else 1,
            details={"drift_score": round(drift, 4), "threshold": threshold, "method": "js_histogram"},
            severity=severity,
        )

    # Categorical: top-value frequency divergence
    top_k = 15
    cur_freq = Counter(cur_vals).most_common(top_k)
    base_freq = Counter(base_vals).most_common(top_k)
    all_keys = {k for k, _ in cur_freq} | {k for k, _ in base_freq}
    cur_map = dict(cur_freq)
    base_map = dict(base_freq)
    cur_total = len(cur_vals)
    base_total = len(base_vals)
    max_delta = 0.0
    for k in all_keys:
        delta = abs(cur_map.get(k, 0) / cur_total - base_map.get(k, 0) / base_total)
        max_delta = max(max_delta, delta)
    return ExpectationResult(
        expectation="expect_column_distribution_drift",
        column=column,
        passed=max_delta <= threshold,
        failing_count=0 if max_delta <= threshold else 1,
        details={
            "drift_score": round(max_delta, 4),
            "max_freq_delta": round(max_delta, 4),
            "threshold": threshold,
            "method": "categorical",
        },
        severity=severity,
    )


def infer_expectations_for_schema(
    columns: list[str],
    schema: dict[str, str],
    *,
    primary_key: str | None = None,
    dest_kind: str = "",
    validation_mode: str = "strict",
) -> list[dict[str, Any]]:
    """Auto-generate standard expectations from schema metadata (dbt-style)."""
    specs: list[dict[str, Any]] = []
    schemaless = (dest_kind or "").lower() in SCHEMALESS_DESTS
    for col in columns:
        t = (schema.get(col) or "VARCHAR").upper()
        is_id = col.lower().endswith("_id") or col.lower() == "id"
        is_primary = col == primary_key
        if is_primary or is_id:
            # Schemaless destinations only enforce uniqueness/nullability on the
            # primary `_id`; other `*_id` fields are normal FKs and may repeat.
            if schemaless and not is_primary and col.lower() != "_id":
                continue
            severity = "block" if is_primary else "warn"
            specs.append({"fn": "expect_column_unique", "column": col, "severity": severity})
            specs.append({"fn": "expect_column_not_null", "column": col, "max_null_rate": 0.0, "severity": severity})
        if t in {"INTEGER", "DECIMAL", "NUMERIC", "FLOAT", "NUMBER"}:
            if re.search(r"amount|price|total|balance|amt|revenue", col, re.I):
                specs.append({
                    "fn": "expect_column_values_between",
                    "column": col,
                    "min_value": None,
                    "max_value": None,
                    "mostly": 0.98,
                })
        if re.search(r"email", col, re.I):
            specs.append({
                "fn": "expect_column_values_match_regex",
                "column": col,
                "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
                "mostly": 0.9,
                "severity": "warn",
            })
        if re.search(r"status|state|type|category", col, re.I):
            # Categorical columns in schemaless documents are often sparse.
            # Only treat a 5% null rate as a hard blocker in maximum validation mode;
            # otherwise surface it as a warning so the transfer is not held up.
            if not schemaless:
                cat_severity = "block" if (validation_mode or "").lower() == "maximum" else "warn"
                specs.append({"fn": "expect_column_not_null", "column": col, "max_null_rate": 0.05, "severity": cat_severity})
    return specs


_DISPATCH: dict[str, ExpectationFn] = {
    "expect_column_unique": expect_column_unique,
    "expect_column_not_null": expect_column_not_null,
    "expect_column_accepted_values": expect_column_accepted_values,
    "expect_column_values_between": expect_column_values_between,
    "expect_column_values_match_regex": expect_column_values_match_regex,
    "expect_table_row_count_between": expect_table_row_count_between,
    "expect_column_pair_values_equal": expect_column_pair_values_equal,
    "expect_column_distribution_drift": expect_column_distribution_drift,
}


def run_expectation_suite(
    rows: list[dict[str, Any]],
    expectations: list[dict[str, Any]],
    *,
    baseline_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Execute a full expectation suite — returns GX-style validation result."""
    results: list[ExpectationResult] = []
    for spec in expectations:
        fn_name = spec.get("fn", "")
        fn = _DISPATCH.get(fn_name)
        if not fn:
            continue
        kwargs = {k: v for k, v in spec.items() if k not in {"fn", "column"}}
        if fn_name == "expect_column_distribution_drift":
            kwargs["baseline_rows"] = baseline_rows or []
            result = fn(rows, spec["column"], **kwargs)
        elif fn_name == "expect_table_row_count_between":
            result = fn(rows, **kwargs)
        elif fn_name == "expect_column_pair_values_equal":
            result = fn(rows, spec["column_a"], spec["column_b"], **kwargs)
        else:
            result = fn(rows, spec["column"], **kwargs)
        results.append(result)

    blocking = [r for r in results if not r.passed and r.severity == "block"]
    warnings = [r for r in results if not r.passed and r.severity == "warn"]
    return {
        "passed": len(blocking) == 0,
        "blocks_transfer": len(blocking) > 0,
        "expectations_run": len(results),
        "expectations_passed": sum(1 for r in results if r.passed),
        "expectations_failed": sum(1 for r in results if not r.passed),
        "blocking_failures": [r.to_dict() for r in blocking],
        "warnings": [r.to_dict() for r in warnings],
        "results": [r.to_dict() for r in results],
    }


def run_auto_expectations(
    rows: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, str],
    *,
    primary_key: str | None = None,
    baseline_rows: list[dict[str, Any]] | None = None,
    dest_kind: str = "",
    validation_mode: str = "strict",
) -> dict[str, Any]:
    """Infer + run standard expectations for a dataset."""
    specs = infer_expectations_for_schema(
        columns,
        schema,
        primary_key=primary_key,
        dest_kind=dest_kind,
        validation_mode=validation_mode,
    )
    if baseline_rows:
        for col in columns:
            if (schema.get(col) or "").upper() in {"DECIMAL", "INTEGER", "VARCHAR"}:
                specs.append({
                    "fn": "expect_column_distribution_drift",
                    "column": col,
                    "threshold": 0.35,
                    "severity": "warn",
                })
    return run_expectation_suite(rows, specs, baseline_rows=baseline_rows)
