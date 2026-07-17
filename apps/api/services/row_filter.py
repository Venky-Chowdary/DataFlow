"""Row-level filtering for source records before mapping and transfer.

Supports atomic predicates and nested `and`/`or` composition.  Values are
compared in a type-coercion-aware way: if both the row value and the filter
value can be parsed as numbers, numeric comparison is used; otherwise the
comparison falls back to string equality/case-sensitive containment.
"""

from __future__ import annotations

import re
from typing import Any

_FILTER_OPS = frozenset({
    "eq", "ne", "gt", "gte", "lt", "lte",
    "in", "not_in", "contains", "startswith", "endswith", "regex",
    "is_null", "is_not_null",
})


def _to_scalar(value: Any) -> Any:
    """Unwrap single-element lists that APIs sometimes send as scalars."""
    if isinstance(value, list) and len(value) == 1:
        return value[0]
    return value


def _is_null(value: Any) -> bool:
    return value is None or value == "" or (isinstance(value, float) and value != value)


def _compare_values(row_value: Any, filter_value: Any, op: str) -> bool:
    rv = _to_scalar(row_value)
    fv = _to_scalar(filter_value)

    if op == "is_null":
        return _is_null(rv)
    if op == "is_not_null":
        return not _is_null(rv)

    if op in {"in", "not_in"}:
        allowed = fv if isinstance(fv, (list, tuple, set)) else [fv]
        allowed_str = {str(v) for v in allowed}
        return (str(rv) in allowed_str) if op == "in" else (str(rv) not in allowed_str)

    rv_str = str(rv) if rv is not None else ""
    fv_str = str(fv) if fv is not None else ""

    if op == "contains":
        return fv_str in rv_str
    if op == "startswith":
        return rv_str.startswith(fv_str)
    if op == "endswith":
        return rv_str.endswith(fv_str)
    if op == "regex":
        try:
            return bool(re.search(fv_str, rv_str))
        except re.error:
            return False

    # Numeric comparison when both sides parse cleanly as floats.
    try:
        rv_num = float(rv_str.replace(",", ""))
        fv_num = float(fv_str.replace(",", ""))
        if op == "eq":
            return rv_num == fv_num
        if op == "ne":
            return rv_num != fv_num
        if op == "gt":
            return rv_num > fv_num
        if op == "gte":
            return rv_num >= fv_num
        if op == "lt":
            return rv_num < fv_num
        if op == "lte":
            return rv_num <= fv_num
    except (ValueError, TypeError):
        pass

    if op == "eq":
        return rv_str == fv_str
    if op == "ne":
        return rv_str != fv_str
    if op == "gt":
        return rv_str > fv_str
    if op == "gte":
        return rv_str >= fv_str
    if op == "lt":
        return rv_str < fv_str
    if op == "lte":
        return rv_str <= fv_str

    return False


def _matches(record: dict[str, Any], spec: dict[str, Any]) -> bool:
    if not spec:
        return True

    if "and" in spec:
        return all(_matches(record, child) for child in spec["and"] if child)
    if "or" in spec:
        return any(_matches(record, child) for child in spec["or"] if child)

    column = spec.get("column") or spec.get("field") or ""
    op = (spec.get("operator") or spec.get("op") or "eq").lower()
    if op not in _FILTER_OPS:
        raise ValueError(f"Unsupported filter operator: {op}")
    value = record.get(column)
    return _compare_values(value, spec.get("value"), op)


def apply_row_filter(records: list[dict[str, Any]], spec: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return only records that satisfy ``spec``.

    An empty or ``None`` spec is a no-op (returns the input unchanged).
    """
    if not spec or not records:
        return records
    return [r for r in records if _matches(r, spec)]


def apply_row_filter_to_matrix(
    headers: list[str],
    rows: list[list[Any]],
    spec: dict[str, Any] | None,
) -> list[list[Any]]:
    """Filter a row matrix (list of lists/tuples) using a filter spec.

    The first row is used to build a column-name mapping; each row is converted
    to a dict, filtered, then rebuilt in the original order.
    """
    if not spec or not rows:
        return rows
    records = [dict(zip(headers, row)) for row in rows]
    filtered = apply_row_filter(records, spec)
    return [[r.get(h) for h in headers] for r in filtered]


class RowFilter:
    """Callable wrapper around a filter spec for composability."""

    def __init__(self, spec: dict[str, Any] | None = None):
        self.spec = spec or {}

    def __call__(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return apply_row_filter(records, self.spec)

    def apply(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return apply_row_filter(records, self.spec)

    def apply_matrix(self, headers: list[str], rows: list[list[Any]]) -> list[list[Any]]:
        return apply_row_filter_to_matrix(headers, rows, self.spec)
