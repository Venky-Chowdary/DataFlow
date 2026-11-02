"""Row filters for CDC incremental snapshots.

Equivalent of Debezium's ``execute-snapshot`` ``additional-conditions``: an
operator re-snapshots only part of a table (``region = 'EU'``, ``updated_at >=
'2026-01-01'``) while the stream keeps running. Unlike Debezium we never accept a
raw SQL string — a signal is data an API caller controls, so the filter is the
same structured spec the batch row filter uses (``services/row_filter.py``) and
is compiled to a parameterised predicate per dialect. Values are always binds;
columns are validated identifiers quoted with the connector's quote character.

Semantics (documented for operators):

* The filter bounds only the *snapshot* read. Stream events for rows outside
  the filter are still applied — CDC never drops changes.
* A filtered snapshot does not delete destination rows that fall outside the
  filter; it is a targeted backfill, not a re-sync.
* SQL three-valued logic: NULL never satisfies ``ne`` / ``not_in`` / range ops;
  combine with ``{"column": c, "op": "is_null"}`` under ``or`` to include NULLs.
* ``regex`` is refused (no portable pushdown); the request fails at enqueue time.
"""

from __future__ import annotations

import json
import logging
from typing import Any

_logger = logging.getLogger(__name__)

MAX_DEPTH = 8
MAX_NODES = 64
MAX_IN_VALUES = 1000

_COMPARE = {"eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
_IN = {"in", "not_in"}
_NULL = {"is_null", "is_not_null"}
_LIKE = {"contains", "startswith", "endswith"}
SUPPORTED_OPS = frozenset(_COMPARE) | _IN | _NULL | _LIKE
_ALIASES = {
    "=": "eq", "==": "eq", "!=": "ne", "<>": "ne", ">": "gt", ">=": "gte",
    "<": "lt", "<=": "lte", "not in": "not_in", "notin": "not_in",
    "isnull": "is_null", "is null": "is_null", "notnull": "is_not_null",
    "is not null": "is_not_null", "starts_with": "startswith", "ends_with": "endswith",
}
_SQL_DIALECTS = {"postgresql", "mysql", "sqlserver", "oracle"}
_LIKE_ESCAPE = "!"


class SnapshotFilterError(ValueError):
    """Invalid incremental-snapshot filter (raised at enqueue, never at read)."""


def _scalar(value: Any, where: str) -> Any:
    if isinstance(value, bool) or isinstance(value, (int, float, str)):
        return value
    raise SnapshotFilterError(f"{where}: value must be a string, number or boolean, got {type(value).__name__}")


def _normalize_node(node: Any, depth: int, count: list[int]) -> dict[str, Any]:
    if not isinstance(node, dict):
        raise SnapshotFilterError("filter node must be an object")
    if depth > MAX_DEPTH:
        raise SnapshotFilterError(f"filter nesting deeper than {MAX_DEPTH}")
    count[0] += 1
    if count[0] > MAX_NODES:
        raise SnapshotFilterError(f"filter has more than {MAX_NODES} conditions")
    for key in ("and", "or"):
        if key in node:
            children = node.get(key)
            if not isinstance(children, list) or not children:
                raise SnapshotFilterError(f"'{key}' needs a non-empty list of conditions")
            return {key: [_normalize_node(c, depth + 1, count) for c in children]}
    from connectors.sql_identifiers import require_safe_identifier

    column = str(node.get("column") or node.get("field") or "").strip()
    if not column:
        raise SnapshotFilterError("filter condition needs 'column'")
    try:
        column = require_safe_identifier(column, allow_raw=True, max_len=128)
    except ValueError as exc:
        raise SnapshotFilterError(f"unsafe filter column {column!r}: {exc}") from exc
    raw_op = str(node.get("op") or node.get("operator") or "eq").strip().lower()
    op = _ALIASES.get(raw_op, raw_op)
    if op == "regex":
        raise SnapshotFilterError("'regex' cannot be pushed down to the source; use startswith/contains or a range")
    if op not in SUPPORTED_OPS:
        raise SnapshotFilterError(f"unsupported filter operator {raw_op!r}; supported: {sorted(SUPPORTED_OPS)}")
    where = f"condition on {column!r}"
    if op in _NULL:
        return {"column": column, "op": op}
    if "value" not in node or node.get("value") is None:
        raise SnapshotFilterError(f"{where}: '{op}' needs a non-null value (use is_null for NULL)")
    value = node["value"]
    if op in _IN:
        if not isinstance(value, list) or not value:
            raise SnapshotFilterError(f"{where}: '{op}' needs a non-empty list")
        if len(value) > MAX_IN_VALUES:
            raise SnapshotFilterError(f"{where}: '{op}' list longer than {MAX_IN_VALUES}")
        return {"column": column, "op": op, "value": [_scalar(v, where) for v in value]}
    value = _scalar(value, where)
    if op in _LIKE and not isinstance(value, str):
        raise SnapshotFilterError(f"{where}: '{op}' needs a string value")
    return {"column": column, "op": op, "value": value}


def normalize_snapshot_filter(spec: Any) -> dict[str, Any] | None:
    """Validate and canonicalise a filter spec; ``None`` when there is no filter."""
    if spec is None or spec == "" or spec == {}:
        return None
    if isinstance(spec, str):
        try:
            spec = json.loads(spec)
        except json.JSONDecodeError as exc:
            raise SnapshotFilterError(f"filter is not valid JSON: {exc.msg}") from exc
    if isinstance(spec, list):
        spec = {"and": spec}
    return _normalize_node(spec, 1, [0])


def snapshot_filter_columns(spec: dict[str, Any] | None) -> list[str]:
    if not spec:
        return []
    for key in ("and", "or"):
        if key in spec:
            out: list[str] = []
            for child in spec[key]:
                out.extend(c for c in snapshot_filter_columns(child) if c not in out)
            return out
    return [spec["column"]]


def _like_pattern(op: str, value: str, dialect: str) -> str:
    specials = [_LIKE_ESCAPE, "%", "_"] + (["["] if dialect == "sqlserver" else [])
    escaped = "".join(_LIKE_ESCAPE + ch if ch in specials else ch for ch in value)
    if op == "startswith":
        return escaped + "%"
    if op == "endswith":
        return "%" + escaped
    return "%" + escaped + "%"


def compile_snapshot_filter_sql(
    spec: dict[str, Any] | None,
    *,
    dialect: str,
    quote_char: str = '"',
    upper_columns: bool = False,
    placeholder: str = "%s",
) -> tuple[str, list[Any]]:
    """``(predicate_sql, params)`` for a normalised spec; ``("", [])`` when none."""
    if not spec:
        return "", []
    dialect = (dialect or "").strip().lower()
    if dialect not in _SQL_DIALECTS:
        raise SnapshotFilterError(f"snapshot filter pushdown not supported for dialect {dialect!r}")
    from connectors.sql_identifiers import quote_sql_identifier

    params: list[Any] = []

    def _walk(node: dict[str, Any]) -> str:
        for key, joiner in (("and", " AND "), ("or", " OR ")):
            if key in node:
                return "(" + joiner.join(_walk(c) for c in node[key]) + ")"
        name = node["column"].upper() if upper_columns else node["column"]
        col = quote_sql_identifier(name, quote_char)
        op = node["op"]
        if op == "is_null":
            return f"{col} IS NULL"
        if op == "is_not_null":
            return f"{col} IS NOT NULL"
        if op in _IN:
            values = node["value"]
            params.extend(values)
            marks = ", ".join([placeholder] * len(values))
            return f"{col} {'NOT IN' if op == 'not_in' else 'IN'} ({marks})"
        if op in _LIKE:
            params.append(_like_pattern(op, node["value"], dialect))
            return f"{col} LIKE {placeholder} ESCAPE '{_LIKE_ESCAPE}'"
        params.append(node["value"])
        return f"{col} {_COMPARE[op]} {placeholder}"

    sql = _walk(spec)
    return sql, params


def compile_snapshot_filter_mongo(spec: dict[str, Any] | None) -> dict[str, Any]:
    """MongoDB ``find`` filter for a normalised spec (``{}`` when none)."""
    if not spec:
        return {}
    import re

    for key in ("and", "or"):
        if key in spec:
            return {f"${key}": [compile_snapshot_filter_mongo(c) for c in spec[key]]}
    col, op = spec["column"], spec["op"]
    if op == "is_null":
        return {col: None}
    if op == "is_not_null":
        return {col: {"$ne": None}}
    if op == "eq":
        return {col: spec["value"]}
    if op in _IN:
        return {col: {"$in" if op == "in" else "$nin": list(spec["value"])}}
    if op in _LIKE:
        body = re.escape(spec["value"])
        pattern = {"startswith": f"^{body}", "endswith": f"{body}$"}.get(op, body)
        return {col: {"$regex": pattern}}
    mongo_op = {"ne": "$ne", "gt": "$gt", "gte": "$gte", "lt": "$lt", "lte": "$lte"}[op]
    return {col: {mongo_op: spec["value"]}}


def describe_snapshot_filter(spec: dict[str, Any] | None) -> str:
    """Human-readable form for logs, job metadata and the operator UI."""
    if not spec:
        return ""
    for key in ("and", "or"):
        if key in spec:
            return "(" + f" {key.upper()} ".join(describe_snapshot_filter(c) for c in spec[key]) + ")"
    op = spec["op"]
    if op in _NULL:
        return f"{spec['column']} {op.replace('_', ' ').upper()}"
    return f"{spec['column']} {_COMPARE.get(op, op.upper())} {json.dumps(spec['value'], default=str)}"


def signal_filter_sql(
    sig: Any,
    *,
    dialect: str,
    quote_char: str = '"',
    upper_columns: bool = False,
) -> tuple[str, list[Any]]:
    """Compile ``sig.row_filter`` for a chunk read (re-validated: stores are editable)."""
    spec = normalize_snapshot_filter(getattr(sig, "row_filter", None))
    if spec:
        _logger.debug(
            "CDC incremental snapshot %s filtered chunk: %s",
            getattr(sig, "id", "?"),
            describe_snapshot_filter(spec),
        )
    return compile_snapshot_filter_sql(
        spec, dialect=dialect, quote_char=quote_char, upper_columns=upper_columns
    )
