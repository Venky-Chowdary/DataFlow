"""Row-level filtering for source records before mapping and transfer.

Supports atomic predicates and nested `and`/`or` composition.  Values are
compared in a type-coercion-aware way: if both the row value and the filter
value can be parsed as numbers, numeric comparison is used; otherwise the
comparison falls back to string equality/case-sensitive containment.
"""

from __future__ import annotations

import json
import re
from typing import Any

_FILTER_OPS = frozenset({
    "eq", "ne", "gt", "gte", "lt", "lte",
    "in", "not_in", "contains", "startswith", "endswith", "regex",
    "is_null", "is_not_null",
})

#: Other spellings of the operators above. A symbol is only ever looked up
#: here and replaced by its word form; it never reaches a query (D328).
FILTER_OP_ALIASES = {
    "=": "eq", "==": "eq", "!=": "ne", "<>": "ne", ">": "gt", ">=": "gte",
    "<": "lt", "<=": "lte", "not in": "not_in", "notin": "not_in",
    "isnull": "is_null", "is null": "is_null", "notnull": "is_not_null",
    "is not null": "is_not_null", "starts_with": "startswith", "ends_with": "endswith",
}

#: The operator list an error and the MCP tool schema show.
FILTER_OPS_HELP = (
    "eq (=, ==), ne (!=, <>), gt (>), gte (>=), lt (<), lte (<=), in, not_in, "
    "contains, startswith, endswith, regex, is_null, is_not_null"
)


def canonical_filter_op(raw: Any) -> str:
    """The word-form operator ``raw`` names; ValueError listing the accepted set."""
    text = " ".join(str(raw if raw is not None else "eq").split()).lower() or "eq"
    op = FILTER_OP_ALIASES.get(text, text)
    if op not in _FILTER_OPS:
        raise ValueError(
            f"Unsupported filter operator: {raw}. Use one of: {FILTER_OPS_HELP}."
        )
    return op


#: Keys a predicate node may carry. Anything else is a shape this filter does
#: not understand, and a filter that is not understood is refused, never
#: skipped: the operator declared rows to keep out.
_LEAF_KEYS = frozenset({"column", "field", "op", "operator", "value"})
_JOINERS = ("and", "or")
_NULL_OPS = frozenset({"is_null", "is_not_null"})

FILTER_SHAPE_HELP = (
    'a predicate {"column": <name>, "op": <operator>, "value": <value>} or a '
    'composition {"and": [...]} / {"or": [...]} of predicates'
)


def filter_spec_from_json(text: str | None) -> dict[str, Any]:
    """A form field's filter: blank is no filter, anything else a JSON filter object.

    ValueError when the text is not JSON, or is JSON but not a filter this
    module can evaluate (a list, a string, ``{"sql": ...}``). Reading such a
    field as "no filter" landed every row under a declared filter and reported
    the run verified. The parsed object is returned as sent; it is valid.
    """
    if not str(text or "").strip():
        return {}
    try:
        parsed = json.loads(str(text))
    except ValueError as exc:
        raise ValueError(
            f"not valid JSON ({exc.msg}). Send {FILTER_SHAPE_HELP}."
        ) from exc
    canonical_filter_spec(parsed)
    return parsed


def canonical_filter_spec(spec: dict[str, Any] | None) -> dict[str, Any]:
    """``spec`` validated, with every operator in its word form.

    The one validator of a row filter. ValueError when any node is not a
    predicate this module can evaluate: no ``column``, an unknown operator, an
    unknown key (``{"sql": ...}``, ``{"where": ...}``, ``{"region": "EU"}``), an
    empty composition or an invalid regular expression. Such a node used to
    compare an absent column with a blank value and match every row, so the
    whole table landed under a filter that was declared and reported verified.
    """
    if spec is None:
        return {}
    if not isinstance(spec, dict):
        raise ValueError(f"A filter must be {FILTER_SHAPE_HELP}; got {type(spec).__name__}.")
    node = dict(spec)
    if not node:
        return node
    joiners = [j for j in _JOINERS if j in node]
    if joiners:
        joiner = joiners[0]
        extra = sorted(str(k) for k in node if k != joiner)
        if extra:
            raise ValueError(
                f"A filter {joiner!r} node carries nothing else; found {', '.join(extra)}. "
                f"Use {FILTER_SHAPE_HELP}."
            )
        children = node[joiner]
        if not isinstance(children, (list, tuple)) or not children:
            raise ValueError(f"A filter {joiner!r} node needs a non-empty list of predicates.")
        node[joiner] = [canonical_filter_spec(_child_node(child, joiner)) for child in children]
        return node
    unknown = sorted(str(k) for k in node if k not in _LEAF_KEYS)
    if unknown:
        raise ValueError(
            f"Unsupported filter key(s): {', '.join(unknown)}. Use {FILTER_SHAPE_HELP}."
        )
    column = node.get("column") or node.get("field")
    if not isinstance(column, str) or not column.strip():
        raise ValueError(
            f"A filter predicate needs a column name. Use {FILTER_SHAPE_HELP}."
        )
    key = "operator" if node.get("operator") else "op"
    node[key] = canonical_filter_op(node.get(key) or "eq")
    if node[key] == "regex":
        try:
            re.compile(_cell_text(_to_scalar(node.get("value"))))
        except re.error as exc:
            raise ValueError(
                f"Filter on {column!r} has an invalid regular expression: {exc}."
            ) from None
    return node


def _child_node(child: Any, joiner: str) -> dict[str, Any]:
    if not isinstance(child, dict) or not child:
        raise ValueError(
            f"Every entry of a filter {joiner!r} list must be a predicate; "
            f"got {child!r}."
        )
    return child


def _to_scalar(value: Any) -> Any:
    """Unwrap single-element lists that APIs sometimes send as scalars."""
    if isinstance(value, list) and len(value) == 1:
        return value[0]
    return value


def _is_null(value: Any) -> bool:
    """Absence for source filters. Reader-wired SQL NULL is not a token.

    ``None`` / ``""`` / NaN used to be the only blanks. After PostgreSQL /
    SQLite extract emits ``SQL_NULL_SENTINEL``, ``is_null`` missed it and
    ``is_not_null`` kept the sentinel as a present row.
    """
    from services.value_serializer import is_null_evidence

    if is_null_evidence(value):
        return True
    return isinstance(value, float) and value != value


def _cell_text(value: Any) -> str:
    """One filter token. Reader-null / blank become ``""`` (same as None).

    ``str(value)`` invented ``True`` vs dest ``"true"``, scientific
    ``1E+2`` vs dest-canonical ``100``, and ``str(None) == "None"`` so
    ``in ["None"]`` matched a SQL NULL.
    """
    from services.value_serializer import present_cell_text

    return present_cell_text(value) or ""


def _compare_values(row_value: Any, filter_value: Any, op: str) -> bool:
    rv = _to_scalar(row_value)
    fv = _to_scalar(filter_value)

    if op == "is_null":
        return _is_null(rv)
    if op == "is_not_null":
        return not _is_null(rv)

    if op in {"in", "not_in"}:
        allowed = fv if isinstance(fv, (list, tuple, set)) else [fv]
        allowed_str = {_cell_text(v) for v in allowed}
        return (_cell_text(rv) in allowed_str) if op == "in" else (
            _cell_text(rv) not in allowed_str
        )

    rv_str = _cell_text(rv)
    fv_str = _cell_text(fv)

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

    # Numeric comparison when both sides parse on the write-path locale contract.
    try:
        from services.transform_engine import decimal_wire_value

        rv_parsed = decimal_wire_value(rv_str)
        fv_parsed = decimal_wire_value(fv_str)
        if rv_parsed is None or fv_parsed is None:
            raise ValueError("not a locale-safe number")
        if op == "eq":
            return rv_parsed == fv_parsed
        if op == "ne":
            return rv_parsed != fv_parsed
        if op == "gt":
            return rv_parsed > fv_parsed
        if op == "gte":
            return rv_parsed >= fv_parsed
        if op == "lt":
            return rv_parsed < fv_parsed
        if op == "lte":
            return rv_parsed <= fv_parsed
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

    column = spec.get("column") or spec.get("field")
    if not column:
        # Never match-all: a predicate that names no column cannot be judged
        # (canonical_filter_spec refuses it before a run starts).
        raise ValueError(
            f"A filter predicate needs a column name. Use {FILTER_SHAPE_HELP}."
        )
    op = canonical_filter_op(spec.get("operator") or spec.get("op") or "eq")
    value = record.get(column)
    return _compare_values(value, spec.get("value"), op)


def filter_columns(spec: dict[str, Any] | None) -> list[str]:
    """Every column a filter spec reads, so a projected walk still has them.

    A walk that only selected the bounded carriers dropped ``status`` and then
    judged rows the write had already filtered out.
    """
    out: list[str] = []
    node = spec or {}
    for key in ("and", "or"):
        if key in node:
            for child in node.get(key) or []:
                out.extend(filter_columns(child))
            return out
    col = str(node.get("column") or node.get("field") or "").strip()
    if col:
        out.append(col)
    return out


def iter_filtered_rows(rows, spec: dict[str, Any] | None):
    """Yield rows that satisfy ``spec``. Pass-through when there is no filter.

    Streaming counterpart of :func:`apply_row_filter` so a table walk can
    judge the same subset Execute writes without materialising the table.
    """
    if rows is None:
        return None
    if not spec:
        return rows

    def _gen():
        for rec in rows:
            if isinstance(rec, dict) and _matches(rec, spec):
                yield rec

    return _gen()


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
