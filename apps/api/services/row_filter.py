"""Row-level filtering for source records before mapping and transfer.

Supports atomic predicates and nested `and`/`or` composition.  Values are
compared in a type-coercion-aware way: if both the row value and the filter
value can be parsed as numbers, numeric comparison is used; otherwise the
comparison falls back to string equality/case-sensitive containment.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

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
    _refuse_null_operand(column, node[key], node.get("value"))
    if node[key] == "regex":
        try:
            re.compile(_cell_text(_to_scalar(node.get("value"))))
        except re.error as exc:
            raise ValueError(
                f"Filter on {column!r} has an invalid regular expression: {exc}."
            ) from None
    return node


def _refuse_null_operand(column: str, op: str, value: Any) -> None:
    """A comparison against NULL matches no row in SQL; say so before a run.

    ``x = NULL`` and ``x IN (NULL)`` are UNKNOWN, and ``x NOT IN (a, NULL)``
    is UNKNOWN for every x, so such a filter would land nothing (before D364
    it matched ``''``). The CDC snapshot compiler refuses the same input.
    """
    if op in _NULL_OPS:
        return
    from services.value_serializer import is_reader_null_cell

    if op in {"in", "not_in"}:
        items = list(value) if isinstance(value, (list, tuple, set)) else [value]
    else:
        items = [_to_scalar(value)]
    if not items or any(is_reader_null_cell(item) for item in items):
        raise ValueError(
            f"Filter on {column!r}: {op} needs a non-null value. A comparison "
            "with NULL matches no row in SQL, and NOT IN with a NULL in the "
            "list matches none. Use is_null or is_not_null, joined with 'or' "
            "to keep NULL rows."
        )


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


def empty_string_is_sql_null(engine: str | None) -> bool:
    """Oracle stores a zero-length string as NULL. Every other engine does not.

    Oracle SQL reference, Nulls: "The database currently treats a character
    value with a length of zero as null." PostgreSQL, SQL Server, MySQL and
    MariaDB store ``''`` as a value. A filter on those engines must not drop it.
    """
    return (engine or "").strip().lower() in {"oracle", "oracle_db"}


def _is_null(value: Any, *, engine: str | None = None) -> bool:
    """SQL NULL for a source filter. Empty text is a value, except on Oracle.

    ``is_null_evidence`` is a type-evidence probe: it is true for blank and
    whitespace text. That dropped a PostgreSQL ``''`` from ``is_not_null``
    (D365). The reader predicate (``is_reader_null_cell``) matches ``None``,
    the missing sentinel and the reader NULL wire, and leaves ``''``,
    whitespace, ``'NULL'`` and ``'none'`` as values. Comparisons other than
    ``is_null`` / ``is_not_null`` still do not match a NULL (SQL three-valued
    logic, D364).
    """
    from services.value_serializer import is_reader_null_cell

    if is_reader_null_cell(value):
        return True
    # NaN is a float value. Treating it as NULL dropped it from is_not_null
    # and from every comparison (D365).
    if engine is None:
        from services.source_engine_scope import active_source_engine

        engine = active_source_engine()
    if empty_string_is_sql_null(engine) and value == "":
        return True
    return False


def _is_numeric_nan(value: Any) -> bool:
    if isinstance(value, float):
        return value != value
    try:
        from decimal import Decimal

        if isinstance(value, Decimal):
            return value.is_nan()
    except Exception:
        return False
    # The text "nan" / "Nan" is a string. Only a real float or Decimal NaN
    # uses PostgreSQL's numeric ordering (D365).
    return False


def _compare_nan(row_value: Any, filter_value: Any, op: str) -> bool:
    """PostgreSQL float ordering: NaN = NaN, and NaN is greater than every number."""
    row_nan = _is_numeric_nan(row_value)
    filt_nan = _is_numeric_nan(filter_value)
    if op in {"in", "not_in"}:
        allowed = filter_value if isinstance(filter_value, (list, tuple, set)) else [filter_value]
        matched = any(_is_numeric_nan(item) for item in allowed) if row_nan else False
        return matched if op == "in" else not matched
    if op == "eq":
        return row_nan and filt_nan
    if op == "ne":
        return not (row_nan and filt_nan)
    if op == "lt":
        return (not row_nan) and filt_nan
    if op == "lte":
        return filt_nan
    if op == "gt":
        return row_nan and not filt_nan
    if op == "gte":
        return row_nan
    return False


def _boolean_word(value: Any) -> bool:
    """True when the cell is a boolean literal, a Python bool, or a one-byte BIT."""
    if isinstance(value, bool):
        return True
    if isinstance(value, (bytes, bytearray)) and len(value) == 1 and value[0] in (0, 1):
        return True
    return _cell_text(value).strip().lower() in {"true", "false", "t", "f"}


def _boolean_bit(value: Any) -> bool | None:
    """1/0/true/false as a bit. ``''``, ``yes``, and every other token are not."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, float) and value in (0.0, 1.0):
        return bool(value)
    try:
        from decimal import Decimal

        if isinstance(value, Decimal) and value in (Decimal(0), Decimal(1)):
            return bool(value)
    except Exception:
        pass
    if isinstance(value, (bytes, bytearray)) and len(value) == 1 and value[0] in (0, 1):
        return bool(value[0])
    text = _cell_text(value).strip().lower()
    if text in {"true", "t", "1", "1.0"}:
        return True
    if text in {"false", "f", "0", "0.0"}:
        return False
    return None


def _boolean_equal(left: Any, right: Any) -> bool:
    """``eq`` for a boolean literal against a bit, shared by ``in`` (D382)."""
    if not (_boolean_word(left) or _boolean_word(right)):
        return False
    a, b = _boolean_bit(left), _boolean_bit(right)
    return a is not None and a == b


def _cell_text(value: Any) -> str:
    """One filter token. Reader-null / blank become ``""`` (same as None).

    ``str(value)`` invented ``True`` vs dest ``"true"``, scientific
    ``1E+2`` vs dest-canonical ``100``, and ``str(None) == "None"`` so
    ``in ["None"]`` matched a SQL NULL.
    """
    from services.value_serializer import present_cell_text

    return present_cell_text(value) or ""


def _compare_values(
    row_value: Any, filter_value: Any, op: str, *, engine: str | None = None
) -> bool:
    rv = _to_scalar(row_value)
    fv = _to_scalar(filter_value)

    if op == "is_null":
        return _is_null(rv, engine=engine)
    if op == "is_not_null":
        return not _is_null(rv, engine=engine)

    # NULL <op> x is UNKNOWN, so the row is excluded. ``ne`` and ``not_in``
    # do not match NULL either (SQL three-valued logic). ``''`` is a value
    # and is compared below (D364). On Oracle ``''`` is NULL, so the engine
    # is part of the predicate (D365).
    if _is_null(rv, engine=engine):
        return False

    if op in {"in", "not_in"}:
        allowed = list(fv) if isinstance(fv, (list, tuple, set)) else [fv]
        # ``x IN (a, NULL)`` is true only for ``a``: the NULL item equals
        # nothing. ``x NOT IN (a, NULL)`` is never true, because ``x <> NULL``
        # is UNKNOWN for every x (SQL three-valued logic; PostgreSQL 9.24 and
        # MySQL agree). canonical_filter_spec refuses such a list up front.
        if any(_is_null(item, engine=engine) for item in allowed):
            if op == "not_in":
                return False
            allowed = [item for item in allowed if not _is_null(item, engine=engine)]
    elif _is_null(fv, engine=engine):
        # ``x = NULL`` / ``x < NULL`` is UNKNOWN, never a match on ''.
        return False

    # PostgreSQL numeric NaN is not NULL. It equals NaN and sorts above every
    # non-NaN number (D365). A text compare of "nan" against "10" does not.
    if _is_numeric_nan(rv) or _is_numeric_nan(fv):
        return _compare_nan(rv, fv, op)

    if op in {"in", "not_in"}:
        hit = any(_boolean_equal(rv, item) for item in allowed)
        if not hit:
            allowed_str = {_cell_text(v) for v in allowed}
            hit = _cell_text(rv) in allowed_str
        return hit if op == "in" else not hit

    rv_str = _cell_text(rv)
    fv_str = _cell_text(fv)

    # MySQL BOOLEAN / SQL Server BIT(1) / Oracle NUMBER(1) arrive as 1 or 0.
    # ``flag eq true`` must match that bit. ``''`` and NULL are not false
    # (D365, D364). Ordering operators stay numeric or text (D382).
    if op in {"eq", "ne"} and (_boolean_word(rv) or _boolean_word(fv)):
        left, right = _boolean_bit(rv), _boolean_bit(fv)
        if left is not None and right is not None:
            return (left == right) if op == "eq" else (left != right)

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


def _matches(
    record: dict[str, Any], spec: dict[str, Any], *, engine: str | None = None
) -> bool:
    if not spec:
        return True

    if "and" in spec:
        return all(_matches(record, child, engine=engine) for child in spec["and"] if child)
    if "or" in spec:
        return any(_matches(record, child, engine=engine) for child in spec["or"] if child)

    column = spec.get("column") or spec.get("field")
    if not column:
        # Never match-all: a predicate that names no column cannot be judged
        # (canonical_filter_spec refuses it before a run starts).
        raise ValueError(
            f"A filter predicate needs a column name. Use {FILTER_SHAPE_HELP}."
        )
    op = canonical_filter_op(spec.get("operator") or spec.get("op") or "eq")
    value = record.get(column)
    return _compare_values(value, spec.get("value"), op, engine=engine)


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


def null_dropping_warnings(spec: dict[str, Any] | None) -> list[str]:
    """Plan-time warning for the D364 behaviour change: NULL rows now drop out.

    SQL three-valued logic makes ``NULL <> x`` and ``NULL < x`` unknown.
    Before D364 the in-memory evaluator read NULL as ``''``, so ``ne``,
    ``not_in``, ``lt`` and ``lte`` kept NULL rows; an existing schedule with
    such a filter now lands fewer rows. MongoDB ``$ne``/``$nin`` match null
    and missing fields, so the snapshot filter adds an existence guard.
    """
    negated: list[str] = []
    below: list[str] = []

    def walk(node: Any) -> None:
        if not isinstance(node, dict):
            return
        for key in ("and", "or"):
            if key in node:
                for child in node.get(key) or []:
                    walk(child)
                return
        raw = " ".join(str(node.get("op") or node.get("operator") or "").split()).lower()
        op = FILTER_OP_ALIASES.get(raw, {"neq": "ne", "nin": "not_in"}.get(raw, raw))
        column = str(node.get("column") or node.get("field") or "column")
        if op in {"ne", "not_in"}:
            negated.append(column)
        elif op in {"lt", "lte"}:
            below.append(column)

    walk(spec)
    out: list[str] = []
    if negated:
        names = ", ".join(dict.fromkeys(negated))
        out.append(
            f"Filter on {names} uses not-equal or not-in, which drops NULL rows. "
            "SQL treats that comparison as unknown, and MongoDB $ne/$nin match "
            "null and missing fields unless the plan requires the field to exist "
            "and be non-null."
        )
    if below:
        names = ", ".join(dict.fromkeys(below))
        out.append(
            f"Filter on {names} uses less-than, which drops NULL rows (SQL treats "
            "NULL < x as unknown). Earlier runs kept them; join an is_null "
            "condition with 'or' to keep them."
        )
    return out


def iter_filtered_rows(rows, spec: dict[str, Any] | None, *, engine: str | None = None):
    """Yield rows that satisfy ``spec``. Pass-through when there is no filter.

    Streaming counterpart of :func:`apply_row_filter` so a table walk can
    judge the same subset Execute writes without materialising the table.
    ``engine`` is the source engine: Oracle treats ``''`` as NULL.
    """
    if rows is None:
        return None
    if not spec:
        return rows

    def _gen():
        for rec in rows:
            if isinstance(rec, dict) and _matches(rec, spec, engine=engine):
                yield rec

    return _gen()


def apply_row_filter(
    records: list[dict[str, Any]],
    spec: dict[str, Any] | None,
    *,
    engine: str | None = None,
) -> list[dict[str, Any]]:
    """Return only records that satisfy ``spec``.

    An empty or ``None`` spec is a no-op (returns the input unchanged).
    ``engine`` selects Oracle's empty-string rule. A not-equal filter logs
    the plan warning that NULL rows are dropped.
    """
    if not spec or not records:
        return records
    for warning in null_dropping_warnings(spec):
        logger.info(warning)
    return [r for r in records if _matches(r, spec, engine=engine)]


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
