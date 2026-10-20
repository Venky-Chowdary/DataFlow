"""Apply a shaping recipe on the read, wherever rows enter the engine.

One recipe, one engine, one accounting. The design-time API previews a recipe on
a sample; this module is what Execute uses on the population, and both go through
:class:`services.shape_engine.ShapeEngine` so a previewed row and a written row
cannot disagree.

Rows arrive in three shapes in this codebase — record dicts, batches of record
dicts, and a headers+matrix pair — so there is one entry point for each. All
three carry the same running :class:`ShapeEffect`, which the caller reports into
the write ledger as ``rows_shaped_out`` rather than as quarantine findings: a row
the operator asked to remove is not a data-quality failure.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from decimal import Decimal
from typing import Any

from services.set_graph import block_message, set_hash_refusal
from services.shape_engine import ShapeEngine, ShapeRowError
from services.shape_models import ShapeError, ShapeRecipe

__all__ = [
    "ShapeError",
    "ShapeRowError",
    "ShapeRunner",
    "apply_shape_type_ceilings",
    "build_shape_runner",
    "shape_ledger_terms",
    "shaped_schema",
]


class ShapeRunner:
    """A live recipe plus the accounting for the rows it has seen.

    A runner is stateful on purpose: the effect it carries is the run's evidence.
    It is created once per job (or per stream) and threaded through the read so
    the counts cover the whole population, not one chunk.
    """

    def __init__(
        self,
        recipe: ShapeRecipe,
        *,
        sample_limit: int = 25,
        workspace_id: str = "",
        typed_values: bool = False,
    ) -> None:
        self.recipe = recipe
        # typed_values: source is a database — '' and whitespace-only text
        # are stored values, not spreadsheet-empty cells (X-1).
        self.typed_values = typed_values
        self._engine = ShapeEngine(
            recipe, sample_limit=sample_limit, workspace_id=workspace_id
        )

    def _apply(self, fn):
        from services.shape_expr import _BLANK_AS_NULL

        token = _BLANK_AS_NULL.set(not self.typed_values)
        try:
            return fn()
        finally:
            _BLANK_AS_NULL.reset(token)

    @property
    def recipe_hash(self) -> str:
        return self.recipe.recipe_hash

    @property
    def workspace_id(self) -> str:
        """Workspace the runner resolves lookup references inside."""
        return self._engine.workspace_id

    @property
    def effect(self):
        return self._engine.effect

    @property
    def output_columns(self) -> tuple[str, ...]:
        live = getattr(self._engine, "output_columns", None)
        if live:
            return live
        return self.recipe.output_columns

    @property
    def removes_rows(self) -> bool:
        return self.recipe.has_active_step

    def records(self, batch: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Shape one batch of record dicts, dropping filtered/diverted rows."""
        return self._apply(lambda: self._engine.apply_batch(batch))

    def batches(
        self,
        batches: Iterable[Sequence[Mapping[str, Any]]],
    ) -> Iterator[list[dict[str, Any]]]:
        """Shape a stream of batches, preserving batch boundaries.

        An emptied batch is still yielded: a caller counting batches, checkpointing
        on them, or reporting progress must not silently lose one because every row
        in it was shaped out.
        """
        for batch in batches:
            yield self._apply(lambda: self._engine.apply_batch(batch))

    def matrix(
        self,
        headers: Sequence[str],
        rows: Sequence[Sequence[Any]],
    ) -> tuple[list[str], list[list[Any]]]:
        """Shape a headers+rows matrix, returning the shaped header order.

        The recipe can add, drop and rename columns, so the header row it returns
        is the recipe's output order — the caller must use it, not the one it had.
        """
        shaped_rows: list[list[Any]] = []
        for row in rows:
            record = {h: (row[i] if i < len(row) else None) for i, h in enumerate(headers)}
            for shaped in self._apply(lambda: list(self._engine.apply_records(record))):
                out_headers = list(self.output_columns or headers)
                shaped_rows.append([shaped.get(h) for h in out_headers])
        out_headers = list(self.output_columns or headers)
        return out_headers, shaped_rows

    def drain(self) -> list[dict[str, Any]]:
        """Rows buffering steps held until end-of-stream (see ShapeEngine.drain)."""
        return self._engine.drain()

    def take_diverted(self):
        """Rows shape steps diverted since the last call — quarantine matter.

        The transfer persists these to the job's DLQ after every page so a
        diverted row is replayable quarantine with step/op/reason, not only a
        ledger counter.
        """
        return self._engine.take_diverted()

    def drain_matrix(self) -> tuple[list[str], list[list[Any]]]:
        """Rows buffering steps held until end-of-stream, as a matrix page.

        dedupe keep-last, aggregate, sort and rank emit only once the whole
        population was read; the transfer calls this after the last batch so
        their winners still reach the writer. Empty when nothing was held.
        """
        rows = self._engine.drain()
        headers = list(self.output_columns)
        return headers, [[r.get(h) for h in headers] for r in rows]

    def report(self) -> dict[str, Any]:
        """Proof-pack fragment: what the recipe is, and what it did."""
        effect = self._engine.effect
        return {
            "recipe_hash": self.recipe_hash,
            "set_hash": self.recipe.set_hash,
            "summary": self.recipe.describe(),
            "steps": [s.to_wire() for s in self.recipe.enabled_steps],
            "input_columns": list(self.recipe.input_columns),
            "output_columns": list(self.recipe.output_columns),
            "effect": effect.to_dict(),
            "balanced": effect.balanced,
        }


TYPED_VALUE_SOURCES = frozenset({
    "postgresql", "postgres", "mysql", "mariadb", "sqlite", "sqlserver",
    "mssql", "oracle", "mongodb", "bigquery", "snowflake", "databricks",
    "clickhouse", "redshift", "db2", "informix", "duckdb", "cassandra",
    "cockroachdb", "greenplum", "teradata", "sybase", "hana", "vertica",
    "db2i", "firebird", "timescaledb", "yugabytedb", "singlestore",
})


def is_typed_value_source(fmt: str) -> bool:
    """True when the source is a typed database whose '' and whitespace-only
    text cells are stored values — not spreadsheet-empty cells (X-1)."""
    return str(fmt or "").strip().lower() in TYPED_VALUE_SOURCES


def build_shape_runner(
    payload: Any,
    *,
    source_columns: Sequence[str] | None = None,
    source_types: Mapping[str, str] | None = None,
    approved_hash: str = "",
    approved_set_hash: str = "",
    sample_limit: int = 25,
    workspace_id: str = "",
    typed_values: bool = False,
) -> ShapeRunner | None:
    """Parse a recipe payload into a runner, or ``None`` when there is nothing to do.

    ``approved_hash`` is the row-recipe identity Validate approved. When it is
    given and the parsed recipe hashes to anything else, this raises instead of
    running: a run that shapes rows differently from the run that was approved
    is a different run, whatever the payload claims.

    ``approved_set_hash`` is the set-graph identity. Both empty means there is
    no graph. A graph that cannot run raises before the pass-through return, so
    a recipe with no row steps and a join is not treated as "do nothing".
    """
    recipe = ShapeRecipe.parse(
        payload, source_columns=source_columns, source_types=source_types
    )
    _runner_typed = typed_values
    blocked = block_message(recipe.set_graph)
    if blocked:
        raise ShapeError(blocked)
    mismatched = set_hash_refusal(recipe.set_hash, approved_set_hash)
    if mismatched:
        raise ShapeError(mismatched)
    if not recipe.enabled_steps:
        if approved_hash:
            raise ShapeError(
                "this run was approved with a transform recipe "
                f"({approved_hash}), but no recipe was supplied — "
                "re-validate before running"
            )
        return None
    if approved_hash and recipe.recipe_hash != approved_hash:
        raise ShapeError(
            f"transform recipe {recipe.recipe_hash} is not the one approved at Validate "
            f"({approved_hash}) — re-validate the changed recipe before running"
        )
    return ShapeRunner(recipe, sample_limit=sample_limit, workspace_id=workspace_id, typed_values=_runner_typed)


def _scale_ceilings(recipe: ShapeRecipe) -> dict[str, int]:
    """Columns whose fractional scale a step bounds for *every* row, and the bound.

    Sample-driven inference adds headroom because the sample may not hold the
    widest value the population does. A ``round to 8`` step removes that doubt:
    no row can carry a ninth fractional digit, whatever the sample showed. So the
    bound is read off the recipe rather than guessed from the rows — which is what
    makes shaping able to answer a narrowing destination carrier at all. Without
    it a column rounded to 8 is still declared scale 10 and a NUMBER(11,8)
    destination is refused as lossy, for values that now fit it exactly.

    A later step that writes the column again drops the bound: only the last hand
    on a value can promise anything about it.
    """
    ceilings: dict[str, int] = {}
    for step in recipe.enabled_steps:
        if step.op == "rename_column":
            moved = ceilings.pop(step.column, None)
            target = str(step.options.get("to") or "")
            if moved is not None and target:
                ceilings[target] = moved
            continue
        for column in step.writes:
            if step.op in ("round_number", "truncate_number"):
                try:
                    places = int(step.options.get("places"))
                except (TypeError, ValueError):
                    ceilings.pop(column, None)
                    continue
                if places >= 0:
                    ceilings[column] = places
            else:
                ceilings.pop(column, None)
    return ceilings


def _capped_decimal(declared: str, places: int) -> str:
    """``declared`` with its scale held down to ``places``, integer digits kept.

    A recipe that bounds every value to a whole number (``places=0``) is an
    integer carrier. Reporting ``DECIMAL(p,0)`` made Map treat an existing
    ``INT`` / ``int4`` destination as a fidelity collapse for values the writer
    would accept — Case A: the operator rounded, and the product still refused.
    Digits beyond signed BIGINT stay ``DECIMAL(p,0)``; that is the same rule
    ``observe_numeric_samples`` uses for unshaped integers.
    """
    from services.type_system import (
        SIGNED_BIGINT_SAFE_PRECISION,
        normalize_logical_type,
        parse_numeric_precision_scale,
    )

    logical = ""
    try:
        logical = normalize_logical_type(declared)
    except Exception:
        logical = ""
    precision, scale = parse_numeric_precision_scale(declared)
    if places == 0 and logical in {"decimal", "integer", "float", "number"}:
        int_digits = 1
        if precision is not None:
            int_digits = max(precision - (scale or 0), 1)
        if int_digits <= SIGNED_BIGINT_SAFE_PRECISION:
            return "INTEGER"
        return f"DECIMAL({int_digits},0)"
    if precision is None or scale is None or scale <= places:
        return declared
    int_digits = max(precision - scale, 1)
    return f"DECIMAL({int_digits + places},{places})"


def apply_shape_type_ceilings(
    column_types: Mapping[str, str],
    recipe: ShapeRecipe,
) -> dict[str, str]:
    """Hold inferred carriers down to the scale the recipe promised.

    Validate and Execute must ask this together. Inferring from the shaped
    sample alone can still report ``DECIMAL(12,9)`` (or ``DECIMAL(3,0)``) after
    ``round_number(places=0)``, and Map then blocks an existing INT destination
    that the write would have filled with whole numbers.
    """
    ceilings = _scale_ceilings(recipe)
    resolved = {str(k): str(v) for k, v in column_types.items()}
    for column, places in ceilings.items():
        if column in resolved:
            resolved[column] = _capped_decimal(resolved[column], places)
    return resolved


# X-104 — derived-column static typing. Sizing a produced column from the
# first batch's observed values is a ticking overflow: a DECIMAL(4,2) created
# because the sample's totals were small refuses a legitimate 9999999.9999 on
# run 2, and fail-closed means a scheduled job goes red the first time the
# data grows. An expression's declared type is a bound, not a measurement —
# round(NUMERIC(12,4) * INT, 2) is DECIMAL(22,2) no matter what the sample
# held. When the AST gives no bound the column takes the wide default and
# says so in the plan summary.
_DERIVE_WIDE_NUMERIC = "DECIMAL(38,10)"
_NUMERIC_CAP = 38

_STRING_RESULT_FNS = {
    "lower", "upper", "title_case", "initcap", "trim", "ltrim", "rtrim",
    "strip", "substr", "substring", "left", "right", "replace",
    "regex_replace", "regex_extract", "split_part", "concat", "concat_ws",
    "lpad", "rpad", "pad", "format_date", "format_timestamp", "repeat",
    "reverse", "to_char", "str", "translate", "strip_characters",
}
_BOOL_RESULT_FNS = {
    "is_null", "is_not_null", "is_blank", "regex_matches", "starts_with",
    "ends_with", "contains", "between",
}
_INT_RESULT_FNS = {
    "length", "len", "char_length", "character_length", "ascii", "position",
    "strpos", "instr", "sign",
}
_DATE_RESULT_FNS = {"to_date", "date", "current_date"}
_TIMESTAMP_RESULT_FNS = {"to_timestamp", "now", "current_timestamp", "to_datetime"}
_NUMERIC_UNBOUNDED_FNS = {
    "sqrt", "power", "pow", "exp", "ln", "log", "log10", "degrees", "radians",
    "sin", "cos", "tan", "asin", "acos", "atan", "pi", "to_number",
}
_NUMERIC_UNION_FNS = {"least", "greatest", "min", "max"}
_COMPARISON_OPS = {"=", "==", "!=", "<>", "<", "<=", ">", ">=",
                   "like", "ilike", "in", "not in", "is", "is not",
                   "and", "or"}
_ARITHMETIC_OPS = {"+", "-", "*", "/", "%"}


def _numeric_ps(declared: str | None) -> tuple[int | None, int | None]:
    """(precision, scale) of a declared numeric type, else (None, None)."""
    if not declared:
        return None, None
    from services.type_system import (
        normalize_logical_type,
        parse_numeric_precision_scale,
    )
    try:
        if normalize_logical_type(declared) not in {
            "decimal", "number", "numeric", "integer", "float",
        }:
            return None, None
    except Exception:
        return None, None
    precision, scale = parse_numeric_precision_scale(declared)
    if precision is None:
        # A bare numeric/integer declaration: bound it generously rather than
        # pretend the column is not numeric.
        return (19, 0) if "int" in declared.lower() else (_NUMERIC_CAP, 10)
    return precision, scale or 0


def _declare_numeric(precision: int, scale: int) -> str:
    scale = max(scale, 0)
    precision = min(max(precision, scale + 1), _NUMERIC_CAP)
    return f"DECIMAL({precision},{scale})"


def _union_expr_types(types: list[str | None]) -> str | None:
    """The one type every branch can produce, or None when unbounded."""
    known = [t for t in types if t]
    if not known or len(known) != len(types):
        return None
    numerics = [_numeric_ps(t) for t in known]
    if all(p is not None for p, _ in numerics):
        int_digits = max((p - s) for p, s in numerics if p is not None)
        scale = max(s for _, s in numerics if s is not None)
        return _declare_numeric(int_digits + scale, scale)
    if len(set(known)) == 1:
        return known[0]
    non_numeric = [
        t for t in known
        if normalize_expr_type_name(t) not in {"decimal", "number", "numeric",
                                               "integer", "float"}
    ]
    return non_numeric[0] if len(set(non_numeric)) == 1 else "TEXT"


def normalize_expr_type_name(declared: str) -> str:
    from services.type_system import normalize_logical_type
    try:
        return normalize_logical_type(declared)
    except Exception:
        return ""


def _static_expr_type(node: Any, col_types: Mapping[str, str]) -> str | None:
    """Declared type provable from an expression AST and operand types.

    Returns None when the expression has no static bound — the caller then
    falls back to the wide numeric default (arithmetic) or the sample
    inference (everything else).
    """
    from services.shape_expr import (
        _Binary, _Call, _Case, _Column, _Literal, _Unary, Expression,
    )

    if isinstance(node, Expression):
        return _static_expr_type(node.root, col_types)
    if isinstance(node, _Literal):
        value = node.value
        if value is None:
            return None
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, Decimal):
            tup = value.as_tuple()
            digits = len(tup.digits)
            scale = max(-tup.exponent, 0)
            return _declare_numeric(digits, scale)
        if isinstance(value, int):
            return _declare_numeric(len(str(abs(value))), 0)
        if isinstance(value, float):
            return "FLOAT"
        return "TEXT"
    if isinstance(node, _Column):
        return col_types.get(node.name)
    if isinstance(node, _Unary):
        if node.op == "not":
            return "BOOLEAN"
        inner = _static_expr_type(node.operand, col_types)
        return inner if inner and _numeric_ps(inner)[0] is not None else None
    if isinstance(node, _Case):
        results = [result for _, result in node.whens]
        if node.fallback is not None:
            results.append(node.fallback)
        return _union_expr_types(
            [
                _static_expr_type(r, col_types)
                for r in results
                if not (
                    isinstance(r, _Literal) and r.value is None
                )
            ]
        )
    if isinstance(node, _Call):
        name = str(node.name)
        if name in _STRING_RESULT_FNS:
            return "TEXT"
        if name in _BOOL_RESULT_FNS:
            return "BOOLEAN"
        if name in _INT_RESULT_FNS:
            return _declare_numeric(19, 0)
        if name in _DATE_RESULT_FNS:
            return "DATE"
        if name in _TIMESTAMP_RESULT_FNS:
            return "TIMESTAMP"
        if name in {"if", "coalesce"}:
            branch_args = node.args[1:] if name == "if" else node.args
            return _union_expr_types(
                [
                    _static_expr_type(a, col_types)
                    for a in branch_args
                    if not (isinstance(a, _Literal) and a.value is None)
                ]
            )
        if name in _NUMERIC_UNION_FNS:
            return _union_expr_types(
                [_static_expr_type(a, col_types) for a in node.args]
            )
        if name in _NUMERIC_UNBOUNDED_FNS:
            return _DERIVE_WIDE_NUMERIC
        if name in {"abs", "ceil", "ceiling", "floor"}:
            operand = _static_expr_type(node.args[0], col_types)
            p, s = _numeric_ps(operand)
            if p is None:
                return _DERIVE_WIDE_NUMERIC
            if name == "abs":
                return _declare_numeric(p, s or 0)
            return _declare_numeric(p - (s or 0) + 1, 0)
        if name in {"round", "truncate"}:
            operand = _static_expr_type(node.args[0], col_types)
            p, s = _numeric_ps(operand)
            places = node.args[1] if len(node.args) > 1 else None
            k = None
            if isinstance(places, _Literal) and places.value is not None:
                try:
                    as_int = int(places.value)
                    if as_int == places.value or str(places.value) == str(as_int):
                        k = as_int
                except (TypeError, ValueError):
                    k = None
            if p is None or k is None:
                return _DERIVE_WIDE_NUMERIC
            int_digits = max(p - (s or 0), 1)
            # +1 headroom: rounding can carry into a new most-significant
            # digit (99.5 → 100).
            return _declare_numeric(int_digits + k + 1, k)
        return None
    if isinstance(node, _Binary):
        op = str(node.op).casefold()
        if op in _COMPARISON_OPS:
            return "BOOLEAN"
        if op in _ARITHMETIC_OPS:
            left_p, left_s = _numeric_ps(
                _static_expr_type(node.left, col_types)
            )
            right_p, right_s = _numeric_ps(
                _static_expr_type(node.right, col_types)
            )
            if left_p is None or right_p is None:
                return _DERIVE_WIDE_NUMERIC
            left_i = max(left_p - (left_s or 0), 1)
            right_i = max(right_p - (right_s or 0), 1)
            if op in {"+", "-"}:
                scale = max(left_s or 0, right_s or 0)
                return _declare_numeric(max(left_i, right_i) + scale + 1, scale)
            if op == "*":
                return _declare_numeric(
                    left_p + right_p + 1, (left_s or 0) + (right_s or 0)
                )
            if op == "/":
                # Division is the unbounded case the defect named: declare the
                # wide default rather than sizing off whatever the batch held.
                return _DERIVE_WIDE_NUMERIC
            scale = max(left_s or 0, right_s or 0)
            return _declare_numeric(min(left_i, right_i) + scale, scale)
    return None


def derived_static_types(
    recipe: ShapeRecipe,
    prior_schema: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Declared types provable from derive_column/set_value expressions.

    Steps run in order, so a derived column can reference an earlier produced
    column — the working map tracks each step's declared output so later
    expressions see it. Columns with no static bound keep the wide numeric
    default; a column the AST cannot type at all is left to the sample
    inference (``None`` → caller falls back).
    """
    working: dict[str, str] = {str(k): str(v) for k, v in (prior_schema or {}).items()}
    declared: dict[str, str] = {}
    for step in recipe.enabled_steps:
        # X-109: row-set ops produce typed columns too — aggregate measures
        # and surrogate keys were inferred as TEXT from the batch sample on
        # every engine. SQL semantics: count → BIGINT; sum → DECIMAL(p+10,s)
        # of the input; avg → DECIMAL(cap, s+6); min/max/first/last → the
        # input's own type; surrogate_key → BIGINT.
        if step.op == "aggregate":
            for measure in (step.options or {}).get("measures") or []:
                if not isinstance(measure, Mapping):
                    continue
                fn = str(measure.get("function") or "").lower()
                tgt = str(measure.get("as") or measure.get("to") or "")
                if not tgt:
                    continue
                src_t = working.get(str(measure.get("column") or ""))
                if fn in ("count", "count_distinct", "countdistinct"):
                    decl = "BIGINT"
                elif fn == "sum":
                    p, s = _numeric_ps(src_t)
                    decl = _declare_numeric(
                        min((p or _NUMERIC_CAP) + 10, _NUMERIC_CAP), s or 0
                    )
                elif fn in ("avg", "average"):
                    p, s = _numeric_ps(src_t)
                    decl = _declare_numeric(_NUMERIC_CAP, (s or 0) + 6)
                elif fn in ("min", "max", "first", "last"):
                    decl = src_t or "VARCHAR"
                else:
                    continue
                working[tgt] = decl
                declared[tgt] = decl
            continue
        if step.op == "surrogate_key":
            tgt = str(
                (step.options or {}).get("to")
                or (step.options or {}).get("as")
                or step.column
                or ""
            )
            if tgt:
                working[tgt] = "BIGINT"
                declared[tgt] = "BIGINT"
            continue
        # X-116: only structurally-typed outputs are declared here.
        # nest_json's ``to`` is a JSON object by construction → json (PG
        # jsonb, MySQL json — was inferred TEXT). unnest's ``index_to`` is a
        # positional counter → BIGINT. flatten_json keys and unnest's ``to``
        # are NOT statically typed: the same column legitimately mixes a
        # JSON object and a plain string across rows, so forcing jsonb
        # would mis-store the scalars — sample inference keeps them.
        if step.op == "nest_json":
            opts = step.options or {}
            tgt = str(opts.get("to") or step.column or "")
            if tgt:
                working[tgt] = "json"
                declared[tgt] = "json"
            continue
        if step.op == "unnest_json":
            opts = step.options or {}
            idx = str(opts.get("index_to") or "")
            if idx:
                working[idx] = "BIGINT"
                declared[idx] = "BIGINT"
            continue
        if step.op not in ("derive_column", "set_value"):
            continue
        target = (
            str(step.options.get("to") or "")
            if step.op == "derive_column"
            else str(step.column or "")
        )
        if not target:
            continue
        static = (
            _static_expr_type(
                getattr(step.expression, "root", step.expression), working
            )
            if step.expression is not None
            else None
        )
        if static is None:
            continue
        working[target] = static
        declared[target] = static
    return declared


def shaped_schema(
    runner: ShapeRunner,
    shaped_rows: Sequence[Mapping[str, Any]],
    prior_schema: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Declared types for the shaped columns.

    A column no step writes keeps the type the source declared — shaping must not
    re-open a carrier decision it never touched. A column a step writes (or
    creates) is re-inferred from the shaped values, because its old type describes
    values that no longer exist.
    """
    from services.schema_inference import infer_schema_map

    prior = dict(prior_schema or {})
    ceilings = _scale_ceilings(runner.recipe)
    columns = list(runner.output_columns) or list(shaped_rows[0].keys() if shaped_rows else [])
    touched = runner.recipe.touched_columns
    rename_source = {
        str(step.options.get("to") or ""): step.column
        for step in runner.recipe.enabled_steps
        if step.op == "rename_column"
    }

    unknown = [c for c in columns if c in touched or c not in prior]
    inferred: dict[str, str] = {}
    # No shaped rows means nothing was measured, so nothing may be declared from
    # them: inferring off an empty sample reports every column as VARCHAR and a
    # numeric destination then reads as a lossy coercion of a text column.
    if unknown and shaped_rows:
        samples = {
            column: [
                "" if row.get(column) is None else str(row.get(column))
                for row in shaped_rows[:500]
            ]
            for column in unknown
        }
        inferred, _ = infer_schema_map(samples)

    # X-104: expression-derived columns declare their static type — a bound
    # from the AST, not a measurement of the batch that happened to run first.
    static = derived_static_types(runner.recipe, prior)

    out: dict[str, str] = {}
    for column in columns:
        if column not in touched and column in prior:
            out[column] = prior[column]
            continue
        # A pure rename carries its column's declared type across, untouched.
        origin = rename_source.get(column, "")
        if origin and origin not in touched and origin in prior:
            out[column] = prior[origin]
            continue
        declared = (
            static.get(column)
            or inferred.get(column)
            or prior.get(column)
            or "VARCHAR"
        )
        ceiling = ceilings.get(column)
        if ceiling is not None:
            declared = _capped_decimal(declared, ceiling)
        out[column] = declared
    return out


def shape_ledger_terms(runner: ShapeRunner | None) -> dict[str, Any]:
    """The conservation terms a shaped run adds to the write ledger.

    ``rows_shaped_out`` is rows the recipe *removed* — drop/filter — while
    ``rows_shape_diverted`` counts rows sent to quarantine. A diverted row is
    not a removal: it sits in the DLQ with a reason and is charged to
    ``rejected_rows`` so the ledger reads ``in = out + quarantined`` (R2) —
    counting it in both terms double-subtracted it from conservation.
    """
    if runner is None:
        return {}
    effect = runner.effect
    out = {
        "shape_recipe_hash": runner.recipe_hash,
        "rows_shaped_in": effect.rows_in,
        "rows_shaped_out": effect.rows_shaped_out,
        "rows_shape_filtered": effect.rows_shaped_out,
        "rows_shape_diverted": effect.rows_diverted,
        "rows_expanded": effect.rows_expanded,
        "shape_cells_changed": effect.cells_changed,
        "shape_nulls_introduced": effect.nulls_introduced,
    }
    # Per dedupe step: the declared winner rule, its keys, and the exact
    # conservation tallies — Gate-8 balances rows_in against winners +
    # ledgered losers and checks winner uniqueness on the destination.
    dedupe_steps: list[dict[str, Any]] = []
    for i, step in enumerate(runner.recipe.enabled_steps):
        if step.op != "dedupe_rows":
            continue
        tally = effect.steps[i] if i < len(effect.steps) else None
        opts = step.options or {}
        # Declared source types for the columns the check compares — Gate-8
        # canonicalizes both sides under them, never wire text (DD-1).
        _types = dict(getattr(runner.recipe, "source_types", None) or {})
        _order_cols = [str(opts.get("order_by") or "")] + [
            str(t.get("column") if isinstance(t, Mapping) else t)
            for t in (opts.get("then_by") or [])
        ]
        dedupe_steps.append(
            {
                "step": i + 1,
                "keys": [str(c) for c in opts.get("keys") or []],
                "keep": str(opts.get("keep") or "first"),
                "order_by": str(opts.get("order_by") or "") or None,
                "order_dir": str(opts.get("order_dir") or "") or None,
                "nulls": str(opts.get("nulls") or "last"),
                "then_by": list(opts.get("then_by") or []),
                "tie_break": str(opts.get("tie_break") or "first_seen"),
                "merge": str(opts.get("merge") or "") or None,
                "key_types": {
                    c: _types[c] for c in (opts.get("keys") or []) if c in _types
                },
                "order_types": {
                    c: _types[c] for c in _order_cols if c in _types
                },
                "rows_in": int(getattr(tally, "rows_in", 0) or 0),
                "rows_out": int(getattr(tally, "rows_out", 0) or 0),
                "rows_diverted": int(getattr(tally, "rows_diverted", 0) or 0),
                "rows_removed": int(getattr(tally, "rows_removed", 0) or 0),
            }
        )
    if dedupe_steps:
        out["shape_dedupe"] = dedupe_steps
        out["shape_output_columns"] = list(runner.output_columns or ())
    # X-101: hash_identity's encoding changed (rk1 \x1f-joined → rk2 tagged
    # length-prefixed). State the version in the run summary so an operator
    # comparing row keys across runs knows existing keys were re-keyed, not
    # silently changed.
    rk_steps = [
        {
            "step": i + 1,
            "column": str(step.options.get("to") or "_df_row_key"),
            "encoding": "rk2",
        }
        for i, step in enumerate(runner.recipe.enabled_steps)
        if step.op == "hash_identity"
    ]
    if rk_steps:
        out["shape_hash_identity"] = rk_steps
    # X-104: the plan shows each derived column's declared type — the static
    # bound, not a batch measurement — and flags the ones with no static
    # bound that took the wide default so an operator knows it was declared,
    # not measured.
    derived = derived_static_types(
        runner.recipe, getattr(runner.recipe, "source_types", None)
    )
    if derived:
        out["shape_derived_columns"] = [
            {
                "column": column,
                "declared_type": declared,
                "wide_default": declared == _DERIVE_WIDE_NUMERIC,
            }
            for column, declared in derived.items()
        ]
    return out
