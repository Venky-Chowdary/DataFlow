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
    "recipe_output_schema",
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
        self._sample_limit = sample_limit
        self._engine = ShapeEngine(
            recipe, sample_limit=sample_limit, workspace_id=workspace_id
        )

    def fork(self) -> "ShapeRunner":
        """A fresh runner for the same recipe under the same value model.

        Design-time probes (the sample Map is built from, the population fit
        scan, Validate) must not add to this runner's accounting, but they
        must evaluate every step exactly as the write does. Re-building a
        runner from ``.recipe`` alone dropped ``typed_values``, so a database
        source was probed with spreadsheet semantics ('' read as NULL) and the
        prediction disagreed with what was written.
        """
        return ShapeRunner(
            self.recipe,
            sample_limit=self._sample_limit,
            workspace_id=self.workspace_id,
            typed_values=self.typed_values,
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
        return self._apply(self._engine.drain)

    def population(
        self, rows: Iterable[Mapping[str, Any]]
    ) -> Iterator[dict[str, Any]]:
        """Every row the recipe emits for ``rows``, buffered steps drained last.

        One row in may be zero (filter), one, or several (unnest) rows out, and
        dedupe/aggregate/sort emit only at the end of the stream — a scan that
        kept ``shaped[0]`` per row judged a different population than the
        writer receives.
        """
        for row in rows:
            yield from self.records([dict(row)])
        yield from self.drain()

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
        rows = self.drain()
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


# Keyed on engine family: every duplex SQL tile is typed (one resolver).
TYPED_VALUE_SOURCES = frozenset({
    "postgresql", "mysql", "sqlite", "sqlserver", "oracle", "mongodb",
    "bigquery", "snowflake", "databricks", "clickhouse", "redshift", "db2",
    "informix", "duckdb", "cassandra", "greenplum", "teradata", "sybase",
    "hana", "vertica", "db2i", "firebird",
})


def is_typed_value_source(fmt: str) -> bool:
    """True when the source is a typed database whose '' and whitespace-only
    text cells are stored values — not spreadsheet-empty cells (X-1)."""
    from src.transfer.engine_family import family_or_kind

    return family_or_kind(fmt) in TYPED_VALUE_SOURCES


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


def shaped_schema(
    runner: ShapeRunner,
    shaped_rows: Sequence[Mapping[str, Any]],
    prior_schema: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Declared types for the shaped columns (see :func:`recipe_output_schema`)."""
    columns = list(runner.output_columns) or list(shaped_rows[0].keys() if shaped_rows else [])
    return recipe_output_schema(
        runner.recipe,
        columns,
        shaped_rows,
        prior_schema,
        typed_values=runner.typed_values,
    )


def recipe_output_schema(
    recipe: ShapeRecipe,
    columns: Sequence[str],
    shaped_rows: Sequence[Mapping[str, Any]],
    prior_schema: Mapping[str, str] | None = None,
    *,
    typed_values: bool = False,
) -> dict[str, str]:
    """The typed schema a recipe hands the writer — one rule for every phase.

    Execute, Validate and the Transform preview all ask this; the carrier Map is
    shown is therefore the carrier the writer creates.

    A column no step writes keeps the type the source declared — shaping must not
    re-open a carrier decision it never touched. A column a step writes (or
    creates) is re-inferred from the shaped values, because its old type describes
    values that no longer exist.
    """
    from services.schema_inference import infer_schema_map

    prior = dict(prior_schema or {})
    ceilings = _scale_ceilings(recipe)
    columns = list(columns)
    touched = recipe.touched_columns
    rename_source = {
        str(step.options.get("to") or ""): step.column
        for step in recipe.enabled_steps
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
        if typed_values:
            # A database's '' / '   ' is a stored value, and it is not a number
            # or a date. Inference reads it as an empty cell, so a trimmed text
            # column holding '' came back DECIMAL and the write refused ''.
            for column in unknown:
                if any(
                    isinstance(row.get(column), str) and not row.get(column).strip()
                    for row in shaped_rows[:500]
                ):
                    inferred[column] = _text_carrier(prior.get(column, ""))

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
        declared = inferred.get(column) or prior.get(column) or "VARCHAR"
        ceiling = ceilings.get(column)
        if ceiling is not None:
            declared = _capped_decimal(declared, ceiling)
        out[column] = declared
    return out


def _text_carrier(prior: str) -> str:
    """The prior carrier when it is already text, else VARCHAR."""
    head = str(prior or "").strip().upper()
    if head.startswith(("TEXT", "VARCHAR", "CHAR", "NVARCHAR", "NCHAR", "STRING", "CLOB")):
        return str(prior)
    return "VARCHAR"


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
        # Union-appended rows are counted in effect.rows_in (they enter the
        # pipeline) but they were never read from the source, so for the
        # write ledger they are an expansion of the source population —
        # without folding them in, Gate-8 reports the appended rows as
        # unexplained extras on the destination (CI-4 shape_join_union).
        "rows_expanded": effect.rows_expanded
        + getattr(effect, "rows_union_appended", 0),
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
    return out
