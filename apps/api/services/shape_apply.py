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

from services.shape_engine import ShapeEngine, ShapeRowError
from services.shape_models import ShapeError, ShapeRecipe

__all__ = [
    "ShapeError",
    "ShapeRowError",
    "ShapeRunner",
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

    def __init__(self, recipe: ShapeRecipe, *, sample_limit: int = 25) -> None:
        self.recipe = recipe
        self._engine = ShapeEngine(recipe, sample_limit=sample_limit)

    @property
    def recipe_hash(self) -> str:
        return self.recipe.recipe_hash

    @property
    def effect(self):
        return self._engine.effect

    @property
    def output_columns(self) -> tuple[str, ...]:
        return self.recipe.output_columns

    @property
    def removes_rows(self) -> bool:
        return self.recipe.has_active_step

    def records(self, batch: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Shape one batch of record dicts, dropping filtered/diverted rows."""
        return self._engine.apply_batch(batch)

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
            yield self._engine.apply_batch(batch)

    def matrix(
        self,
        headers: Sequence[str],
        rows: Sequence[Sequence[Any]],
    ) -> tuple[list[str], list[list[Any]]]:
        """Shape a headers+rows matrix, returning the shaped header order.

        The recipe can add, drop and rename columns, so the header row it returns
        is the recipe's output order — the caller must use it, not the one it had.
        """
        out_headers = list(self.recipe.output_columns or headers)
        shaped_rows: list[list[Any]] = []
        for row in rows:
            record = {h: (row[i] if i < len(row) else None) for i, h in enumerate(headers)}
            shaped = self._engine.apply_row(record)
            if shaped is None:
                continue
            shaped_rows.append([shaped.get(h) for h in out_headers])
        return out_headers, shaped_rows

    def report(self) -> dict[str, Any]:
        """Proof-pack fragment: what the recipe is, and what it did."""
        effect = self._engine.effect
        return {
            "recipe_hash": self.recipe_hash,
            "summary": self.recipe.describe(),
            "steps": [s.to_wire() for s in self.recipe.enabled_steps],
            "input_columns": list(self.recipe.input_columns),
            "output_columns": list(self.recipe.output_columns),
            "effect": effect.to_dict(),
            "balanced": effect.balanced,
        }


def build_shape_runner(
    payload: Any,
    *,
    source_columns: Sequence[str] | None = None,
    approved_hash: str = "",
    sample_limit: int = 25,
) -> ShapeRunner | None:
    """Parse a recipe payload into a runner, or ``None`` when there is nothing to do.

    ``approved_hash`` is the identity Validate approved. When it is given and the
    parsed recipe hashes to anything else, this raises instead of running: a run
    that shapes rows differently from the run that was approved is a different
    run, whatever the payload claims.
    """
    recipe = ShapeRecipe.parse(payload, source_columns=source_columns)
    if not recipe.enabled_steps:
        if approved_hash:
            raise ShapeError(
                "this run was approved with a shaping recipe "
                f"({approved_hash}), but no recipe was supplied — "
                "re-validate before running"
            )
        return None
    if approved_hash and recipe.recipe_hash != approved_hash:
        raise ShapeError(
            f"shaping recipe {recipe.recipe_hash} is not the one approved at Validate "
            f"({approved_hash}) — re-validate the changed recipe before running"
        )
    return ShapeRunner(recipe, sample_limit=sample_limit)


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
    """``declared`` with its scale held down to ``places``, integer digits kept."""
    from services.type_system import parse_numeric_precision_scale

    precision, scale = parse_numeric_precision_scale(declared)
    if precision is None or scale is None or scale <= places:
        return declared
    int_digits = max(precision - scale, 1)
    return f"DECIMAL({int_digits + places},{places})"


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


def shape_ledger_terms(runner: ShapeRunner | None) -> dict[str, Any]:
    """The conservation terms a shaped run adds to the write ledger.

    ``rows_shaped_out`` is every row the recipe removed — filtered *and*
    diverted — because the conservation identity closes on rows that left the
    read, and a diverted row left it just as a filtered one did. The two reasons
    stay visible in their own terms so the report can say which.
    """
    if runner is None:
        return {}
    effect = runner.effect
    return {
        "shape_recipe_hash": runner.recipe_hash,
        "rows_shaped_in": effect.rows_in,
        "rows_shaped_out": effect.rows_shaped_out + effect.rows_diverted,
        "rows_shape_filtered": effect.rows_shaped_out,
        "rows_shape_diverted": effect.rows_diverted,
        "shape_cells_changed": effect.cells_changed,
        "shape_nulls_introduced": effect.nulls_introduced,
    }
