"""The image Validate must judge when a pre-load transform is approved.

Execute shapes rows on the read, before any writer sees them, so the rows the
destination is offered are the transformed rows. Validate was still judging the
raw source: an operator who added ``strip_characters`` to remove a control
character watched Validate block on that control character, on a value the write
would never have carried. That is the worst kind of gate — it refuses work the
engine would have done correctly, and it teaches the operator that the transform
step does nothing.

This module resolves the one image both phases agree on: the recipe applied to
the rows Validate holds, the column set it produces, and the types those columns
now carry. It changes no gate; it only makes every gate ask its question about
the values that will actually be written.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from services.schema_inference import infer_schema_map
from services.shape_engine import ShapeEngine, ShapeRowError
from services.shape_models import ShapeError, ShapeRecipe


class ShapePreflightRefused(ValueError):
    """The approved recipe cannot run on the rows Validate holds."""


@dataclass(frozen=True, slots=True)
class ShapedPreflightImage:
    """What Validate should judge, and the evidence for why it differs."""

    applied: bool
    recipe_hash: str
    columns: list[str]
    column_types: dict[str, str]
    sample_rows: list[dict[str, Any]] | None
    rows_in: int = 0
    rows_out: int = 0
    rows_removed: int = 0
    rows_diverted: int = 0
    retyped_columns: dict[str, str] = field(default_factory=dict)

    def note(self) -> str:
        """One operator-facing line describing what Validate judged."""
        if not self.applied:
            return ""
        parts = [f"Transform recipe {self.recipe_hash} applied before the gates"]
        if self.rows_removed:
            parts.append(f"{self.rows_removed} sampled row(s) removed by transform")
        if self.rows_diverted:
            parts.append(f"{self.rows_diverted} sampled row(s) diverted by transform")
        if self.retyped_columns:
            named = ", ".join(
                f"{name} → {carrier}" for name, carrier in sorted(self.retyped_columns.items())
            )
            parts.append(f"transformed column type(s) re-read from the transformed sample: {named}")
        return " — ".join(parts)


def shaped_preflight_image(
    recipe_payload: Any,
    *,
    columns: Sequence[str],
    column_types: Mapping[str, str],
    sample_rows: Sequence[Mapping[str, Any]] | None,
) -> ShapedPreflightImage:
    """Apply an approved recipe to the Validate image, or pass it through.

    Raises :class:`ShapePreflightRefused` when the recipe is unrunnable or
    refuses one of the rows Validate holds — Validate fails closed rather than
    scoring a program Execute would abort on.
    """
    declared = [str(c) for c in columns]
    declared_types = {str(k): str(v) for k, v in (column_types or {}).items()}
    rows = [dict(r) for r in (sample_rows or [])]
    passthrough = ShapedPreflightImage(
        applied=False,
        recipe_hash="",
        columns=declared,
        column_types=declared_types,
        sample_rows=None if sample_rows is None else rows,
    )

    try:
        recipe = ShapeRecipe.parse(recipe_payload, source_columns=declared or None)
    except ShapeError as exc:
        raise ShapePreflightRefused(
            f"The approved transform recipe cannot run against this source: {exc}"
        ) from exc
    if not recipe.enabled_steps:
        return passthrough

    engine = ShapeEngine(recipe, keep_diverted_records=False)
    try:
        shaped = engine.apply_batch(rows)
    except ShapeRowError as exc:
        raise ShapePreflightRefused(
            f"The approved transform recipe refused a sampled row: {exc}"
        ) from exc

    out_columns = list(recipe.output_columns) or _columns_of(shaped) or declared
    out_types, retyped = shaped_column_types(
        out_columns,
        declared_types=declared_types,
        touched=recipe.touched_columns,
        rows=shaped,
        recipe=recipe,
    )
    return ShapedPreflightImage(
        applied=True,
        recipe_hash=recipe.recipe_hash,
        columns=out_columns,
        column_types=out_types,
        sample_rows=None if sample_rows is None else shaped,
        rows_in=engine.effect.rows_in,
        rows_out=engine.effect.rows_out,
        rows_removed=engine.effect.rows_shaped_out,
        rows_diverted=engine.effect.rows_diverted,
        retyped_columns=retyped,
    )


def _columns_of(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    seen: list[str] = []
    for row in rows:
        for key in row:
            name = str(key)
            if name not in seen:
                seen.append(name)
    return seen


def shaped_column_types(
    columns: Sequence[str],
    *,
    declared_types: Mapping[str, str],
    touched: frozenset[str],
    rows: Sequence[Mapping[str, Any]],
    recipe: ShapeRecipe | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    """Types for the transformed image: declared where untouched, re-read where not.

    A column no step wrote keeps the type the source declared — inference from a
    sample is weaker evidence than the catalog. A column the recipe wrote (a cast,
    a parse, a derived column) no longer holds the declared carrier, so its type is
    re-read from the transformed values through the same inference every other read
    path uses. The recipe's scale ceiling is then applied so Validate and Execute
    report the same carrier — a column rounded to whole numbers is ``INTEGER``,
    not a leftover ``DECIMAL(p,s)`` that Map would refuse into an existing INT.
    """
    from services.shape_apply import apply_shape_type_ceilings

    resolved: dict[str, str] = {}
    retyped: dict[str, str] = {}
    needs_inference = [
        name for name in columns if name in touched or name not in declared_types
    ]
    inferred: dict[str, str] = {}
    if needs_inference and rows:
        samples: dict[str, list[str]] = {}
        for name in needs_inference:
            values: list[str] = []
            for row in rows:
                value = row.get(name)
                if value is None:
                    continue
                values.append(str(value))
            samples[name] = values
        inferred, _intel = infer_schema_map(samples)

    for name in columns:
        if name in needs_inference:
            carrier = str(inferred.get(name) or "").strip()
            if carrier:
                resolved[name] = carrier
                continue
            # Nothing to read from (an all-null derived column in this sample):
            # keep the declared carrier if there is one and say nothing more.
            if name in declared_types:
                resolved[name] = declared_types[name]
            continue
        resolved[name] = declared_types[name]
    if recipe is not None:
        resolved = apply_shape_type_ceilings(resolved, recipe)
    for name, carrier in resolved.items():
        if declared_types.get(name, carrier) != carrier:
            retyped[name] = carrier
    return resolved, retyped
