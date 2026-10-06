"""Shape — the pre-write stage where raw source data is prepared.

Three endpoints, and each one answers a question the operator is actually
holding: what can I do (`/catalog`), what is wrong with my data and what fixes
it (`/profile`), and what would my recipe do to these rows (`/preview`).
`/validate` is the same check Execute will run, exposed so the UI can refuse a
recipe before an approval is spent on it.

Nothing here reads a source or writes a destination: the caller passes the
sampled rows it already has from introspection. That keeps the design loop free
of connector round-trips, and keeps this router pure enough that its answers are
reproducible — the same rows and the same recipe always yield the same preview,
which is the property Validate≡Execute later depends on.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.shape_engine import ShapeEngine, ShapeRowError
from services.shape_expr import ExpressionError, compile_expression, describe_functions
from services.shape_models import MAX_STEPS, ShapeError, ShapeRecipe, describe_catalog
from services.shape_preflight import shaped_column_types
from services.shape_suggest import profile_columns, suggest_steps

router = APIRouter(prefix="/shape", tags=["Shape"])

# A preview is a design aid, not a load. Bounding it keeps an accidental paste of
# 100k rows from turning an interactive request into a job.
MAX_PREVIEW_ROWS = 2_000


class _RecipeBody(BaseModel):
    """A recipe plus the column set it must be valid against."""

    recipe: dict[str, Any] = Field(default_factory=dict)
    source_columns: list[str] = Field(default_factory=list)


class _PreviewBody(_RecipeBody):
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)
    # Declared source carriers from the catalog. A column no step wrote keeps the
    # type declared here; one the recipe wrote is re-read from the shaped values,
    # so Map decides a carrier from the rows the writer will actually be offered.
    column_types: dict[str, str] = Field(default_factory=dict)
    # Declared destination carriers, so a narrowing decimal is suggested with
    # the scale the writer will actually enforce.
    target_schema: dict[str, str] = Field(default_factory=dict)
    include_profile: bool = True
    # Multi-table: the sample belongs to one stream. The hash is still the
    # whole program (every table's steps). Preview applies only this table.
    focus_table: str = ""
    source_tables: list[str] = Field(default_factory=list)
    source_catalog: dict[str, list[str]] = Field(default_factory=dict)


class _ProfileBody(BaseModel):
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)
    source_columns: list[str] = Field(default_factory=list)
    target_schema: dict[str, str] = Field(default_factory=dict)


class _ExpressionBody(BaseModel):
    expression: str = ""
    source_columns: list[str] = Field(default_factory=list)


@router.get("/catalog")
async def shape_catalog() -> dict[str, Any]:
    """Every operation and function the engine will accept — and the limits."""
    return {
        "operations": describe_catalog(),
        "functions": describe_functions(),
        "max_steps": MAX_STEPS,
        "max_preview_rows": MAX_PREVIEW_ROWS,
        "error_policies": [
            {
                "value": "refuse",
                "label": "Fail the run and name the row",
                "detail": "Default. A value the recipe cannot compute is a decision for a human.",
            },
            {
                "value": "divert",
                "label": "Send the row to quarantine",
                "detail": "The row is kept with its reason; the rest of the load proceeds.",
            },
            {
                "value": "null",
                "label": "Write null and count it",
                "detail": "The only policy that discards information; every occurrence is counted.",
            },
        ],
        "post_load_only": {
            "operations": ["join", "lookup", "aggregate", "pivot", "unpivot", "sort", "rank", "window", "dedupe"],
            "reason": (
                "These need the whole population in one place, which a streaming "
                "read cannot provide; model them on the Transforms page after the load."
            ),
        },
    }


@router.post("/validate")
async def validate_recipe(body: _RecipeBody) -> dict[str, Any]:
    """Check a recipe and return its identity, without running it."""
    recipe = _parse(body.recipe, body.source_columns)
    return _identity(recipe)


@router.post("/expression")
async def check_expression(body: _ExpressionBody) -> dict[str, Any]:
    """Check one expression as the operator types it."""
    try:
        expression = compile_expression(
            body.expression,
            known_columns=body.source_columns or None,
        )
    except ExpressionError as exc:
        return {"valid": False, "error": str(exc)}
    return {
        "valid": True,
        "columns": sorted(expression.columns),
    }


@router.post("/profile")
async def profile_source(body: _ProfileBody) -> dict[str, Any]:
    """Profile the sampled rows and offer the steps that address what was found."""
    rows = _bounded(body.sample_rows)
    profiles = profile_columns(rows, columns=body.source_columns or None)
    return {
        "sampled_rows": len(rows),
        "columns": [p.to_dict() for p in profiles],
        "suggestions": suggest_steps(profiles, target_schema=body.target_schema),
        "sample_notice": (
            "Profiled from the sampled rows shown here. Validate re-checks the "
            "whole population before Execute."
        ),
    }


@router.post("/preview")
async def preview_recipe(body: _PreviewBody) -> dict[str, Any]:
    """Apply a recipe to the sampled rows and report exactly what it did."""
    rows = _bounded(body.sample_rows)
    columns = body.source_columns or _columns_of(rows)
    recipe, preview_note, program_hash = _recipe_for_preview(body, columns)

    engine = ShapeEngine(recipe)
    shaped: list[dict[str, Any]] = []
    refusal: dict[str, Any] | None = None
    for row in rows:
        try:
            shaped.extend(engine.apply_records(row))
        except ShapeRowError as exc:
            # A refusal is the recipe's answer for this row, and the preview's job
            # is to show it now rather than let Execute discover it at scale.
            refusal = exc.as_dict()
            break

    out_columns = list(engine.output_columns) or list(recipe.output_columns) or _columns_of(shaped or rows)
    profiles = (
        profile_columns(shaped or rows, columns=out_columns or None)
        if body.include_profile
        else []
    )
    out_types, retyped = shaped_column_types(
        out_columns,
        declared_types=body.column_types,
        touched=recipe.touched_columns,
        rows=shaped,
    )
    identity = _identity(recipe, output_columns=out_columns)
    if program_hash:
        identity["recipe_hash"] = program_hash
    return {
        "recipe": identity,
        "sampled_rows": len(rows),
        "column_types": out_types,
        "retyped_columns": retyped,
        "before": [_wire_row(r) for r in rows[:200]],
        "after": [_wire_row(r) for r in shaped[:200]],
        "effect": _wire(engine.effect.to_dict()),
        "changed_cells": _changed_cells(rows, shaped, recipe),
        "refusal": refusal,
        "shaped_profile": [p.to_dict() for p in profiles],
        "suggestions": suggest_steps(profiles, target_schema=body.target_schema)
        if body.include_profile
        else [],
        "preview_note": preview_note,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _recipe_for_preview(body: _PreviewBody, columns: list[str]) -> tuple[ShapeRecipe, str, str]:
    """The recipe to apply to this sample, and a note when other tables wait.

    A multi-table program's hash covers every stream. The sample is one
    table, so only that table's steps are applied here. Each other table is
    still parsed against its catalog columns — a step that names a missing
    column is refused before anyone approves the program.
    """
    from services.multi_stream_plan import (
        enabled_recipe_steps,
        full_recipe_hash,
        partition_shape_steps,
    )
    from services.rule_compiler.normalize import fold

    tables = [str(name).strip() for name in body.source_tables if str(name).strip()]
    tagged = any(
        str(step.get("source_table") or "").strip()
        for step in enabled_recipe_steps(body.recipe)
    )
    if len(tables) < 2 or not tagged:
        return _parse(body.recipe, columns), "", ""

    grouped, refusal = partition_shape_steps(body.recipe, tables)
    if refusal:
        raise HTTPException(status_code=400, detail=refusal)
    catalog = body.source_catalog or {}
    for name, steps in grouped.items():
        owned = _catalog_columns(catalog, name)
        if not owned:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"{name} has transform steps but its columns are not in the "
                    "source catalog, so the step cannot be proved. Reload that "
                    "table's schema."
                ),
            )
        _parse({"steps": steps}, owned)

    focus = str(body.focus_table or "").strip()
    focus_name = ""
    if focus:
        want = fold(focus)
        for name in tables:
            if fold(name) == want:
                focus_name = name
                break
        if not focus_name:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Preview table “{focus}” is not among the selected streams "
                    f"({', '.join(tables)})."
                ),
            )
    else:
        focus_name = tables[0]
    focus_steps = grouped.get(focus_name) or []
    applied = (
        _parse({"steps": focus_steps}, columns)
        if focus_steps
        else ShapeRecipe.parse({"steps": []})
    )
    # Identity is the whole program. Approving the preview approves every table.
    digest = full_recipe_hash(body.recipe)
    others = len(grouped) - (1 if focus_name in grouped else 0)
    note = (
        f"Preview shows {focus_name}. "
        + (
            f"{others} other stream(s) keep their own steps and run at Execute."
            if others
            else "Other selected streams pass through unchanged."
        )
    )
    return applied, note, digest


def _catalog_columns(catalog: dict[str, list[str]], name: str) -> list[str]:
    from services.rule_compiler.normalize import fold

    want = fold(name)
    for key, cols in catalog.items():
        if fold(str(key)) == want:
            return [str(col) for col in cols if str(col).strip()]
    return []


def _parse(payload: Any, columns: list[str]) -> ShapeRecipe:
    try:
        return ShapeRecipe.parse(payload, source_columns=columns or None)
    except ShapeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _identity(
    recipe: ShapeRecipe,
    *,
    output_columns: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "valid": True,
        "recipe_hash": recipe.recipe_hash,
        "step_count": len(recipe.enabled_steps),
        "has_active_step": recipe.has_active_step,
        "input_columns": list(recipe.input_columns),
        "output_columns": list(output_columns if output_columns is not None else recipe.output_columns),
        "summary": recipe.describe(),
        "steps": [s.to_wire() for s in recipe.steps],
    }


def _bounded(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(rows) > MAX_PREVIEW_ROWS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"a preview holds at most {MAX_PREVIEW_ROWS} rows; "
                f"{len(rows)} were sent"
            ),
        )
    return rows


def _columns_of(rows: list[dict[str, Any]]) -> list[str]:
    seen: dict[str, None] = {}
    for row in rows:
        for key in row:
            seen.setdefault(str(key), None)
    return list(seen)


def _changed_cells(
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
    recipe: ShapeRecipe,
) -> list[dict[str, Any]]:
    """Which cells the recipe changed, so the UI can highlight them.

    Only meaningful while no row was removed, since after that the two lists no
    longer line up; an active recipe reports its counts instead.
    """
    if recipe.has_active_step or len(before) != len(after):
        return []
    marks: list[dict[str, Any]] = []
    for index, (source_row, shaped_row) in enumerate(zip(before, after)):
        for column, value in shaped_row.items():
            if column not in source_row:
                marks.append({"row": index, "column": column, "kind": "added"})
            elif _render(source_row[column]) != _render(value):
                marks.append({"row": index, "column": column, "kind": "changed"})
        for column in source_row:
            if column not in shaped_row:
                marks.append({"row": index, "column": column, "kind": "removed"})
    return marks[:2000]


def _render(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _wire_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _wire(value) for key, value in row.items()}


def _wire(value: Any) -> Any:
    """JSON without losing a decimal to a float on the way out."""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _wire(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_wire(v) for v in value]
    return value
