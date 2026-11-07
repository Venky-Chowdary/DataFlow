"""Per-column transform pipelines named in chat (MCP ``column_transforms``).

A ``column_transforms`` value names either ONE Map transform —
``{"email": "hash_pii"}``, stamped on the mapping as before — or an ordered
pipeline of them::

    {"name": ["trim", "upper"], "amount": "trim, decimal",
     "ts": ["datetime", "assume_timezone:UTC"]}

A pipeline is a recipe, so it compiles to the canonical ``shape_recipe`` with
the Transform Library's own step compiler and runs on the read through
:class:`services.shape_engine.ShapeEngine` — the program Studio's Transform
canvas and a bound Library recipe run. Validate judges its shaped image,
Execute shapes the population, Gate-8 re-applies and proves it, all from one
recipe and one hash. There is no second composition engine for chat.

The base refused every pipeline as "not a transform the engine applies", so
an operator could not run more than one step on a column from MCP at all.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

#: Separators an operator types between bare steps: ``trim, upper``,
#: ``trim | upper``, ``trim -> upper``. A parameterized step (``replace:a:b``)
#: may itself contain them, so only an all-bare chain is split; anything with
#: a parameter must use the list form.
_CHAIN_SPLIT = re.compile(r"\s*(?:->|,|\|)\s*")


def chain_steps(spec: Any) -> list[str] | None:
    """The ordered steps ``spec`` names, or ``None`` for a single Map transform."""
    from services.transform_engine import (
        is_parameterized_transform,
        normalize_engine_transform,
    )

    if isinstance(spec, (list, tuple)):
        return [str(item).strip() for item in spec if str(item or "").strip()]
    text = str(spec or "").strip()
    if not text or normalize_engine_transform(text) is not None:
        return None
    parts = [p for p in _CHAIN_SPLIT.split(text) if p]
    if len(parts) < 2:
        return None
    if all(
        normalize_engine_transform(p) is not None and not is_parameterized_transform(p)
        for p in parts
    ):
        return parts
    return None


def split_column_transforms(
    column_transforms: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """``(single Map transforms, pipelines)`` keyed as the operator wrote them."""
    singles: dict[str, Any] = {}
    chains: dict[str, list[str]] = {}
    for key, spec in (column_transforms or {}).items():
        steps = chain_steps(spec)
        if steps is None:
            singles[key] = spec
        else:
            chains[str(key)] = steps
    return singles, chains


def compile_column_chains(
    chains: Mapping[str, Sequence[str]],
    source_columns: Sequence[str],
) -> tuple[dict[str, Any], str]:
    """``(shape_recipe payload, refusal)`` — refusal is ``""`` when it compiles.

    Keys resolve case-insensitively to SOURCE columns: a pipeline runs on the
    read, before Map. Every step must compile to a shape step that does what
    the Map transform of the same name does; one that cannot is refused by
    name rather than half-applied.
    """
    from services.shape_models import ShapeError, ShapeRecipe
    from services.transform_engine import KNOWN_TRANSFORMS, normalize_engine_transform
    from services.transform_recipe import RecipeStep, _step_to_shape_wire

    by_lower = {str(c).strip().lower(): str(c) for c in source_columns}
    steps: list[dict[str, Any]] = []
    for key, ops in chains.items():
        column = by_lower.get(str(key).strip().lower())
        if column is None:
            return {}, (
                f"column_transforms key {key!r} names no source column (source: "
                f"{', '.join(list(source_columns)[:20])}). A multi-step transform "
                "runs on the read, before Map, so it is keyed by the source name."
            )
        if not ops:
            return {}, f"column_transforms[{key!r}] is an empty list — name at least one step."
        for op in ops:
            if normalize_engine_transform(op) is None:
                return {}, (
                    f"column_transforms[{key!r}] step {op!r} is not a transform the "
                    f"engine applies. Use one of: {', '.join(sorted(KNOWN_TRANSFORMS))} "
                    "or replace:/default:/assume_timezone:."
                )
            try:
                steps.append(_step_to_shape_wire(RecipeStep(column=column, op=op)))
            except ValueError as exc:
                return {}, f"column_transforms[{key!r}] step {op!r}: {exc}"
    payload: dict[str, Any] = {"steps": steps}
    try:
        ShapeRecipe.parse(payload, source_columns=list(source_columns))
    except ShapeError as exc:
        return {}, f"column_transforms pipeline cannot run: {exc}"
    return payload, ""
