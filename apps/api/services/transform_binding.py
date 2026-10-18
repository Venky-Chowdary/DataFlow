"""Bind a Transform Library recipe to a transfer/schedule.

A schedule never stores recipe steps twice — it stores the *binding*:
recipe id, the version it resolved to, the mode (``pin`` vs ``follow``), and
the parameter values. Resolution compiles the recipe into the canonical
``shape_recipe`` payload the engine already executes, so a bound recipe and
a hand-authored shape recipe share one path, one hash gate, one proof.

``pin``   — the version resolved at bind time is baked in. A later library
            edit never reaches the schedule.
``follow``— each run re-resolves the latest version. If that changes the
            recipe the Validate-approved hash no longer matches and the run
            refuses, naming the drift — an upgrade is never silent.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.transform_recipe import TransformRecipe, compile_recipe

BINDING_MODES = frozenset({"pin", "follow"})


@dataclass
class ResolvedBinding:
    recipe: TransformRecipe
    payload: dict[str, Any]
    recipe_hash: str
    bound_version: int


def resolve_recipe_binding(
    recipe_id: str,
    *,
    version: int | None = None,
    params: dict[str, Any] | None = None,
    source_columns: list[str] | None = None,
    workspace_id: str | None = None,
) -> ResolvedBinding:
    """Resolve ``recipe_id`` (+version/params) into an engine-ready payload.

    Raises ValueError naming the exact refusal — draft recipe, missing
    version, unbound parameter, contract drift, or an op that cannot compile.
    Callers surface it as a 422/refusal; a recipe that cannot resolve never
    reaches a run.
    """
    from services.transform_library import get_transform_library

    recipe = get_transform_library().get(
        recipe_id, workspace_id=workspace_id, version=version
    )
    if recipe is None:
        raise ValueError(
            f"Transform recipe {recipe_id}"
            + (f" version {version}" if version else "")
            + " not found — pick a transform from the Library dropdown, "
            "or create it first"
        )
    if recipe.status != "active":
        issues = "; ".join(recipe.issues) or "not validated"
        raise ValueError(
            f"Transform recipe '{recipe.name}' is a draft and cannot bind "
            f"({issues}) — fix and validate it in the Library first"
        )
    if not recipe.enabled:
        raise ValueError(
            f"Transform recipe '{recipe.name}' is disabled — enable it in "
            "the Library before binding it to a transfer"
        )
    payload, shape = compile_recipe(
        recipe, source_columns=source_columns, params=params
    )
    return ResolvedBinding(
        recipe=recipe,
        payload=payload,
        recipe_hash=shape.recipe_hash,
        bound_version=recipe.version,
    )


def apply_schedule_binding(
    payload: dict[str, Any],
    *,
    source_columns: list[str] | None = None,
    workspace_id: str | None = None,
) -> dict[str, Any]:
    """Resolve a schedule create/update payload's transform binding.

    Mutates and returns ``payload``: on success ``shape_recipe`` carries the
    compiled steps and ``transform_recipe_version`` the version actually
    bound. An empty ``transform_recipe_id`` clears the binding (and the
    shape recipe it produced) only when the caller explicitly sent it.
    """
    recipe_id = str(payload.get("transform_recipe_id") or "").strip()
    if not recipe_id:
        return payload
    mode = str(payload.get("transform_recipe_version_mode") or "pin").strip().lower()
    if mode not in BINDING_MODES:
        raise ValueError(
            f"transform_recipe_version_mode must be one of {sorted(BINDING_MODES)}"
        )
    version = int(payload.get("transform_recipe_version") or 0) or None
    bound = resolve_recipe_binding(
        recipe_id,
        version=version if mode == "pin" else None,
        params=payload.get("transform_recipe_params"),
        source_columns=source_columns,
        workspace_id=workspace_id,
    )
    payload["shape_recipe"] = bound.payload
    payload["transform_recipe_version"] = bound.bound_version
    payload["transform_recipe_version_mode"] = mode
    return payload
