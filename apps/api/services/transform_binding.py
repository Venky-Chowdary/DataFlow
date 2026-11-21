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


def _recipe_steps(shape: Any) -> list[Any]:
    if not isinstance(shape, dict):
        return []
    steps = shape.get("steps")
    return list(steps) if isinstance(steps, list) else []


def _read_column_names(columns: list[str] | None) -> list[str] | None:
    names = [str(name).strip() for name in (columns or []) if str(name).strip()]
    return names or None


def _program_hash(steps: list[Any]) -> str:
    """Identity of staged steps. Empty when they are not a runnable program."""
    if not steps:
        return ""
    try:
        from services.shape_models import ShapeRecipe

        return ShapeRecipe.parse({"steps": steps}).recipe_hash
    except Exception:
        return ""


def compile_transfer_recipe(
    request: Any,
    *,
    read_columns: list[str] | None = None,
) -> bool:
    """Compile ``transform_recipe_id`` into ``shape_recipe``, or refuse.

    Execute reads ``shape_recipe`` only. A run that stored the library id and
    left ``shape_recipe`` empty shipped the raw rows under a verified badge
    (D400). Pin mode bakes the version resolved here. A library edit between
    stage and confirm changes the hash and is refused — the new recipe is
    not applied silently. An empty id does nothing. Returns True when this
    call attached the compiled recipe.
    """
    recipe_id = str(getattr(request, "transform_recipe_id", "") or "").strip()
    if not recipe_id:
        return False
    mode = str(getattr(request, "transform_recipe_version_mode", "") or "pin").strip().lower() or "pin"
    if mode not in BINDING_MODES:
        raise ValueError(
            f"transform_recipe_version_mode must be one of {sorted(BINDING_MODES)}"
        )
    version = int(getattr(request, "transform_recipe_version", 0) or 0) or None
    params = getattr(request, "transform_recipe_params", None) or None
    # The recipe runs on the read, before Map. Mapping sources are the image
    # Map saw — after rename note→memo that source is "memo" — so checking the
    # recipe against them refuses the rename of "note" that preview just ran
    # (D400). Only an explicit read-column list (Validate's source columns)
    # is a column contract. Execute leaves it empty and the live read checks
    # the columns when the recipe is applied.
    bound = resolve_recipe_binding(
        recipe_id,
        version=version if mode == "pin" else None,
        params=params if isinstance(params, dict) else None,
        source_columns=_read_column_names(read_columns),
        workspace_id=str(getattr(request, "workspace_id", "") or "") or None,
    )
    existing = _recipe_steps(getattr(request, "shape_recipe", None))
    approved = str(getattr(request, "approved_shape_recipe_hash", "") or "").strip()
    if existing:
        staged = _program_hash(existing)
        if staged and staged == bound.recipe_hash:
            # Preview already compiled this pinned version. Keep those steps.
            request.transform_recipe_version = bound.bound_version
            request.transform_recipe_version_mode = mode
            if not approved:
                request.approved_shape_recipe_hash = bound.recipe_hash
            return False
        if approved and approved == bound.recipe_hash:
            return False
        if approved and approved != bound.recipe_hash:
            raise ValueError(
                "Transform recipe "
                f"{recipe_id} changed between stage and confirm "
                f"(approved {approved}, library now {bound.recipe_hash}). "
                "The new recipe was not applied. Stage the run again so the "
                "plan and the execute recipe are the same."
            )
        raise ValueError(
            f"This run has shape_recipe steps and transform_recipe_id {recipe_id}, "
            "and they are not the same compiled recipe. Put the steps in the "
            "library recipe, or drop one of them, then stage the run again."
        )
    request.shape_recipe = bound.payload
    request.transform_recipe_version = bound.bound_version
    request.transform_recipe_version_mode = mode
    request.approved_shape_recipe_hash = bound.recipe_hash
    return True


def compile_preflight_recipe(body: Any) -> None:
    """Validate judges the library recipe Execute will run (D400, R8).

    ``body`` is a preflight request: its ``shape_recipe`` becomes the compiled
    recipe, so the gates see the shaped image. A recipe that cannot compile
    (a draft, a missing id, steps mixed with another recipe) raises
    ValueError here, before Confirm, with the same text Execute would give.

    The recipe is looked up in the caller's workspace, as Execute does: a
    preflight body carries none, and an unscoped lookup compiled another
    workspace's recipe (R11) that Execute then refused (R8). The body is never
    asked for one, because the caller writes the body.
    """
    from types import SimpleNamespace

    from services.workspace_filter import current_workspace_stamp

    probe = SimpleNamespace(
        transform_recipe_id=str(getattr(body, "transform_recipe_id", "") or ""),
        transform_recipe_version=int(getattr(body, "transform_recipe_version", 0) or 0),
        transform_recipe_version_mode=str(getattr(body, "transform_recipe_version_mode", "") or "pin"),
        transform_recipe_params=getattr(body, "transform_recipe_params", None),
        mappings=getattr(body, "mappings", None),
        workspace_id=current_workspace_stamp(for_write=False) or "",
        shape_recipe=getattr(body, "shape_recipe", None),
        approved_shape_recipe_hash=str(getattr(body, "approved_shape_recipe_hash", "") or ""),
    )
    # ``columns`` is the source table. A rename's old name is still there;
    # Map's source names are the names the recipe emits.
    if compile_transfer_recipe(probe, read_columns=list(getattr(body, "columns", None) or [])):
        body.shape_recipe = probe.shape_recipe


def recipe_ignored_failure(request: Any) -> str:
    """Why a run that named a library recipe must not be called verified.

    The proof checks recipe output. An id with no compiled steps means the
    raw rows would be certified (D400). Empty when the recipe is absent or
    compiled.
    """
    recipe_id = str(getattr(request, "transform_recipe_id", "") or "").strip()
    if not recipe_id or _recipe_steps(getattr(request, "shape_recipe", None)):
        return ""
    return (
        f"Transform recipe {recipe_id} was not compiled into this run "
        "(shape_recipe has no steps). The destination would hold the raw "
        "rows, so the run is refused instead of verified."
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


def compile_and_record_recipe(request: Any, job_id: str, store: Any) -> str:
    """Compile a saved recipe into ``request`` and stamp it on the job (D400).

    Returns the refusal text when the recipe cannot run as asked (a draft, a pin
    drift, a second recipe, or a recipe the run would ignore), else ``""``. A
    compiled recipe is written on the job record so Resume, replay and the
    proof read the same shape the run used; a failure to store it is logged and
    never turns a runnable transfer into a refusal.
    """
    import logging

    try:
        compiled = compile_transfer_recipe(request)
    except ValueError as exc:
        return str(exc)
    refusal = recipe_ignored_failure(request)
    if compiled and not refusal:
        try:
            store.update_job_fields(
                job_id,
                {
                    "transfer_request.shape_recipe": request.shape_recipe,
                    "transfer_request.transform_recipe_version": request.transform_recipe_version,
                    "transfer_request.transform_recipe_version_mode": request.transform_recipe_version_mode,
                    "transfer_request.approved_shape_recipe_hash": request.approved_shape_recipe_hash,
                },
                touch_updated_at=False,
            )
        except Exception:
            logging.getLogger(__name__).exception(
                "Could not store the compiled recipe on %s", job_id
            )
    return refusal
