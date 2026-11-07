"""Transform Library — standalone, reusable row-level transform recipes.

The library is source-of-truth (A): operators author named, versioned recipes
here once; a transfer's Transform step then selects them by id+version (B).
These endpoints manage definitions, versions, where-used, and preview them on
sample rows — execution runs through the canonical shape engine, never a
second transform path.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field, field_validator

from services.transform_engine import KNOWN_TRANSFORMS
from services.transform_library import get_transform_library
from services.transform_recipe import (
    ON_ERROR_MODES,
    RecipeStep,
    TransformRecipe,
    preview_recipe,
)
from services.transform_recipe_templates import clone_template, list_templates
from services.workspace_filter import WorkspaceFilter
from services.workspace_access import (
    assert_resource_workspace,
    resolve_read_workspace,
    resolve_write_workspace,
    tenant_scope,
)

router = APIRouter(prefix="/transform-library", tags=["Transform Library"])

OnError = Literal["fail", "keep_original"]
PREVIEW_ROW_LIMIT = 100


class StepPayload(BaseModel):
    column: str = ""
    op: str = Field(..., min_length=1)
    on_error: OnError = "fail"
    enabled: bool = True

    @field_validator("on_error", mode="before")
    @classmethod
    def _engine_word_is_fail(cls, value: Any) -> Any:
        """``refuse`` is the transfer engine's word for ``fail`` (QA round 11, T9)."""
        text = str(value or "").strip().lower()
        return "fail" if text in ("", "refuse") else text
    options: dict[str, Any] = Field(default_factory=dict)
    output_column: str = ""


class ParameterSpec(BaseModel):
    default: Any = None
    description: str = ""


class RecipeCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    steps: list[StepPayload] = Field(..., min_length=1)
    description: str = ""
    enabled: bool = True
    parameters: dict[str, ParameterSpec] = Field(default_factory=dict)
    input_columns: list[str] = Field(default_factory=list)
    output_columns: list[str] = Field(default_factory=list)


class RecipeUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=120)
    steps: Optional[list[StepPayload]] = None
    description: Optional[str] = None
    enabled: Optional[bool] = None
    parameters: Optional[dict[str, ParameterSpec]] = None
    input_columns: Optional[list[str]] = None
    output_columns: Optional[list[str]] = None
    """Optimistic concurrency: client must send the version last loaded."""
    expected_version: Optional[int] = Field(default=None, ge=0)


class PreviewRequest(BaseModel):
    rows: list[dict[str, Any]] = Field(default_factory=list, max_length=PREVIEW_ROW_LIMIT)
    steps: Optional[list[StepPayload]] = None
    """When set, preview this step list instead of the stored recipe's."""
    params: Optional[dict[str, Any]] = None
    """Parameter values overriding the recipe's declared defaults."""
    per_step: bool = False
    """When true, each row also carries the value after every step."""


class UpgradePreviewPayload(BaseModel):
    """Version-upgrade preview request: which bound version moves to which.

    ``bound_version`` defaults to the consuming schedule's pinned version
    when ``schedule_id`` is given, else the recipe's previous version;
    ``target_version`` defaults to the recipe's latest.
    """

    target_version: Optional[int] = Field(default=None, ge=1)
    bound_version: Optional[int] = Field(default=None, ge=1)
    schedule_id: Optional[str] = None


def _to_steps(payloads: list[StepPayload]) -> list[RecipeStep]:
    return [
        RecipeStep(
            column=p.column,
            op=p.op,
            on_error=p.on_error,
            enabled=p.enabled,
            options=dict(p.options),
            output_column=p.output_column,
        )
        for p in payloads
    ]


def _to_parameters(
    specs: dict[str, ParameterSpec],
) -> dict[str, dict[str, Any]]:
    return {k: {"default": v.default, "description": v.description} for k, v in specs.items()}


@router.get("/")
def list_recipes(
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> list[dict[str, Any]]:
    scope = tenant_scope(request, workspace_id)
    return [r.to_dict() for r in get_transform_library().list() if scope.allows(r.workspace_id)]


@router.get("/ops")
def list_ops() -> dict[str, Any]:
    """The ops a recipe step may use, so the selector UI enumerates them."""
    from services.shape_expr import describe_functions
    from services.shape_models import STEP_CATALOG, describe_catalog

    return {
        "ops": sorted(STEP_CATALOG),
        "ops_detail": describe_catalog(),
        "engine_aliases": sorted(KNOWN_TRANSFORMS),
        "on_error_modes": sorted(ON_ERROR_MODES),
        "functions": describe_functions(),
    }


@router.get("/templates")
def get_templates() -> list[dict[str, Any]]:
    """Clonable starter recipes — the Quickstart-template layer."""
    return list_templates()


@router.post("/templates/{key}/clone", status_code=201)
def clone(
    key: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    ws = resolve_write_workspace(request, workspace_id)
    recipe = clone_template(key, workspace_id=ws or "")
    if recipe is None:
        raise HTTPException(status_code=404, detail="Transform template not found")
    return get_transform_library().save(recipe).to_dict()


@router.get("/{recipe_id}")
def get_recipe(
    recipe_id: str,
    request: Request,
    version: Optional[int] = None,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    ws = resolve_read_workspace(request, workspace_id)
    recipe = get_transform_library().get(
        recipe_id, workspace_id=ws, version=version
    )
    if not recipe:
        raise HTTPException(
            status_code=404,
            detail=(
                "Transform recipe not found"
                if version is None
                else f"Transform recipe version {version} not found"
            ),
        )
    assert_resource_workspace(request, recipe.workspace_id or "")
    return recipe.to_dict()


@router.get("/{recipe_id}/versions")
def list_versions(
    recipe_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    ws = resolve_read_workspace(request, workspace_id)
    store = get_transform_library()
    recipe = store.get(recipe_id, workspace_id=ws)
    if not recipe:
        raise HTTPException(status_code=404, detail="Transform recipe not found")
    assert_resource_workspace(request, recipe.workspace_id or "")
    versions = [recipe.version]
    for v in range(1, recipe.version):
        if store.get(recipe_id, workspace_id=ws, version=v) is not None:
            versions.append(v)
    return {"recipe_id": recipe_id, "versions": sorted(set(versions))}


def _schedule_users(
    recipe_id: str, latest_version: int, workspace_filter: WorkspaceFilter | None = None
) -> list[dict[str, Any]]:
    """Schedules that bind this recipe — the where-used scan.

    Scope is schedules only: a schedule re-resolves a follow-latest recipe at
    run time, so deleting the recipe would break the next fire. One-off
    transfers snapshot compiled steps at apply, so nothing else needs
    scanning — the scope is named on the response so a client never reads it
    as "everything".
    """
    from services.schedule_store import list_schedules

    users: list[dict[str, Any]] = []
    for sched in list_schedules(workspace_filter):
        if getattr(sched, "transform_recipe_id", "") != recipe_id:
            continue
        users.append(
            {
                "schedule_id": sched.id,
                "schedule_name": getattr(sched, "name", "") or sched.id,
                "bound_version": int(getattr(sched, "transform_recipe_version", 0) or 0),
                "version_mode": getattr(sched, "transform_recipe_version_mode", "pin"),
                "latest_version": latest_version,
            }
        )
    return users


@router.get("/{recipe_id}/where-used")
def where_used(
    recipe_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    """Which schedules bind this recipe — the edit-impact view.

    A library edit must show who consumes it before the edit lands; pinning
    a version protects bound transfers, so this list is what makes the
    version-pin choice informed rather than blind.
    """
    ws = resolve_read_workspace(request, workspace_id)
    recipe = get_transform_library().get(recipe_id, workspace_id=ws)
    if not recipe:
        raise HTTPException(status_code=404, detail="Transform recipe not found")
    assert_resource_workspace(request, recipe.workspace_id or "")
    return {
        "recipe_id": recipe_id,
        "used_by": _schedule_users(
            recipe_id, recipe.version, tenant_scope(request, workspace_id).filter
        ),
        "scope": "schedules",
    }


@router.post("/", status_code=201)
def create_recipe(
    body: RecipeCreate,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    ws = resolve_write_workspace(request, workspace_id)
    try:
        recipe = TransformRecipe(
            name=body.name,
            steps=_to_steps(body.steps),
            description=body.description,
            enabled=body.enabled,
            parameters=_to_parameters(body.parameters),
            input_columns=tuple(body.input_columns),
            output_columns=tuple(body.output_columns),
            workspace_id=ws or "",
        )
        return get_transform_library().save(recipe).to_dict()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.patch("/{recipe_id}")
def update_recipe(
    recipe_id: str,
    body: RecipeUpdate,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    ws = resolve_write_workspace(request, workspace_id)
    store = get_transform_library()
    recipe = store.get(recipe_id, workspace_id=ws)
    if not recipe:
        raise HTTPException(status_code=404, detail="Transform recipe not found")
    assert_resource_workspace(request, recipe.workspace_id or "", write=True)

    if body.expected_version is not None and int(body.expected_version) != int(
        recipe.version or 0
    ):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Transform version conflict: expected {body.expected_version}, "
                f"current is {recipe.version}. Reload and retry."
            ),
        )

    try:
        if body.name is not None:
            recipe.name = body.name
        if body.steps is not None:
            recipe.steps = _to_steps(body.steps)
        if body.description is not None:
            recipe.description = body.description
        if body.enabled is not None:
            recipe.enabled = body.enabled
        if body.parameters is not None:
            recipe.parameters = _to_parameters(body.parameters)
        if body.input_columns is not None:
            recipe.input_columns = tuple(body.input_columns)
        if body.output_columns is not None:
            recipe.output_columns = tuple(body.output_columns)
        return store.save(recipe).to_dict()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.delete("/{recipe_id}", status_code=204, response_class=Response)
def delete_recipe(
    recipe_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> Response:
    ws = resolve_write_workspace(request, workspace_id)
    store = get_transform_library()
    recipe = store.get(recipe_id, workspace_id=ws)
    if not recipe:
        raise HTTPException(status_code=404, detail="Transform recipe not found")
    assert_resource_workspace(request, recipe.workspace_id or "", write=True)
    bound = _schedule_users(recipe_id, recipe.version)
    if bound:
        names = ", ".join(u["schedule_name"] for u in bound[:10])
        raise HTTPException(
            status_code=409,
            detail={
                "error": "recipe_in_use",
                "message": (
                    f"Recipe is bound to {len(bound)} schedule(s) — unbind them "
                    "before deleting. One-off transfers already snapshot the "
                    "compiled steps, so only schedules are listed."
                ),
                "scope": "schedules",
                "used_by": bound,
                "names": names,
            },
        )
    if not store.delete(recipe_id):
        raise HTTPException(status_code=404, detail="Transform recipe not found")
    return Response(status_code=204)


class CompileRequest(BaseModel):
    """Parameter values overriding the recipe's declared defaults."""
    params: Optional[dict[str, Any]] = None
    """Source column names to check the recipe's input contract against."""
    source_columns: Optional[list[str]] = None


@router.post("/{recipe_id}/compile")
def compile_for_transfer(
    recipe_id: str,
    body: CompileRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
    version: Optional[int] = None,
) -> dict[str, Any]:
    """Resolve a library recipe into the engine-ready shape payload.

    The in-flow Transform dropdown uses this: pick recipe + version + params
    → get the exact steps the transfer will run and the identity hash that
    pins them. A draft recipe or an unbound parameter refuses here, at
    design time — never mid-run.
    """
    ws = resolve_read_workspace(request, workspace_id)
    recipe = get_transform_library().get(
        recipe_id, workspace_id=ws, version=version
    )
    if not recipe:
        raise HTTPException(status_code=404, detail="Transform recipe not found")
    assert_resource_workspace(request, recipe.workspace_id or "")
    if recipe.status != "active":
        raise HTTPException(
            status_code=409,
            detail=(
                f"Transform recipe '{recipe.name}' is a draft and cannot run — "
                "fix and validate it in the Library first"
            ),
        )
    if not recipe.enabled:
        raise HTTPException(
            status_code=409,
            detail=f"Transform recipe '{recipe.name}' is disabled",
        )
    try:
        from services.transform_recipe import compile_recipe

        payload, shape = compile_recipe(
            recipe, source_columns=body.source_columns, params=body.params
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {
        "recipe_id": recipe.id,
        "recipe_name": recipe.name,
        "bound_version": recipe.version,
        "shape_recipe": payload,
        "recipe_hash": shape.recipe_hash,
        "parameters": recipe.parameters,
        "input_columns": list(recipe.input_columns),
        "output_columns": list(shape.output_columns),
    }


@router.post("/{recipe_id}/upgrade-preview")
def upgrade_preview(
    recipe_id: str,
    body: UpgradePreviewPayload,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    """Per-step diff a version upgrade would apply — B3 upgrade path.

    Editing a shared recipe creates a new version; consumers pinned to an
    older one must opt in explicitly. This endpoint is the opt-in surface:
    it returns the exact per-step changes between the bound version and
    the target BEFORE anything re-pins, plus the schedules that bind the
    recipe. Nothing mutates — the client re-pins the schedule only after
    reviewing this diff.
    """
    ws = resolve_read_workspace(request, workspace_id)
    store = get_transform_library()
    recipe = store.get(recipe_id, workspace_id=ws)
    if not recipe:
        raise HTTPException(status_code=404, detail="Transform recipe not found")
    assert_resource_workspace(request, recipe.workspace_id or "")

    from services.schedule_store import get_schedule

    bound_version = body.bound_version
    if bound_version is None and body.schedule_id:
        sched = get_schedule(body.schedule_id)
        if not sched:
            raise HTTPException(status_code=404, detail="Schedule not found")
        bound_version = int(
            getattr(sched, "transform_recipe_version", 0) or 0
        ) or None
    if bound_version is None:
        bound_version = max(1, int(recipe.version or 1) - 1)
    target_version = body.target_version or int(recipe.version or 1)

    old = store.get(recipe_id, workspace_id=ws, version=bound_version)
    if old is None:
        raise HTTPException(
            status_code=404,
            detail=f"Transform recipe version {bound_version} not found",
        )
    new = store.get(recipe_id, workspace_id=ws, version=target_version)
    if new is None:
        raise HTTPException(
            status_code=404,
            detail=f"Transform recipe version {target_version} not found",
        )

    from services.transform_library import recipe_diff_hash, recipe_step_diff

    step_diff = recipe_step_diff(
        [st.to_dict() for st in old.steps],
        [st.to_dict() for st in new.steps],
    )
    return {
        "recipe_id": recipe_id,
        "recipe_name": recipe.name,
        "from_version": bound_version,
        "to_version": target_version,
        "step_diff": step_diff,
        "diff_hash": recipe_diff_hash(
            recipe_id, bound_version, target_version, step_diff
        ),
        "rollback": int(target_version) < int(bound_version),
        "enabled_changed": bool(old.enabled) != bool(new.enabled),
        "used_by": _schedule_users(
            recipe_id, int(recipe.version or 1), tenant_scope(request, workspace_id).filter
        ),
        "mutates_nothing": True,
    }


@router.post("/{recipe_id}/preview")
def preview(
    recipe_id: str,
    body: PreviewRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    ws = resolve_read_workspace(request, workspace_id)
    recipe = get_transform_library().get(recipe_id, workspace_id=ws)
    if not recipe:
        raise HTTPException(status_code=404, detail="Transform recipe not found")
    assert_resource_workspace(request, recipe.workspace_id or "")
    if body.steps is not None:
        draft = TransformRecipe(
            name=recipe.name,
            steps=_to_steps(body.steps),
            parameters=recipe.parameters,
        )
        recipe = draft
    try:
        return preview_recipe(
            recipe, body.rows, params=body.params, per_step=body.per_step
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
