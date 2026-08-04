"""Post-load SQL transformation projects — CRUD, plan preview, and run."""

from __future__ import annotations

from typing import Any, Literal, Optional

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field

from services.transform_models import (
    DataTest,
    TransformDefinitionError,
    TransformModel,
    build_plan,
)
from services.transform_store import TransformProject, get_transform_store
from services.workspace_access import (
    assert_resource_workspace,
    resolve_read_workspace,
    resolve_write_workspace,
)

router = APIRouter(prefix="/transforms", tags=["Transformations"])

Materialization = Literal["view", "table", "incremental", "ephemeral"]
IncrementalStrategy = Literal["merge", "append", "delete_insert"]
TestType = Literal["unique", "not_null", "accepted_values", "relationships", "positive"]
Severity = Literal["error", "warn"]


class DataTestPayload(BaseModel):
    test_type: TestType
    column: str = ""
    severity: Severity = "error"
    values: list[str] = Field(default_factory=list)
    to_model: str = ""
    to_column: str = ""


class ModelPayload(BaseModel):
    name: str = Field(..., min_length=1, max_length=63)
    sql: str = Field(..., min_length=1)
    materialization: Materialization = "view"
    description: str = ""
    unique_key: str = ""
    incremental_strategy: IncrementalStrategy = "merge"
    tests: list[DataTestPayload] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    enabled: bool = True


class ProjectCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    destination_connector_id: str = Field(..., min_length=1)
    contract_id: str = ""
    schema_name: str = Field(default="", alias="schema")
    models: list[ModelPayload] = Field(default_factory=list)
    enabled: bool = True
    run_after_transfer: bool = True
    trigger_tables: list[str] = Field(default_factory=list)
    description: str = ""

    model_config = {"populate_by_name": True}


class ProjectUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=120)
    destination_connector_id: Optional[str] = None
    contract_id: Optional[str] = None
    schema_name: Optional[str] = Field(default=None, alias="schema")
    models: Optional[list[ModelPayload]] = None
    enabled: Optional[bool] = None
    run_after_transfer: Optional[bool] = None
    trigger_tables: Optional[list[str]] = None
    description: Optional[str] = None
    """Optimistic concurrency: client must send the version last loaded."""
    expected_version: Optional[int] = Field(default=None, ge=0)

    model_config = {"populate_by_name": True}


class PlanPreviewRequest(BaseModel):
    """Plan a model set without persisting it, so the editor can show the DAG."""

    models: list[ModelPayload] = Field(default_factory=list)
    dialect: str = "postgresql"
    schema_name: str = Field(default="", alias="schema")

    model_config = {"populate_by_name": True}


def _to_models(payloads: list[ModelPayload]) -> list[TransformModel]:
    """Convert API payloads to domain models.

    ``TransformModel.__post_init__`` validates, so a malformed model raises
    here rather than reaching the store or the warehouse.
    """
    return [
        TransformModel(
            name=p.name,
            sql=p.sql,
            materialization=p.materialization,
            description=p.description,
            unique_key=p.unique_key,
            incremental_strategy=p.incremental_strategy,
            tests=[
                DataTest(
                    test_type=t.test_type,
                    column=t.column,
                    severity=t.severity,
                    values=list(t.values),
                    to_model=t.to_model,
                    to_column=t.to_column,
                )
                for t in p.tests
            ],
            tags=list(p.tags),
            enabled=p.enabled,
        )
        for p in payloads
    ]


def _project_response(project: TransformProject) -> dict[str, Any]:
    payload = project.to_dict()
    try:
        payload["plan"] = build_plan(project.models).to_dict()
    except TransformDefinitionError as exc:
        # A stored project can become unplannable if it was written before a
        # validation rule existed. Say so instead of 500ing the whole list.
        payload["plan"] = {"error": str(exc)}
    return payload


@router.get("/")
def list_projects(
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> list[dict[str, Any]]:
    ws = resolve_read_workspace(request, workspace_id)
    return [_project_response(p) for p in get_transform_store().list(ws)]


@router.get("/{project_id}")
def get_project(
    project_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    ws = resolve_read_workspace(request, workspace_id)
    project = get_transform_store().get(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Transformation project not found")
    assert_resource_workspace(request, project.workspace_id or "")
    return _project_response(project)


@router.post("/", status_code=201)
def create_project(
    body: ProjectCreate,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    ws = resolve_write_workspace(request, workspace_id)
    try:
        project = TransformProject(
            name=body.name,
            destination_connector_id=body.destination_connector_id,
            contract_id=(body.contract_id or "").strip(),
            schema=body.schema_name,
            models=_to_models(body.models),
            enabled=body.enabled,
            run_after_transfer=body.run_after_transfer,
            trigger_tables=list(body.trigger_tables),
            description=body.description,
            workspace_id=ws or "",
        )
        return _project_response(get_transform_store().save(project))
    except (TransformDefinitionError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.patch("/{project_id}")
def update_project(
    project_id: str,
    body: ProjectUpdate,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    ws = resolve_write_workspace(request, workspace_id)
    store = get_transform_store()
    project = store.get(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Transformation project not found")
    assert_resource_workspace(request, project.workspace_id or "")

    if body.expected_version is not None and int(body.expected_version) != int(project.version or 0):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Transform version conflict: expected {body.expected_version}, "
                f"current is {project.version}. Reload and retry."
            ),
        )

    try:
        if body.name is not None:
            project.name = body.name
        if body.destination_connector_id is not None:
            project.destination_connector_id = body.destination_connector_id
        if body.contract_id is not None:
            project.contract_id = (body.contract_id or "").strip()
        if body.schema_name is not None:
            project.schema = body.schema_name
        if body.models is not None:
            project.models = _to_models(body.models)
        if body.enabled is not None:
            project.enabled = body.enabled
        if body.run_after_transfer is not None:
            project.run_after_transfer = body.run_after_transfer
        if body.trigger_tables is not None:
            project.trigger_tables = list(body.trigger_tables)
        if body.description is not None:
            project.description = body.description
        return _project_response(store.save(project))
    except (TransformDefinitionError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.delete("/{project_id}", status_code=204, response_class=Response)
def delete_project(
    project_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> Response:
    resolve_write_workspace(request, workspace_id)
    store = get_transform_store()
    project = store.get(project_id)
    if project:
        assert_resource_workspace(request, project.workspace_id or "")
    if not store.delete(project_id):
        raise HTTPException(status_code=404, detail="Transformation project not found")
    return Response(status_code=204)


@router.post("/plan")
def preview_plan(body: PlanPreviewRequest) -> dict[str, Any]:
    """Resolve the DAG and compile SQL without touching a warehouse.

    This is what makes the editor trustworthy: an operator sees the exact
    statements and the execution order before anything runs, and a dependency
    cycle is a 422 at author time rather than a half-applied run later.
    """
    try:
        models = _to_models(body.models)
        plan = build_plan(models)
    except TransformDefinitionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    from services.transform_runner import (
        TransformRunner,
        UnsupportedMaterializationError,
    )

    runner = TransformRunner(
        {"type": body.dialect, "schema": body.schema_name},
        dialect=body.dialect,
        schema=body.schema_name,
        dry_run=True,
    )
    compiled: list[dict[str, Any]] = []
    for name in plan.order:
        model = plan.models[name]
        entry: dict[str, Any] = {
            "name": name,
            "materialization": model.materialization,
            "relation": runner.relation_for(name) if model.is_materialized else "",
            "strategy": runner.physical_strategy(model),
            "refs": model.refs,
            "sources": model.sources,
        }
        try:
            entry["statements"] = runner.build_statements(model, plan.models)
        except UnsupportedMaterializationError as exc:
            # Report per-model rather than failing the whole preview: the
            # operator needs to see which model the dialect cannot express.
            entry["statements"] = []
            entry["error"] = str(exc)
        entry["tests"] = [
            {
                "test_type": t.test_type,
                "column": t.column,
                "severity": t.severity,
                "sql": runner.build_test_sql(model, t) if model.is_materialized else "",
            }
            for t in model.tests
        ]
        compiled.append(entry)

    return {"plan": plan.to_dict(), "models": compiled, "dialect": runner.dialect}


@router.post("/{project_id}/run")
def run_project(
    project_id: str,
    request: Request,
    dry_run: bool = False,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    """Run a project on demand against its destination."""
    ws = resolve_write_workspace(request, workspace_id)
    project = get_transform_store().get(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Transformation project not found")
    assert_resource_workspace(request, project.workspace_id or "")

    from services.connector_store import get_connector

    connector = get_connector(project.destination_connector_id, workspace_id=ws or None)
    if not connector:
        raise HTTPException(
            status_code=422,
            detail=(
                "The project's destination connector no longer exists. "
                "Re-point the project at a saved connector before running it."
            ),
        )

    # to_dict keeps the password, which the engine needs to connect. It is
    # never returned from this handler — only the run result is.
    cfg = connector.to_dict()
    from src.transfer.connector_capabilities import resolve_driver_type
    from services.transform_runner import TransformRunner

    runner = TransformRunner(
        cfg,
        dialect=resolve_driver_type(str(cfg.get("type") or "")),
        schema=project.schema or str(cfg.get("schema") or ""),
        dry_run=dry_run,
    )
    result = runner.run(list(project.models))
    return result.to_dict()
