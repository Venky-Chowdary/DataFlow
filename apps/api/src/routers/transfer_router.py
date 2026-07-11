"""Universal transfer API — any source to any destination."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, BackgroundTasks
from pydantic import BaseModel, ConfigDict, Field
from typing import Optional

router = APIRouter(prefix="/transfer", tags=["Universal Transfer"])


class EndpointDTO(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    kind: str = Field(..., description="file | database | file_export")
    format: str = Field("", description="csv, json, mongodb, postgresql, snowflake")
    connector_id: Optional[str] = None
    host: str = ""
    port: int = 0
    database: str = ""
    db_schema: str = Field("public", alias="schema")
    table: str = ""
    collection: str = ""
    username: str = ""
    password: str = ""
    connection_string: str = ""
    warehouse: str = ""


class AnalyzeRequest(BaseModel):
    source: EndpointDTO
    destination: EndpointDTO
    source_columns: list[str] = Field(default_factory=list)
    source_schema: dict[str, str] = Field(default_factory=dict)


class MapColumnsRequest(BaseModel):
    source_columns: list[str]
    source_schema: dict[str, str] = Field(default_factory=dict)
    target_columns: list[str] = Field(default_factory=list)
    target_schema: dict[str, str] = Field(default_factory=dict)
    validation_mode: str = "balanced"
    file_format: Optional[str] = None
    use_llm: bool = True
    source_samples: dict[str, list[str]] = Field(default_factory=dict)


@router.get("/capabilities")
async def transfer_capabilities():
    """All live source → destination combinations + honest platform manifest."""
    from ..transfer.registry import get_capabilities
    from ..transfer.connector_capabilities import manifest_summary

    caps = get_capabilities()
    caps["platform"] = manifest_summary()
    caps["format_conversion"] = __import__(
        "services.format_converter", fromlist=["conversion_matrix"]
    ).conversion_matrix()
    try:
        from services.llm_mapping import llm_provider_available
        caps["llm_mapping_available"] = llm_provider_available()
    except Exception:
        caps["llm_mapping_available"] = False
    return caps


@router.get("/platform")
async def platform_status():
    """Honest readiness summary for UI and marketing."""
    from ..transfer.connector_capabilities import manifest_summary
    from ..services.catalog_service import catalog_summary

    cat = catalog_summary()
    manifest = manifest_summary()
    try:
        from services.llm_mapping import llm_provider_available
        llm_ready = llm_provider_available()
    except Exception:
        llm_ready = False
    return {
        "catalog_total": cat.get("total", 0),
        "transfer_ready": cat.get("transfer_live", 0),
        "connect_test_only": cat.get("connect_only", 0),
        "roadmap": cat.get("roadmap", 0),
        "live_drivers": manifest.get("transfer_live_drivers", []),
        "live_route_combinations": manifest.get("live_route_combinations", 0),
        "llm_mapping_available": llm_ready,
        "preflight_gates": 8,
        "tagline": "Governed transfers with honest connector readiness",
    }


@router.post("/route")
async def analyze_route(body: AnalyzeRequest):
    """Score a source → destination route with conversion and driver hints."""
    from services.universal_router import analyze_route as score_route

    return score_route(
        body.source.kind,
        body.source.format or ("csv" if body.source.kind == "file" else body.source.format),
        body.destination.kind,
        body.destination.format or ("json" if body.destination.kind == "file_export" else body.destination.format),
    )


@router.post("/analyze")
async def analyze_transfer(request: AnalyzeRequest):
    """Understand source/destination and show auto-creation plan."""
    from ..transfer.engine import get_transfer_engine
    from ..transfer.models import EndpointConfig

    engine = get_transfer_engine()
    src_data = request.source.model_dump(by_alias=True)
    dst_data = request.destination.model_dump(by_alias=True)
    return engine.analyze_compatibility(
        EndpointConfig.from_dict(request.source.kind, src_data),
        EndpointConfig.from_dict(request.destination.kind, dst_data),
        source_columns=request.source_columns or None,
        source_schema=request.source_schema or None,
    )


@router.post("/introspect")
async def introspect_endpoint_route(request: AnalyzeRequest):
    """Probe source or destination — list tables/collections, infer schema."""
    from ..transfer.endpoint_intelligence import introspect_endpoint
    from ..transfer.models import EndpointConfig

    src = EndpointConfig.from_dict(request.source.kind, request.source.model_dump(by_alias=True))
    dst = EndpointConfig.from_dict(request.destination.kind, request.destination.model_dump(by_alias=True))
    return {
        "source": introspect_endpoint(src),
        "destination": introspect_endpoint(dst),
    }


@router.post("/map")
async def map_columns_route(body: MapColumnsRequest):
    """
    Map source columns to destination columns using semantic engine.
    Uses real destination schema when target_columns are provided.
    """
    from ..services.preflight_service import confidence_threshold_for_mode

    import sys
    from pathlib import Path
    _api_root = Path(__file__).resolve().parents[2]
    if str(_api_root) not in sys.path:
        sys.path.insert(0, str(_api_root))
    from services.mapping_pipeline import run_mapping_pipeline

    threshold = confidence_threshold_for_mode(body.validation_mode)
    samples_by_col = body.source_samples or {}
    source_schemas = [
        {
            "name": c,
            "inferred_type": body.source_schema.get(c, "VARCHAR"),
            "samples": [str(x) for x in samples_by_col.get(c, [])[:8]],
        }
        for c in body.source_columns
    ]
    target_schemas = [
        {"name": c, "inferred_type": body.target_schema.get(c, "VARCHAR"), "samples": []}
        for c in body.target_columns
    ] if body.target_columns else None

    result = run_mapping_pipeline(
        body.source_columns,
        body.target_columns or [],
        source_schemas=source_schemas,
        target_schemas=target_schemas,
        file_format=body.file_format,
        confidence_threshold=threshold,
        use_llm=body.use_llm,
        source_samples=body.source_samples or None,
    )
    nested_fields: list[dict[str, str]] = []
    try:
        from services.json_intelligence import flatten_column_recommendations

        nested_fields = flatten_column_recommendations(body.source_columns)
    except Exception:
        nested_fields = []

    return {
        "mappings": result["mappings"],
        "transforms": result.get("transforms", []),
        "validation": result["validation"],
        "classification": result["classification"],
        "pruned_sources": result.get("pruned_sources", []),
        "agents_used": result.get("agents_used", []),
        "llm": result.get("llm", {}),
        "confidence_threshold": threshold,
        "destination_aware": bool(body.target_columns),
        "plan_summary": result.get("plan_summary", {}),
        "quality_issues": result.get("quality_issues", []),
        "nested_fields": nested_fields,
    }


class CreatePlanRequest(BaseModel):
    name: str = "Transfer plan"
    source: dict = Field(default_factory=dict)
    destination: dict = Field(default_factory=dict)
    source_columns: list[str] = Field(default_factory=list)
    source_schema: dict[str, str] = Field(default_factory=dict)
    target_columns: list[str] = Field(default_factory=list)
    target_schema: dict[str, str] = Field(default_factory=dict)
    row_count_estimate: int = 0
    sample_rows: list[dict] = Field(default_factory=list)
    policies: dict = Field(default_factory=dict)


class PlanMapRequest(BaseModel):
    validation_mode: str = "balanced"
    use_llm: bool = True
    source_samples: dict[str, list[str]] = Field(default_factory=dict)


class UpdatePlanRequest(BaseModel):
    name: Optional[str] = None
    source: Optional[dict] = None
    destination: Optional[dict] = None
    source_columns: Optional[list[str]] = None
    source_schema: Optional[dict[str, str]] = None
    target_columns: Optional[list[str]] = None
    target_schema: Optional[dict[str, str]] = None
    row_count_estimate: Optional[int] = None
    sample_rows: Optional[list[dict]] = None
    policies: Optional[dict] = None


class SyncMappingsRequest(BaseModel):
    mappings: list[dict] = Field(default_factory=list)


@router.get("/plans")
async def list_transfer_plans(limit: int = 50):
    from services.transfer_plan_store import list_plans

    plans = list_plans(limit=limit)
    return {
        "plans": [
            {
                "id": p.id,
                "name": p.name,
                "status": p.status,
                "active_version": p.active_version,
                "mapped_count": len(p.active_revision().mappings) if p.active_revision() else 0,
                "updated_at": p.updated_at,
                "job_ids": p.job_ids,
            }
            for p in plans
        ]
    }


@router.post("/plans")
async def create_transfer_plan(body: CreatePlanRequest):
    from services.transfer_plan_store import create_plan
    from services.audit_log import append_audit_event

    plan = create_plan(body.model_dump())
    append_audit_event(
        action="transfer_plan.created",
        resource=f"plan/{plan.id}",
        details={"name": plan.name, "source_columns": len(plan.source_columns)},
    )
    return {"plan": plan.to_dict()}


@router.get("/plans/{plan_id}")
async def get_transfer_plan(plan_id: str):
    from services.transfer_plan_store import get_plan

    plan = get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    return {"plan": plan.to_dict()}


@router.patch("/plans/{plan_id}")
async def update_transfer_plan(plan_id: str, body: UpdatePlanRequest):
    from services.transfer_plan_service import patch_plan

    try:
        plan = patch_plan(plan_id, body.model_dump(exclude_none=True))
        return {"plan": plan.to_dict()}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/plans/{plan_id}/mappings")
async def sync_transfer_plan_mappings(plan_id: str, body: SyncMappingsRequest):
    from services.transfer_plan_service import sync_plan_mappings

    try:
        plan = sync_plan_mappings(plan_id, body.mappings)
        return {"plan": plan.to_dict()}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/plans/{plan_id}/map")
async def map_transfer_plan(plan_id: str, body: PlanMapRequest):
    from services.transfer_plan_service import run_plan_mapping

    try:
        return run_plan_mapping(
            plan_id,
            validation_mode=body.validation_mode,
            use_llm=body.use_llm,
            source_samples=body.source_samples or None,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.post("/plans/{plan_id}/preflight")
async def preflight_transfer_plan(plan_id: str):
    from services.transfer_plan_service import run_plan_preflight

    try:
        return run_plan_preflight(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/plans/{plan_id}/approve")
async def approve_transfer_plan(plan_id: str, version: Optional[int] = None):
    from services.transfer_plan_service import approve_plan

    try:
        plan = approve_plan(plan_id, version)
        return {"plan": plan.to_dict()}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get("/plans/{plan_id}/run-payload")
async def transfer_plan_run_payload(plan_id: str):
    """Immutable mapping contract for execution — use with POST /transfer/run."""
    from services.transfer_plan_service import build_run_payload

    try:
        return build_run_payload(plan_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.post("/analyze-file")
async def analyze_file_transfer(
    file: UploadFile = File(...),
    dest_kind: str = Form("database"),
    dest_format: str = Form("mongodb"),
    dest_database: str = Form("test_db"),
    dest_table: str = Form(""),
    dest_collection: str = Form(""),
):
    """Analyze uploaded file against chosen destination — returns DDL plan."""
    from ..transfer.engine import get_transfer_engine
    from ..transfer.models import EndpointConfig
    from ..transfer.adapters import parse_file_route_sample

    content = await file.read()
    from ..services.file_parser import FileParser
    src_fmt = FileParser.detect_file_type(file.filename or "upload.csv", content)
    if src_fmt == "unknown":
        src_fmt = "csv"
    try:
        columns, schema, row_count = parse_file_route_sample(content, file.filename or "upload.csv")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    dest = EndpointConfig(
        kind=dest_kind,
        format=dest_format,
        database=dest_database,
        table=dest_table,
        collection=dest_collection or (file.filename.rsplit(".", 1)[0] if file.filename else "import"),
    )
    source = EndpointConfig(kind="file", format=src_fmt)
    plan = get_transfer_engine().analyze_compatibility(
        source,
        dest,
        source_columns=columns,
        source_schema=schema,
    )
    plan["row_count_estimate"] = row_count
    return plan


@router.post("/run")
async def run_universal_transfer(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(None),
    source_kind: str = Form("file"),
    source_format: str = Form(""),
    dest_kind: str = Form("database"),
    dest_format: str = Form("mongodb"),
    dest_database: str = Form("test_db"),
    dest_schema: str = Form("public"),
    dest_table: str = Form(""),
    dest_collection: str = Form(""),
    dest_connector_id: Optional[str] = Form(None),
    dest_host: str = Form(""),
    dest_port: int = Form(0),
    dest_username: str = Form(""),
    dest_password: str = Form(""),
    dest_connection_string: str = Form(""),
    dest_warehouse: str = Form(""),
    source_connector_id: Optional[str] = Form(None),
    source_database: str = Form(""),
    source_table: str = Form(""),
    source_collection: str = Form(""),
    skip_preflight: str = Form("false"),
    async_mode: str = Form("true"),
    mappings_json: str = Form(""),
    plan_id: Optional[str] = Form(None),
    sync_mode: str = Form("full_refresh_overwrite"),
    schema_policy: str = Form("manual_review"),
    validation_mode: str = Form("strict"),
    backfill_new_fields: str = Form("false"),
    stream_contracts_json: str = Form(""),
):
    """
    Execute universal transfer: file/db → db/file/warehouse.
    Auto-creates tables, collections, and typed schemas.
    """
    from ..transfer.engine import get_transfer_engine
    from ..transfer.models import EndpointConfig, TransferRequest
    from ..transfer.background import run_transfer_async

    src_fmt = source_format
    if source_kind == "file" and file:
        content = await file.read()
        filename = file.filename or "upload.csv"
        if not src_fmt:
            from ..services.file_parser import FileParser
            src_fmt = FileParser.detect_file_type(filename, content)
            if src_fmt == "unknown":
                src_fmt = "csv"
    else:
        content = b""
        filename = ""

    source = EndpointConfig(
        kind=source_kind,
        format=src_fmt,
        connector_id=source_connector_id,
        database=source_database,
        table=source_table,
        collection=source_collection,
    )
    destination = EndpointConfig(
        kind=dest_kind,
        format=dest_format,
        connector_id=dest_connector_id,
        host=dest_host,
        port=dest_port,
        database=dest_database,
        schema=dest_schema,
        table=dest_table,
        collection=dest_collection,
        username=dest_username,
        password=dest_password,
        connection_string=dest_connection_string,
        warehouse=dest_warehouse,
    )

    request = TransferRequest(
        source=source,
        destination=destination,
        skip_preflight=skip_preflight.lower() in ("true", "1", "yes"),
        source_filename=filename,
        source_content=content,
        sync_mode=sync_mode,
        schema_policy=schema_policy,
        validation_mode=validation_mode,
        backfill_new_fields=backfill_new_fields.lower() in ("true", "1", "yes"),
    )
    if plan_id and plan_id.strip():
        from services.transfer_plan_service import build_run_payload
        from services.transfer_plan_store import attach_job

        try:
            payload = build_run_payload(plan_id.strip())
            request.mappings = payload["mappings"]
            if not mappings_json.strip():
                request.column_types = payload.get("column_types") or {}
            policies = payload.get("policies") or {}
            request.sync_mode = policies.get("sync_mode", request.sync_mode)
            request.schema_policy = policies.get("schema_policy", request.schema_policy)
            request.validation_mode = policies.get("validation_mode", request.validation_mode)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
    if mappings_json.strip():
        try:
            import json as _json
            parsed = _json.loads(mappings_json)
            if isinstance(parsed, list):
                request.mappings = parsed
        except Exception:
            pass
    if stream_contracts_json.strip():
        try:
            import json as _json
            parsed = _json.loads(stream_contracts_json)
            if isinstance(parsed, list):
                request.stream_contracts = parsed
        except Exception:
            pass

    engine = get_transfer_engine()
    job_id = engine._create_pending_job(request)

    if plan_id and plan_id.strip():
        from services.transfer_plan_store import attach_job
        attach_job(plan_id.strip(), job_id, status="running")

    if async_mode.lower() in ("true", "1", "yes"):
        background_tasks.add_task(run_transfer_async, job_id, request)
        return {
            "success": True,
            "async": True,
            "job_id": job_id,
            "status": "running",
            "operation": request.operation,
            "message": "Transfer started — stream progress at /connectors/jobs/{job_id}/stream",
        }

    result = engine.execute_tracked(request, job_id)
    if not result.success:
        raise HTTPException(status_code=422, detail={
            "error": result.error,
            "operation": result.operation,
            "job_id": result.job_id,
        })
    return {
        "success": True,
        "async": False,
        "job_id": result.job_id,
        "operation": result.operation,
        "records_transferred": result.records_transferred,
        "source": result.source_summary,
        "destination": result.destination_summary,
        "ddl_executed": result.ddl_executed,
        "columns": result.columns,
    }
