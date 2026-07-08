"""Universal transfer API — any source to any destination."""

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


@router.get("/capabilities")
async def transfer_capabilities():
    """All live source → destination combinations."""
    from ..transfer.registry import get_capabilities
    return get_capabilities()


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

    content = await file.read()
    src_fmt = file.filename.rsplit(".", 1)[-1].lower() if file.filename and "." in file.filename else "csv"
    dest = EndpointConfig(
        kind=dest_kind,
        format=dest_format,
        database=dest_database,
        table=dest_table,
        collection=dest_collection or (file.filename.rsplit(".", 1)[0] if file.filename else "import"),
    )
    source = EndpointConfig(kind="file", format=src_fmt)
    return get_transfer_engine().analyze_compatibility(source, dest, content, file.filename or "upload.csv")


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
    dest_warehouse: str = Form(""),
    source_connector_id: Optional[str] = Form(None),
    source_database: str = Form(""),
    source_table: str = Form(""),
    source_collection: str = Form(""),
    skip_preflight: str = Form("false"),
    async_mode: str = Form("true"),
    mappings_json: str = Form(""),
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
        src_fmt = src_fmt or (file.filename.rsplit(".", 1)[-1].lower() if file.filename and "." in file.filename else "csv")
        content = await file.read()
        filename = file.filename or "upload.csv"
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
