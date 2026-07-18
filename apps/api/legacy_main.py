from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import asdict
from pathlib import Path

API_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(API_ROOT))
sys.path.insert(0, str(API_ROOT.parents[1] / "packages" / "preflight" / "src"))

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from preflight import PreflightEngine, TransferPlan
from preflight.models import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    SourceConfig,
)
from pydantic import BaseModel, Field

from connectors import test_database_connection
from registry import DATABASE_TYPES, FILE_FORMATS, DataOperation, infer_operation
from services.connector_catalog import list_catalog as list_connector_catalog
from services.value_serializer import json_default
from services.connector_factory import generate_connector_from_openapi
from services.connector_store import (
    create_connector,
    delete_connector,
    get_connector,
    list_connectors,
    mark_tested,
    mask_connector,
    update_connector,
)
from services.file_parser import get_file, store_upload
from services.jobs import job_store
from services.mapping_pipeline import run_mapping_pipeline
from services.migration_worker import dispatch_postgresql_migration
from services.object_store import storage_status
from services.preflight_runtime import RuntimePreflightContext, compute_capacity
from services.schema_introspect import introspect_schema
from services.semantic_analyzer import analyze_schema
from services.semantic_mapper import map_columns
from services.transfer_worker import dispatch_file_to_database
from services.workflow import WorkflowPhase, get_phase, set_phase

app = FastAPI(
    title="DataFlow API",
    description="One-click data transfer with fail-fast preflight gates",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

preflight_engine = PreflightEngine(fail_fast=True)


class ColumnSchemaDTO(BaseModel):
    name: str
    inferred_type: str = "VARCHAR"
    nullable: bool = True
    samples: list[str] = Field(default_factory=list)


class ColumnMappingDTO(BaseModel):
    source: str
    target: str
    confidence: float
    transform: str | None = None
    user_override: bool = False
    reasoning: str = ""


class PreflightRequestDTO(BaseModel):
    operation: str | None = None
    source_kind: str = "file"
    source_connected: bool = True
    source_parseable: bool = True
    source_columns: list[ColumnSchemaDTO]
    source_summary: str = ""
    file_id: str | None = None
    source_db_type: str = "postgresql"
    source_host: str = ""
    source_port: int = 5432
    source_database: str = ""
    source_username: str = ""
    source_password: str = ""
    source_schema: str = "public"
    source_connection_string: str = ""
    source_ssl: bool = True
    source_table: str = ""
    dest_kind: str = "database"
    dest_connected: bool = True
    dest_can_write: bool = True
    dest_columns: list[ColumnSchemaDTO]
    dest_summary: str = ""
    dest_db_type: str = "postgresql"
    dest_host: str = ""
    dest_port: int = 5432
    dest_database: str = ""
    dest_username: str = ""
    dest_password: str = ""
    dest_schema: str = "public"
    dest_connection_string: str = ""
    dest_ssl: bool = True
    dest_warehouse: str = ""
    mappings: list[ColumnMappingDTO]
    required_targets: list[str] = Field(default_factory=list)
    dry_run_passed: bool = True
    dry_run_errors: list[str] = Field(default_factory=list)
    confidence_threshold: float = 0.85


class ConnectTestDTO(BaseModel):
    type: str
    host: str = ""
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    schema: str = "public"
    connection_string: str = ""
    ssl: bool = True
    warehouse: str = ""


class MapRequestDTO(BaseModel):
    source_columns: list[str]
    target_columns: list[str] = Field(default_factory=list)
    source_schemas: list[ColumnSchemaDTO] = Field(default_factory=list)
    target_schemas: list[ColumnSchemaDTO] = Field(default_factory=list)
    file_format: str | None = None
    confidence_threshold: float = 0.85


class ConnectorSaveDTO(BaseModel):
    name: str
    type: str
    role: str = "both"
    host: str = ""
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    schema: str = "public"
    connection_string: str = ""
    ssl: bool = True
    warehouse: str = ""


class AnalyzeSchemaDTO(BaseModel):
    columns: list[ColumnSchemaDTO]


class ConnectorGenerateDTO(BaseModel):
    openapi: dict = Field(default_factory=dict)
    spec_url: str = ""


class GateResultDTO(BaseModel):
    gate_id: str
    status: str
    message: str
    duration_ms: float
    details: dict = Field(default_factory=dict)


class PreflightResponseDTO(BaseModel):
    passed: bool
    passed_count: int
    total_gates: int
    gates: list[GateResultDTO]
    blockers: list[GateResultDTO]


def _to_plan(dto: PreflightRequestDTO) -> TransferPlan:
    return TransferPlan(
        source=SourceConfig(
            kind=dto.source_kind,
            connected=dto.source_connected,
            parseable=dto.source_parseable,
            columns=[
                ColumnSchema(
                    name=c.name,
                    inferred_type=c.inferred_type,
                    nullable=c.nullable,
                    samples=c.samples,
                )
                for c in dto.source_columns
            ],
        ),
        destination=DestinationConfig(
            kind=dto.dest_kind,
            connected=dto.dest_connected,
            can_write=dto.dest_can_write,
            can_create_table=True,
            target_columns=[
                ColumnSchema(name=c.name, inferred_type=c.inferred_type) for c in dto.dest_columns
            ],
        ),
        mappings=[
            ColumnMapping(
                source=m.source,
                target=m.target,
                confidence=m.confidence,
                transform=m.transform,
                user_override=m.user_override,
                reasoning=m.reasoning,
            )
            for m in dto.mappings
        ],
        required_targets=dto.required_targets or [c.name for c in dto.dest_columns],
        dry_run_passed=False,
        dry_run_errors=[],
        ddl_compatible=True,
        estimated_bytes=0,
        available_staging_bytes=0,
        confidence_threshold=dto.confidence_threshold,
    )


def _build_preflight_context(dto: PreflightRequestDTO) -> RuntimePreflightContext:
    plan = _to_plan(dto)
    file_record = get_file(dto.file_id) if dto.file_id else None
    file_size = int(file_record.get("file_size_bytes", 0)) if file_record else 0
    source_db: dict | None = None

    if file_record:
        plan.source.row_count_estimate = int(file_record.get("row_count", 0))
    elif dto.source_kind == "database" and dto.source_table and dto.source_db_type == "postgresql":
        source_db = {
            "host": dto.source_host,
            "port": dto.source_port,
            "database": dto.source_database,
            "username": dto.source_username,
            "password": dto.source_password,
            "schema": dto.source_schema,
            "connection_string": dto.source_connection_string,
            "ssl": dto.source_ssl,
        }
        try:
            from connectors.postgresql_reader import count_table_rows

            plan.source.row_count_estimate = count_table_rows(
                **source_db,
                table=dto.source_table,
            )
        except Exception:
            plan.source.row_count_estimate = 1000

    estimated, available = compute_capacity(file_size, estimated_rows=plan.source.row_count_estimate)
    plan.estimated_bytes = estimated
    plan.available_staging_bytes = available
    return RuntimePreflightContext(
        plan,
        file_id=dto.file_id,
        file_size_bytes=file_size,
        source_db=source_db,
        source_table=dto.source_table,
    )


class SchemaIntrospectDTO(BaseModel):
    type: str
    host: str = ""
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    schema: str = "public"
    connection_string: str = ""
    ssl: bool = True
    warehouse: str = ""
    table: str | None = None
    auth_source: str = ""


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "dataflow-api",
        "version": "0.2.0",
        "job_store": getattr(job_store, "backend", "memory"),
        "storage": storage_status(),
    }


@app.get("/api/v1/capabilities")
def capabilities():
    return {
        "databases": DATABASE_TYPES,
        "file_formats": FILE_FORMATS,
        "operations": [op.value for op in DataOperation],
        "live_paths": [
            "file → postgresql",
            "file → snowflake",
            "postgresql → postgresql",
            "postgresql → snowflake",
        ],
        "source_kinds": ["file", "database", "api"],
        "destination_kinds": ["database", "file", "file_export"],
        "connectors": {
            "live": ["postgresql", "snowflake"],
            "planned": [db for db in DATABASE_TYPES if db not in {"postgresql", "snowflake"}],
            "ai_factory_target": 600,
            "catalog_total": list_connector_catalog(limit=1).get("catalog_total", 0),
        },
        "infrastructure": {
            "job_store": getattr(job_store, "backend", "memory"),
            "storage": storage_status(),
            "workflow": "local-stub",
        },
        "examples": [
            "CSV upload → Snowflake",
            "PostgreSQL → MongoDB migration",
            "SQL Server dump → Excel",
            "CSV → Word conversion",
            "Any DB → any DB with connection string",
        ],
    }


@app.get("/api/v1/stats")
def platform_stats():
    return job_store.stats()


@app.get("/api/v1/stats/gates")
def gate_stats():
    jobs = job_store.list_recent(limit=200)
    completed = [j for j in jobs if j.get("status") == "completed"]
    failed = [j for j in jobs if j.get("status") == "failed"]
    total = len(jobs) or 1
    recon_pass = sum(1 for j in completed if (j.get("reconciliation") or {}).get("passed"))
    return {
        "gates": [
            {"id": "g1", "label": "G1 Source", "status": "active", "count": min(100, int(len(completed) / total * 100))},
            {"id": "g2", "label": "G2 Dest", "status": "active", "count": min(100, int(len(completed) / total * 100))},
            {"id": "g3", "label": "G3 Schema", "status": "active", "count": min(100, int(len(completed) / max(1, len(completed) + len(failed)) * 100))},
            {"id": "g4", "label": "G4 Map", "status": "warning" if failed else "active", "count": max(85, min(100, int(recon_pass / max(1, len(completed)) * 100)))},
            {"id": "g5", "label": "G5 Dry", "status": "active", "count": min(100, int(len(completed) / total * 100))},
            {"id": "g6", "label": "G6 DDL", "status": "active", "count": 100 if completed else 0},
            {"id": "g7", "label": "G7 Cap", "status": "active", "count": 100},
            {"id": "g8", "label": "G8 Recon", "status": "active" if recon_pass else "idle", "count": int(recon_pass / max(1, len(completed)) * 100) if completed else 0},
        ],
        "sample_size": len(jobs),
    }


@app.post("/api/v1/schema/introspect")
def schema_introspect(body: SchemaIntrospectDTO):
    if body.type not in DATABASE_TYPES:
        raise HTTPException(status_code=400, detail=f"Unsupported database: {body.type}")
    return introspect_schema(
        body.type,
        host=body.host,
        port=body.port,
        database=body.database,
        username=body.username,
        password=body.password,
        schema=body.schema,
        connection_string=body.connection_string,
        ssl=body.ssl,
        warehouse=body.warehouse,
        table=body.table,
        auth_source=body.auth_source,
    )


@app.get("/api/v1/jobs")
def list_jobs(limit: int = 20):
    return {"jobs": job_store.list_recent(limit=limit)}


@app.get("/api/v1/jobs/{job_id}")
def get_job(job_id: str):
    job = job_store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return asdict(job)


@app.get("/api/v1/jobs/{job_id}/stream")
async def stream_job(job_id: str):
    """Server-sent events for live job progress (falls back to polling on client error)."""

    async def event_generator():
        while True:
            job = job_store.get(job_id)
            if not job:
                yield f"event: error\ndata: {json.dumps({'error': 'Job not found'}, default=json_default)}\n\n"
                break
            payload = asdict(job)
            yield f"data: {json.dumps(payload, default=json_default)}\n\n"
            if job.status in ("completed", "failed"):
                break
            await asyncio.sleep(0.35)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/v1/files/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename required")
    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > 50 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File exceeds 50MB limit")

    try:
        record = store_upload(file.filename, content)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Parse failed: {exc}") from exc

    return {
        "file_id": record["file_id"],
        "filename": record["filename"],
        "format": record["format"],
        "encoding": record.get("encoding", "utf-8"),
        "delimiter": record.get("delimiter", ","),
        "row_count": record["row_count"],
        "columns": record["columns"],
        "preview_rows": record.get("preview_rows", []),
    }


@app.get("/api/v1/files/{file_id}")
def get_uploaded_file(file_id: str):
    record = get_file(file_id)
    if not record:
        raise HTTPException(status_code=404, detail="File not found")
    return {
        "file_id": record["file_id"],
        "filename": record["filename"],
        "format": record["format"],
        "encoding": record.get("encoding", "utf-8"),
        "delimiter": record.get("delimiter", ","),
        "row_count": record["row_count"],
        "columns": record["columns"],
        "preview_rows": record.get("preview_rows", []),
    }


@app.post("/api/v1/map")
def semantic_map(body: MapRequestDTO):
    src_schemas = [s.model_dump() for s in body.source_schemas] if body.source_schemas else None
    tgt_schemas = [s.model_dump() for s in body.target_schemas] if body.target_schemas else None
    result = run_mapping_pipeline(
        body.source_columns,
        body.target_columns,
        source_schemas=src_schemas,
        target_schemas=tgt_schemas,
        file_format=body.file_format,
        confidence_threshold=body.confidence_threshold,
    )
    return result


@app.post("/api/v1/analyze/schema")
def analyze_column_schema(body: AnalyzeSchemaDTO):
    columns = [c.model_dump() for c in body.columns]
    return {"columns": analyze_schema(columns)}


@app.get("/api/v1/connectors/catalog")
def get_connector_catalog(
    q: str = "",
    category: str | None = None,
    status: str | None = None,
    offset: int = 0,
    limit: int = 48,
):
    return list_connector_catalog(q=q, category=category, status=status, offset=offset, limit=limit)


@app.get("/api/v1/connectors/saved")
def get_saved_connectors(role: str | None = None):
    return {"connectors": [mask_connector(c) for c in list_connectors(role)]}


@app.get("/api/v1/connectors/saved/{connector_id}")
def get_saved_connector_detail(connector_id: str):
    conn = get_connector(connector_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connector not found")
    return conn.to_dict()


@app.post("/api/v1/connectors/saved")
def save_connector(body: ConnectorSaveDTO):
    conn = create_connector(body.model_dump())
    return mask_connector(conn)


@app.put("/api/v1/connectors/saved/{connector_id}")
def update_saved_connector(connector_id: str, body: ConnectorSaveDTO):
    updated = update_connector(connector_id, body.model_dump())
    if not updated:
        raise HTTPException(status_code=404, detail="Connector not found")
    return mask_connector(updated)


@app.delete("/api/v1/connectors/saved/{connector_id}")
def remove_saved_connector(connector_id: str):
    if not delete_connector(connector_id):
        raise HTTPException(status_code=404, detail="Connector not found")
    return {"ok": True}


@app.post("/api/v1/connectors/saved/{connector_id}/test")
def test_saved_connector(connector_id: str):
    conn = get_connector(connector_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connector not found")
    result = test_database_connection(
        conn.type,
        host=conn.host,
        port=conn.port,
        database=conn.database,
        username=conn.username,
        password=conn.password,
        schema=conn.schema,
        connection_string=conn.connection_string,
        ssl=conn.ssl,
        warehouse=conn.warehouse,
    )
    mark_tested(connector_id, result.ok)
    return {
        "ok": result.ok,
        "tables": result.tables,
        "error": result.error,
        "message": result.message,
        "driver": result.driver,
    }


@app.post("/api/v1/connectors/generate")
def generate_connector(body: ConnectorGenerateDTO):
    spec = body.openapi
    if not spec and body.spec_url.strip():
        import urllib.request

        try:
            with urllib.request.urlopen(body.spec_url, timeout=15) as resp:
                spec = json.loads(resp.read().decode())
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Failed to fetch OpenAPI spec: {exc}") from exc
    if not spec or "paths" not in spec:
        raise HTTPException(status_code=400, detail="Provide openapi JSON or spec_url")
    return generate_connector_from_openapi(spec)


@app.post("/api/v1/connect/test")
def test_connection(body: ConnectTestDTO):
    if body.type not in DATABASE_TYPES:
        return {"ok": False, "error": f"Unsupported database type: {body.type}"}

    result = test_database_connection(
        body.type,
        host=body.host,
        port=body.port,
        database=body.database,
        username=body.username,
        password=body.password,
        schema=body.schema,
        connection_string=body.connection_string,
        ssl=body.ssl,
        warehouse=body.warehouse,
    )
    return {
        "ok": result.ok,
        "tables": result.tables,
        "error": result.error,
        "message": result.message,
        "driver": result.driver,
    }


@app.post("/api/v1/preflight", response_model=PreflightResponseDTO)
def run_preflight(body: PreflightRequestDTO):
    ctx = _build_preflight_context(body)
    result = preflight_engine.run(ctx)

    def gate_dto(g):
        return GateResultDTO(
            gate_id=g.gate_id.value,
            status=g.status.value,
            message=g.message,
            duration_ms=g.duration_ms,
            details=g.details,
        )

    return PreflightResponseDTO(
        passed=result.passed,
        passed_count=result.passed_count,
        total_gates=result.total_gates,
        gates=[gate_dto(g) for g in result.gates],
        blockers=[gate_dto(b) for b in result.blockers],
    )


@app.post("/api/v1/transfer")
def start_transfer(body: PreflightRequestDTO):
    op = body.operation or infer_operation(body.source_kind, body.dest_kind).value
    ctx = _build_preflight_context(body)
    result = preflight_engine.run(ctx)
    if not result.passed:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "preflight_blocked",
                "blockers": [
                    {"gate": b.gate_id.value, "message": b.message} for b in result.blockers
                ],
            },
        )

    file_record = get_file(body.file_id) if body.file_id else None
    total_rows = (
        file_record["row_count"]
        if file_record
        else _build_preflight_context(body).plan.source.row_count_estimate
    )

    job = job_store.create(
        operation=op,
        source=body.source_summary or body.source_kind,
        destination=body.dest_summary or body.dest_kind,
        total_rows=total_rows,
    )
    set_phase(job.job_id, WorkflowPhase.QUEUED, "Transfer queued")

    can_file = (
        body.source_kind == "file"
        and body.file_id
        and body.dest_kind == "database"
        and body.dest_db_type in {"postgresql", "snowflake"}
        and body.dest_connected
    )
    can_migrate = (
        body.source_kind == "database"
        and body.source_table
        and body.source_db_type == "postgresql"
        and body.dest_kind == "database"
        and body.dest_db_type in {"postgresql", "snowflake"}
        and body.source_connected
        and body.dest_connected
    )

    if not can_file and not can_migrate:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "unsupported_operation",
                "message": "Live execution: file → PostgreSQL/Snowflake, or PostgreSQL → PostgreSQL/Snowflake migration.",
            },
        )

    dest = {
        "host": body.dest_host,
        "port": body.dest_port,
        "database": body.dest_database,
        "username": body.dest_username,
        "password": body.dest_password,
        "schema": body.dest_schema,
        "connection_string": body.dest_connection_string,
        "ssl": body.dest_ssl,
        "warehouse": body.dest_warehouse,
    }
    mapping_dicts = [
        {
            "source": m.source,
            "target": m.target,
            "confidence": m.confidence,
            "user_override": m.user_override,
            "reasoning": m.reasoning,
            "transform": m.transform,
        }
        for m in body.mappings
    ]

    if can_file:
        dispatch_file_to_database(
            job_id=job.job_id,
            file_id=body.file_id,
            mappings=mapping_dicts,
            dest=dest,
            db_type=body.dest_db_type,
        )
    else:
        source = {
            "host": body.source_host,
            "port": body.source_port,
            "database": body.source_database,
            "username": body.source_username,
            "password": body.source_password,
            "schema": body.source_schema,
            "connection_string": body.source_connection_string,
            "ssl": body.source_ssl,
        }
        dispatch_postgresql_migration(
            job_id=job.job_id,
            source=source,
            dest=dest,
            dest_db_type=body.dest_db_type,
            mappings=mapping_dicts,
            source_table=body.source_table,
        )

    return {
        "job_id": job.job_id,
        "status": "queued",
        "operation": op,
        "async": True,
        "message": "Transfer started — poll /api/v1/jobs/{job_id} for progress",
    }
