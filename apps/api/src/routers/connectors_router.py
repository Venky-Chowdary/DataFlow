"""
DataTransfer.space — Connectors API Router
Manage connector configurations and data transfers
"""

import asyncio
import json

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Optional
from ..services.mongodb_service import get_mongodb_service
from ..services.file_parser import FileParser

router = APIRouter(prefix="/connectors", tags=["Connectors"])


# ═══════════════════════════════════════════════════════════════════════════════
# REQUEST/RESPONSE MODELS
# ═══════════════════════════════════════════════════════════════════════════════

class ConnectorConfig(BaseModel):
    """Connector configuration"""
    name: str = Field(..., description="Display name for this connector")
    type: str = Field(..., description="Connector type (mongodb, postgresql, mysql, etc.)")
    host: str = Field(..., description="Host address")
    port: int = Field(..., description="Port number")
    database: str = Field(default="", description="Database name")
    schema: Optional[str] = Field(default=None, description="Schema or dataset name")
    username: Optional[str] = Field(default=None, description="Username")
    password: Optional[str] = Field(default=None, description="Password")
    connection_string: Optional[str] = Field(default=None, description="Full connection string")
    options: dict = Field(default_factory=dict, description="Additional options")


class ConnectorResponse(BaseModel):
    """Response for connector operations"""
    id: str
    name: str
    type: str
    host: str
    port: int
    database: str
    status: str
    created_at: str


class TestConnectionRequest(BaseModel):
    """Request to test a connection"""
    type: str
    host: Optional[str] = ""
    port: Optional[int] = None
    database: str = ""
    schema: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    connection_string: Optional[str] = None


class TransferRequest(BaseModel):
    """Request to transfer data"""
    source_connector_id: Optional[str] = None
    destination_connector_id: str
    destination_database: str
    destination_collection: str
    data: Optional[list[dict]] = None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/test")
async def test_connection(request: TestConnectionRequest):
    """Test a connector configuration before saving"""
    try:
        if request.type == "mongodb":
            from pymongo import MongoClient
            
            if request.connection_string:
                conn_str = request.connection_string
            else:
                if request.username and request.password:
                    conn_str = f"mongodb://{request.username}:{request.password}@{request.host}:{request.port}/"
                else:
                    conn_str = f"mongodb://{request.host}:{request.port}/"
            
            client = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
            info = client.server_info()
            client.close()
            
            return {
                "success": True,
                "message": "Connection successful",
                "details": {
                    "version": info.get("version"),
                    "host": request.host,
                    "port": request.port,
                }
            }
        if request.type == "postgresql":
            from connectors.postgresql import test_postgresql
            probe = test_postgresql(
                host=request.host, port=request.port or 5432, database=request.database,
                username=request.username or "", password=request.password or "",
                schema="public", connection_string=request.connection_string or "", ssl=False,
            )
            return {"success": probe.ok, "message": probe.message if probe.ok else (probe.error or "Failed")}
        if request.type == "mysql":
            from connectors.mysql import test_mysql
            probe = test_mysql(
                host=request.host, port=request.port or 3306, database=request.database,
                username=request.username or "", password=request.password or "",
                schema="", connection_string=request.connection_string or "", ssl=False,
            )
            return {"success": probe.ok, "message": probe.message if probe.ok else (probe.error or "Failed")}
        if request.type == "bigquery":
            from connectors.bigquery import test_bigquery
            probe = test_bigquery(
                host=request.host or "bigquery.googleapis.com", port=request.port or 443, database=request.database,
                username="", password="", schema=request.schema or "dataflow",
                connection_string=request.connection_string or "", ssl=True,
            )
            return {"success": probe.ok, "message": probe.message if probe.ok else (probe.error or "Failed")}
        if request.type == "snowflake":
            from connectors.snowflake import test_snowflake
            probe = test_snowflake(
                host=request.host, port=request.port or 443, database=request.database,
                username=request.username or "", password=request.password or "",
                schema="PUBLIC", connection_string=request.connection_string or "", ssl=True,
                warehouse="",
            )
            return {"success": probe.ok, "message": probe.message if probe.ok else (probe.error or "Failed")}
        if request.type == "dynamodb":
            from connectors.dynamodb import test_dynamodb
            probe = test_dynamodb(
                host=request.host or "us-east-1", port=request.port or 443, database=request.database,
                username=request.username or "", password=request.password or "",
                schema="", connection_string=request.connection_string or "", ssl=True,
            )
            return {"success": probe.ok, "message": probe.message if probe.ok else (probe.error or "Failed")}
        if request.type == "redis":
            from connectors.redis_kv import test_redis
            probe = test_redis(
                host=request.host or "localhost", port=request.port or 6379, database=request.database,
                username=request.username or "", password=request.password or "",
                schema="", connection_string=request.connection_string or "", ssl=False,
            )
            return {"success": probe.ok, "message": probe.message if probe.ok else (probe.error or "Failed")}
        if request.type == "s3":
            from connectors.s3 import test_s3
            probe = test_s3(
                host=request.host or "us-east-1", port=request.port or 443, database=request.database,
                username=request.username or "", password=request.password or "",
                schema="", connection_string=request.connection_string or "", ssl=True,
            )
            return {"success": probe.ok, "message": probe.message if probe.ok else (probe.error or "Failed")}
        if request.type == "elasticsearch":
            from connectors.elasticsearch import test_elasticsearch
            probe = test_elasticsearch(
                host=request.host or "localhost", port=request.port or 9200, database=request.database,
                username=request.username or "", password=request.password or "",
                schema="", connection_string=request.connection_string or "", ssl=False,
            )
            return {"success": probe.ok, "message": probe.message if probe.ok else (probe.error or "Failed")}
        else:
            return {
                "success": False,
                "message": f"Connector type '{request.type}' not yet implemented",
            }
            
    except Exception as e:
        return {
            "success": False,
            "message": f"Connection failed: {str(e)}",
        }


@router.post("/", response_model=ConnectorResponse)
async def create_connector(config: ConnectorConfig):
    """Create and save a new connector configuration"""
    try:
        mongo = get_mongodb_service()
        
        connector_data = {
            "name": config.name,
            "type": config.type,
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "schema": config.schema,
            "username": config.username,
            "password": config.password,
            "connection_string": config.connection_string,
            "options": config.options,
            "status": "configured",
        }
        
        connector_id = mongo.save_connector(connector_data)
        connector = mongo.get_connector(connector_id)
        
        return ConnectorResponse(
            id=connector["_id"],
            name=connector["name"],
            type=connector["type"],
            host=connector["host"],
            port=connector["port"],
            database=connector["database"],
            status=connector["status"],
            created_at=connector["created_at"].isoformat(),
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/")
async def list_connectors():
    """List all saved connectors"""
    try:
        mongo = get_mongodb_service()
        connectors = mongo.list_connectors()
        
        result = []
        for c in connectors:
            result.append({
                "id": c["_id"],
                "name": c["name"],
                "type": c["type"],
                "host": c["host"],
                "port": c["port"],
                "database": c.get("database", ""),
                "status": c.get("status", "configured"),
                "created_at": c["created_at"].isoformat() if c.get("created_at") else None,
            })
        
        return {"connectors": result, "count": len(result)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs")
async def list_transfer_jobs():
    """List recent transfer jobs"""
    try:
        mongo = get_mongodb_service()
        jobs = mongo.list_jobs()
        return {"jobs": jobs, "count": len(jobs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}")
async def get_transfer_job(job_id: str):
    """Get a specific transfer job"""
    try:
        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        for key in ("created_at", "updated_at", "started_at", "completed_at"):
            if job.get(key) and hasattr(job[key], "isoformat"):
                job[key] = job[key].isoformat()
        return job
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/retry")
async def retry_transfer_job(job_id: str, background_tasks: BackgroundTasks):
    """Re-run a failed database migration using stored job configuration."""
    try:
        from ..transfer.background import run_transfer_async
        from ..transfer.engine import get_transfer_engine
        from ..transfer.models import transfer_request_from_dict

        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        payload = job.get("transfer_request")
        if not payload:
            raise HTTPException(
                status_code=400,
                detail="This job has no saved configuration — re-run from Transfer Studio.",
            )
        if payload.get("requires_file_reupload"):
            raise HTTPException(
                status_code=400,
                detail="File uploads must be re-submitted from Transfer Studio.",
            )

        request = transfer_request_from_dict(payload)
        engine = get_transfer_engine()
        new_job_id = engine._create_pending_job(request)
        mongo.update_job_status(new_job_id, "pending", retry_of=job_id, message=f"Retry of job {job_id}")

        background_tasks.add_task(run_transfer_async, new_job_id, request)
        return {
            "success": True,
            "async": True,
            "job_id": new_job_id,
            "retry_of": job_id,
            "status": "running",
            "message": "Retry started — stream progress on the new job.",
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/stream")
async def stream_transfer_job(job_id: str):
    """Server-sent events for live transfer job progress."""

    async def event_generator():
        mongo = get_mongodb_service()
        while True:
            job = mongo.get_job(job_id)
            if not job:
                yield f"event: error\ndata: {json.dumps({'error': 'Job not found'})}\n\n"
                break
            for key in ("created_at", "updated_at", "started_at", "completed_at"):
                if job.get(key) and hasattr(job[key], "isoformat"):
                    job[key] = job[key].isoformat()
            yield f"data: {json.dumps(job)}\n\n"
            if job.get("status") in ("completed", "failed"):
                break
            await asyncio.sleep(0.4)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/{connector_id}")
async def get_connector(connector_id: str):
    """Get a specific connector"""
    try:
        mongo = get_mongodb_service()
        connector = mongo.get_connector(connector_id)
        
        if not connector:
            raise HTTPException(status_code=404, detail="Connector not found")
        
        return connector
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{connector_id}")
async def delete_connector(connector_id: str):
    """Delete a connector"""
    try:
        mongo = get_mongodb_service()
        success = mongo.delete_connector(connector_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Connector not found")
        
        return {"success": True, "message": "Connector deleted"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# FILE UPLOAD & TRANSFER
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload and parse a file"""
    try:
        content = await file.read()
        result = FileParser.parse(content, file.filename)
        
        if not result.success:
            raise HTTPException(status_code=400, detail=result.error)
        
        schema = FileParser.infer_schema(result.data)
        
        return {
            "success": True,
            "filename": file.filename,
            "file_type": result.file_type,
            "row_count": result.row_count,
            "columns": result.columns,
            "schema": schema,
            "sample_data": result.data[:5],
            "data": result.data,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/transfer")
async def transfer_data(
    background_tasks: BackgroundTasks,
    destination_database: str = Form(...),
    destination_collection: str = Form(...),
    file: UploadFile = File(...),
    connector_id: Optional[str] = Form(None),
    skip_preflight: str = Form("false"),
    dest_type: str = Form("mongodb"),
    dest_host: str = Form(""),
    dest_port: int = Form(0),
    dest_schema: str = Form("public"),
    dest_username: str = Form(""),
    dest_password: str = Form(""),
    dest_warehouse: str = Form(""),
    async_mode: str = Form("true"),
    sync_mode: str = Form("full_refresh_overwrite"),
    schema_policy: str = Form("manual_review"),
    validation_mode: str = Form("strict"),
    backfill_new_fields: str = Form("false"),
    stream_contracts_json: str = Form(""),
):
    """Universal file transfer — delegates to UniversalTransferEngine."""
    try:
        from ..transfer.engine import get_transfer_engine
        from ..transfer.models import EndpointConfig, TransferRequest
        from ..transfer.background import run_transfer_async

        content = await file.read()
        src_fmt = file.filename.rsplit(".", 1)[-1].lower() if file.filename and "." in file.filename else "csv"

        request = TransferRequest(
            source=EndpointConfig(kind="file", format=src_fmt),
            destination=EndpointConfig(
                kind="database",
                format=dest_type,
                connector_id=connector_id,
                host=dest_host,
                port=dest_port,
                database=destination_database,
                schema=dest_schema,
                collection=destination_collection,
                table=destination_collection,
                username=dest_username,
                password=dest_password,
                warehouse=dest_warehouse,
            ),
            skip_preflight=skip_preflight.lower() in ("true", "1", "yes"),
            source_filename=file.filename or "upload.csv",
            source_content=content,
            sync_mode=sync_mode,
            schema_policy=schema_policy,
            validation_mode=validation_mode,
            backfill_new_fields=backfill_new_fields.lower() in ("true", "1", "yes"),
        )
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
                "source": {"type": "file", "filename": file.filename, "file_type": src_fmt},
            }

        result = engine.execute_tracked(request, job_id)
        if not result.success:
            raise HTTPException(status_code=422, detail={"error": result.error, "job_id": result.job_id})

        return {
            "success": True,
            "async": False,
            "job_id": result.job_id,
            "source": {"type": "file", "filename": file.filename, "file_type": src_fmt},
            "destination": result.destination_summary,
            "records_transferred": result.records_transferred,
            "columns": result.columns,
            "ddl_executed": result.ddl_executed,
            "operation": result.operation,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
