"""
DataTransfer.space — Connectors API Router
Manage connector configurations and data transfers
"""

import asyncio
import json
import os

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Optional
from pymongo.errors import PyMongoError
from ..services.mongodb_service import get_mongodb_service
from ..services.file_parser import FileParser
from ..transfer.connector_capabilities import resolve_driver_type, get_capabilities
from ..transfer.connector_registry import CONNECTOR_MODULES, run_probe

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
    warehouse: Optional[str] = Field(default=None, description="Snowflake warehouse")
    ssl: bool = Field(default=False, description="Use SSL/TLS")
    auth_mode: Optional[str] = Field(default=None, description="Authentication mode")
    auth_role: Optional[str] = Field(default=None, description="Snowflake / database role")
    api_key: Optional[str] = Field(default=None, description="API key")
    service_account: Optional[str] = Field(default=None, description="Service account JSON")
    options: dict = Field(default_factory=dict, description="Additional options")
    auth_source: Optional[str] = None
    role: Optional[str] = Field(default="both", description="Connector role: source | destination | both")


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
    warehouse: Optional[str] = ""
    ssl: Optional[bool] = False
    auth_mode: Optional[str] = ""
    auth_role: Optional[str] = ""
    api_key: Optional[str] = None
    service_account: Optional[str] = None
    auth_source: Optional[str] = None


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
    from ..transfer.connector_registry import humanize_connection_error

    try:
        if request.type in ("csv", "tsv", "json", "jsonl", "ndjson", "excel", "parquet"):
            path = (request.connection_string or request.host or "").strip()
            if path:
                if "://" in path:
                    return {
                        "success": True,
                        "message": f"{request.type.upper()} file source configured — data will be read from the provided URL or object-store URI.",
                        "details": {"format": request.type, "mode": "file_source", "path": path},
                    }
                if not os.path.exists(path):
                    return {
                        "success": False,
                        "message": f"Path not found: {path}. Create the directory or mount the volume before running.",
                        "details": {"format": request.type, "mode": "file_source", "path": path},
                    }
            return {
                "success": True,
                "message": f"{request.type.upper()} file format supported — upload a sample file or provide a file path to validate parsing",
                "details": {"format": request.type, "mode": "file_source"},
            }

        driver = resolve_driver_type(request.type)

        # Enforce required fields per authentication mode so the UI and API behave
        # consistently and do not pass empty values to a driver that will fail
        # with a cryptic low-level error.
        auth_mode = (request.auth_mode or "").strip().lower()
        if not auth_mode:
            if request.connection_string:
                auth_mode = "connection_string"
            elif request.service_account:
                auth_mode = "service_account"
            elif request.api_key:
                auth_mode = "api_key"
            elif request.username or request.password:
                auth_mode = "user_pass"
            else:
                auth_mode = "user_pass"

        if auth_mode in ("connection_string", "file_path"):
            if not (request.connection_string or "").strip():
                return {"success": False, "message": "Connection string is required.", "driver": driver, "auth_source": request.auth_source or ""}
        elif auth_mode == "service_account":
            if not (request.service_account or "").strip():
                return {"success": False, "message": "Service account JSON or file path is required.", "driver": driver, "auth_source": request.auth_source or ""}
            if not (request.database or "").strip():
                return {"success": False, "message": "Project / bucket / database is required for service account authentication.", "driver": driver, "auth_source": request.auth_source or ""}
        elif auth_mode == "api_key":
            if not (request.api_key or "").strip():
                return {"success": False, "message": "API key is required.", "driver": driver, "auth_source": request.auth_source or ""}
            if not (request.host or "").strip():
                return {"success": False, "message": "Host is required for API key authentication.", "driver": driver, "auth_source": request.auth_source or ""}
        elif auth_mode == "aws_keys":
            if not (request.host or "").strip() and not (request.database or "").strip():
                return {"success": False, "message": "Region / endpoint and bucket / table are required for AWS authentication.", "driver": driver, "auth_source": request.auth_source or ""}
            if not (request.username or "").strip() or not (request.password or "").strip():
                return {"success": False, "message": "Access key ID and secret access key are required for AWS authentication.", "driver": driver, "auth_source": request.auth_source or ""}
        elif auth_mode == "user_pass":
            # Path-based engines (SQLite/DuckDB) use host as a file path; others
            # need a real host and port.
            path_based = driver in ("sqlite", "duckdb")
            has_path = (request.host or "").strip() or (request.database or "").strip()
            if not has_path and path_based:
                return {"success": False, "message": "File path or database name is required for SQLite/DuckDB.", "driver": driver, "auth_source": request.auth_source or ""}
            if not (request.host or "").strip() and not path_based:
                return {"success": False, "message": "Host is required for username & password authentication.", "driver": driver, "auth_source": request.auth_source or ""}
            if not path_based and driver not in ("bigquery", "snowflake", "s3", "dynamodb", "gcs", "adls", "elasticsearch"):
                if not (request.port or 0):
                    return {"success": False, "message": "Port is required for username & password authentication.", "driver": driver, "auth_source": request.auth_source or ""}
            if driver not in ("sqlite", "duckdb", "bigquery", "s3", "dynamodb", "gcs", "adls"):
                if not (request.username or "").strip() or not (request.password or "").strip():
                    return {"success": False, "message": "Username and password are required.", "driver": driver, "auth_source": request.auth_source or ""}

        cfg = {
            "host": request.host or "",
            "port": request.port or 0,
            "database": request.database or "",
            "username": request.username or "",
            "password": request.password or "",
            "schema": request.schema or "",
            "connection_string": request.connection_string or "",
            "ssl": bool(request.ssl) if request.ssl is not None else False,
            "warehouse": request.warehouse or "",
            "type": request.type,
            "auth_mode": request.auth_mode or "",
            "auth_role": request.auth_role or "",
            "role": request.auth_role or "",
            "api_key": request.api_key or "",
            "service_account": request.service_account or "",
            "auth_source": request.auth_source or "",
        }
        ok, msg = run_probe(driver, cfg)
        return {"success": ok, "message": msg, "driver": driver, "auth_source": cfg.get("auth_source", "")}

    except Exception as e:
        return {
            "success": False,
            "message": humanize_connection_error(resolve_driver_type(request.type or ""), e),
        }


@router.post("/", response_model=ConnectorResponse)
async def create_connector(config: ConnectorConfig):
    """Create and save a new connector configuration (file store + MongoDB when available)."""
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
        "warehouse": config.warehouse,
        "ssl": config.ssl,
        "auth_mode": config.auth_mode,
        "auth_role": config.auth_role,
        "auth_source": config.auth_source,
        "role": config.role or "both",
        "api_key": config.api_key,
        "service_account": config.service_account,
        "options": config.options,
        "status": "configured",
    }

    # Canonical persistence: file-backed store (always works without MongoDB)
    try:
        import sys
        from pathlib import Path
        _api_root = Path(__file__).resolve().parents[2]
        if str(_api_root) not in sys.path:
            sys.path.insert(0, str(_api_root))
        from services.connector_store import create_connector as fs_create
        saved = fs_create(connector_data)
        return ConnectorResponse(
            id=saved.id,
            name=saved.name,
            type=saved.type,
            host=saved.host,
            port=saved.port,
            database=saved.database,
            status="configured",
            created_at=saved.created_at,
        )
    except Exception as fs_err:
        pass  # fall through to MongoDB

    try:
        mongo = get_mongodb_service()
        
        mongo_data = {k: v for k, v in connector_data.items() if k != "role" and k != "ssl"}
        connector_id = mongo.save_connector(mongo_data)
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
    """List all saved connectors (file store first, MongoDB fallback)."""
    try:
        import sys
        from pathlib import Path
        _api_root = Path(__file__).resolve().parents[2]
        if str(_api_root) not in sys.path:
            sys.path.insert(0, str(_api_root))
        from services.connector_store import list_connectors as fs_list, mask_connector
        items = fs_list()
        if items:
            return {
                "connectors": [
                    {
                        "id": c.id,
                        "name": c.name,
                        "type": c.type,
                        "host": c.host,
                        "port": c.port,
                        "database": c.database,
                        "status": "configured" if c.last_test_ok is True else ("error" if c.last_tested_at and c.last_test_ok is False else "configured"),
                        "created_at": c.created_at,
                        "last_test_ok": c.last_test_ok,
                    }
                    for c in items
                ],
                "count": len(items),
            }
    except Exception:
        pass

    try:
        mongo = get_mongodb_service()
        connectors = mongo.list_connectors()

        def _status_from_doc(c: dict) -> str:
            last_ok = c.get("last_test_ok")
            last_at = c.get("last_tested_at")
            if last_ok is True:
                return "configured"
            if last_ok is False and last_at:
                return "error"
            return "configured"

        result = []
        for c in connectors:
            created = c.get("created_at")
            result.append({
                "id": c["_id"],
                "name": c["name"],
                "type": c["type"],
                "host": c.get("host", ""),
                "port": c.get("port", 0),
                "database": c.get("database", ""),
                "status": _status_from_doc(c),
                "created_at": created.isoformat() if created and hasattr(created, "isoformat") else created,
                "last_test_ok": c.get("last_test_ok"),
            })

        return {"connectors": result, "count": len(result)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs")
async def list_transfer_jobs():
    """List recent transfer jobs.

    Degrades gracefully when the job store is unavailable: returns an empty
    list flagged ``degraded`` (HTTP 200) so Job Theater still renders instead
    of erroring. The ``/health`` endpoint continues to report the outage, so
    the infrastructure problem is not hidden.
    """
    try:
        mongo = get_mongodb_service()
        jobs = mongo.list_jobs()
        return {"jobs": jobs, "count": len(jobs), "degraded": False}
    except (PyMongoError, ConnectionError) as e:
        return {
            "jobs": [],
            "count": 0,
            "degraded": True,
            "persistence": "unavailable",
            "detail": str(e),
        }
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

        background_tasks.add_task(run_transfer_async, new_job_id, request, resume=True, resume_from_job_id=job_id)
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


@router.post("/jobs/{job_id}/resume")
async def resume_transfer_job(job_id: str, background_tasks: BackgroundTasks):
    """Resume a failed or paused transfer from its last durable checkpoint."""
    try:
        from ..transfer.background import run_transfer_async
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
        mongo.update_job_status(job_id, "pending", message=f"Resume requested for job {job_id}")
        background_tasks.add_task(run_transfer_async, job_id, request, resume=True)
        return {
            "success": True,
            "async": True,
            "job_id": job_id,
            "status": "running",
            "message": "Resume started — stream progress on the job.",
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/cancel")
async def cancel_transfer_job(job_id: str):
    """Request cancellation of a running/pending transfer job."""
    try:
        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.get("status") in ("completed", "failed", "cancelled"):
            return {"success": True, "job_id": job_id, "status": job.get("status"), "message": "Job already terminal"}
        mongo.update_job_status(
            job_id, "cancelled",
            phase="cancelled",
            message="Transfer cancelled by user",
            progress_pct=job.get("progress_pct", 0),
        )
        return {"success": True, "job_id": job_id, "status": "cancelled", "message": "Cancellation requested"}
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

        if result.row_count == 0:
            raise HTTPException(status_code=400, detail="File contains no records")

        if not result.columns:
            raise HTTPException(
                status_code=400,
                detail="No columns detected — use CSV/JSON/JSONL with object rows and consistent field names",
            )
        
        schema = FileParser.infer_schema(result.data)
        try:
            from services.data_profiler import merge_profiler_schema, profile_dataset

            profile = profile_dataset(result.columns, result.data)
            schema = merge_profiler_schema(schema, profile["schema"])
        except Exception:
            profile = None
        sample_cap = 100
        sample = result.data[:sample_cap]

        validation_report = None
        if result.file_type in ("csv", "tsv"):
            import sys
            from pathlib import Path
            _api_root = Path(__file__).resolve().parents[2]
            if str(_api_root) not in sys.path:
                sys.path.insert(0, str(_api_root))
            from services.csv_validator import validate_csv_content
            validation_report = validate_csv_content(content, result.columns, schema)
        
        return {
            "success": True,
            "filename": file.filename,
            "file_type": result.file_type,
            "row_count": result.row_count,
            "columns": result.columns,
            "schema": schema,
            "sample_data": sample,
            "data": sample,
            "validation": validation_report,
            "profile": profile,
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
    dest_connection_string: str = Form(""),
    dest_warehouse: str = Form(""),
    async_mode: str = Form("true"),
    sync_mode: str = Form("full_refresh_overwrite"),
    schema_policy: str = Form("manual_review"),
    validation_mode: str = Form("strict"),
    source_filter_json: str = Form(""),
    backfill_new_fields: str = Form("false"),
    stream_contracts_json: str = Form(""),
    mappings_json: str = Form(""),
):
    """Universal file transfer — delegates to UniversalTransferEngine."""
    try:
        from ..transfer.engine import get_transfer_engine
        from ..transfer.models import EndpointConfig, TransferRequest
        from ..transfer.background import run_transfer_async

        content = await file.read()
        src_fmt = FileParser.detect_file_type(file.filename or "upload.csv", content)
        if src_fmt == "unknown":
            src_fmt = "csv"

        source_filter: dict = {}
        if source_filter_json.strip():
            try:
                import json as _json
                parsed = _json.loads(source_filter_json)
                if isinstance(parsed, dict):
                    source_filter = parsed
            except Exception:
                source_filter = {}

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
                connection_string=dest_connection_string,
                warehouse=dest_warehouse,
            ),
            skip_preflight=skip_preflight.lower() in ("true", "1", "yes"),
            source_filename=file.filename or "upload.csv",
            source_content=content,
            sync_mode=sync_mode,
            schema_policy=schema_policy,
            validation_mode=validation_mode,
            source_filter=source_filter,
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
        if mappings_json.strip():
            try:
                import json as _json
                parsed = _json.loads(mappings_json)
                if isinstance(parsed, list):
                    request.mappings = parsed
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
