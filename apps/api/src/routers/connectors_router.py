"""
DataTransfer.space — Connectors API Router
Manage connector configurations and data transfers
"""

import asyncio
import json
import os
from typing import Any, Optional

from fastapi import (
    APIRouter,
    BackgroundTasks,
    File,
    Form,
    Header,
    HTTPException,
    Request,
    UploadFile,
)
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from pymongo.errors import PyMongoError

from services.team_store import can_read_workspace, can_write_workspace
from services.value_serializer import json_default

from ..services.file_parser import FileParser
from ..services.mongodb_service import get_mongodb_service
from ..transfer.connector_capabilities import resolve_driver_type
from ..transfer.connector_registry import run_probe

router = APIRouter(prefix="/connectors", tags=["Connectors"])


def _actor_email(request: Request) -> str:
    return getattr(request.state, "user_email", None) or "anonymous"


def _resolve_workspace(request: Request, x_workspace_id: str = Header(default="", alias="X-Workspace-Id")) -> str:
    workspace_id = (x_workspace_id or "").strip()
    if workspace_id and not can_read_workspace(workspace_id, _actor_email(request)):
        raise HTTPException(status_code=403, detail="Access to workspace denied")
    return workspace_id


def _require_write_workspace(request: Request, x_workspace_id: str = Header(default="", alias="X-Workspace-Id")) -> str:
    workspace_id = (x_workspace_id or "").strip()
    if workspace_id and not can_write_workspace(workspace_id, _actor_email(request)):
        raise HTTPException(status_code=403, detail="Write access to workspace denied")
    return workspace_id


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
    endpoint_url: Optional[str] = Field(default=None, description="Custom S3/S3-compatible endpoint URL")
    path_style: bool = Field(default=False, description="Force S3 path-style addressing")
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
    workspace_id: str = ""
    role: str = "both"


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
    endpoint_url: Optional[str] = None
    path_style: Optional[bool] = False
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
            "role": getattr(request, "role", None) or "both",
            "api_key": request.api_key or "",
            "service_account": request.service_account or "",
            "endpoint_url": request.endpoint_url or "",
            "path_style": bool(request.path_style),
            "auth_source": request.auth_source or "",
        }
        ok, msg = run_probe(driver, cfg)
        payload: dict[str, Any] = {
            "success": ok,
            "message": msg,
            "driver": driver,
            "auth_source": cfg.get("auth_source", ""),
        }
        if ok and driver in {
            "sqlserver",
            "mssql",
            "oracle",
            "azure_sql_database",
            "microsoft_sql_server",
            "amazon_rds_sql_server",
        }:
            try:
                from services.source_ha_probe import probe_source_ha_safe

                ha = probe_source_ha_safe({**cfg, "type": driver})
                payload["source_ha"] = ha.to_dict()
                if ha.message:
                    payload["message"] = f"{msg} · {ha.message}"
            except Exception:
                pass
        return payload

    except Exception as e:
        return {
            "success": False,
            "message": humanize_connection_error(resolve_driver_type(request.type or ""), e),
        }


@router.post("/", response_model=ConnectorResponse)
async def create_connector(
    config: ConnectorConfig,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Create and save a new connector configuration (file store + MongoDB when available)."""
    workspace_id = _require_write_workspace(request, workspace_id)
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
        "endpoint_url": config.endpoint_url,
        "path_style": config.path_style,
        "options": config.options,
        "workspace_id": workspace_id,
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
            workspace_id=saved.workspace_id or "",
            role=getattr(saved, "role", None) or connector_data.get("role") or "both",
        )
    except Exception:
        pass  # fall through to MongoDB

    try:
        mongo = get_mongodb_service()

        # Persist topology role + ssl — dropping them made every DB look like a source.
        mongo_data = dict(connector_data)
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
            workspace_id=connector.get("workspace_id", ""),
            role=connector.get("role") or connector_data.get("role") or "both",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/")
async def list_connectors(
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """List saved connectors scoped to the requested workspace."""
    workspace_id = _resolve_workspace(request, workspace_id)
    try:
        import sys
        from pathlib import Path
        _api_root = Path(__file__).resolve().parents[2]
        if str(_api_root) not in sys.path:
            sys.path.insert(0, str(_api_root))
        from services.connector_store import list_connectors as fs_list
        items = fs_list(workspace_id=workspace_id)
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
                        "workspace_id": c.workspace_id or "",
                        "role": getattr(c, "role", None) or "both",
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
            if workspace_id and c.get("workspace_id") not in (workspace_id, "", None):
                continue
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
                "workspace_id": c.get("workspace_id", ""),
                "role": c.get("role") or "both",
            })

        return {"connectors": result, "count": len(result)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _can_access_job(request: Request, job: dict) -> bool:
    """True if the actor may see or mutate this job.

    Must stay aligned with ``list_jobs``: unscoped/global jobs (empty
    ``workspace_id``) are returned by the list endpoint for the default
    workspace filter. Denying them here caused Job Theater 404s in production
    when ``DATAFLOW_REQUIRE_WORKSPACE`` / production isolation is on.
    """
    workspace_id = (job.get("workspace_id") or "").strip()
    if not workspace_id:
        # Global / legacy jobs — same visibility as list_jobs for "".
        return True
    return can_read_workspace(workspace_id, _actor_email(request))


@router.get("/jobs")
async def list_transfer_jobs(
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """List recent transfer jobs scoped to a workspace.

    Degrades gracefully when the job store is unavailable: returns an empty
    list flagged ``degraded`` (HTTP 200) so Job Theater still renders instead
    of erroring. The ``/health`` endpoint continues to report the outage, so
    the infrastructure problem is not hidden.
    """
    workspace_id = _resolve_workspace(request, workspace_id)
    try:
        mongo = get_mongodb_service()
        jobs = await asyncio.to_thread(mongo.list_jobs, workspace_id=workspace_id)
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
async def get_transfer_job(job_id: str, request: Request):
    """Get a specific transfer job"""
    try:
        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)

        if not job or not _can_access_job(request, job):
            raise HTTPException(status_code=404, detail="Job not found")

        for key in ("created_at", "updated_at", "started_at", "completed_at"):
            if job.get(key) and hasattr(job[key], "isoformat"):
                job[key] = job[key].isoformat()
        return job
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/jobs/{job_id}")
async def patch_transfer_job(job_id: str, request: Request):
    """Update job metadata (display name). Route/source/dest stay immutable."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    raw_name = body.get("name") if isinstance(body, dict) else None
    if raw_name is None:
        raise HTTPException(status_code=400, detail="Provide name to update")
    name = str(raw_name).strip()
    if not name:
        raise HTTPException(status_code=400, detail="Job name cannot be empty")
    if len(name) > 120:
        raise HTTPException(status_code=400, detail="Job name must be 120 characters or fewer")

    try:
        from services.mongodb_service import _job_name_key

        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)
        if not job or not _can_access_job(request, job):
            raise HTTPException(status_code=404, detail="Job not found")

        current = (job.get("name") or "").strip()
        if name.casefold() != current.casefold():
            workspace_id = (job.get("workspace_id") or "").strip()
            if mongo.is_job_name_taken(name, workspace_id=workspace_id, exclude_job_id=job_id):
                raise HTTPException(status_code=409, detail="This name already exists")

        if not mongo.update_job_fields(job_id, {"name": name, "name_key": _job_name_key(name)}):
            raise HTTPException(status_code=500, detail="Failed to update job")
        updated = mongo.get_job(job_id) or {**job, "name": name}
        for key in ("created_at", "updated_at", "started_at", "completed_at"):
            if updated.get(key) and hasattr(updated[key], "isoformat"):
                updated[key] = updated[key].isoformat()
        return updated
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/retry")
async def retry_transfer_job(job_id: str, background_tasks: BackgroundTasks, request: Request):
    """Re-run a failed database migration using stored job configuration."""
    try:
        from ..transfer.background import run_transfer_async
        from ..transfer.engine import get_transfer_engine
        from ..transfer.models import transfer_request_from_dict

        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)
        if not job or not _can_access_job(request, job):
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
async def resume_transfer_job(job_id: str, background_tasks: BackgroundTasks, request: Request):
    """Resume a failed or paused transfer from its last durable checkpoint."""
    try:
        from ..transfer.background import run_transfer_async
        from ..transfer.models import transfer_request_from_dict

        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)
        if not job or not _can_access_job(request, job):
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
async def cancel_transfer_job(job_id: str, request: Request):
    """Request cancellation of a running/pending transfer job."""
    try:
        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)
        if not job or not _can_access_job(request, job):
            raise HTTPException(status_code=404, detail="Job not found")
        if job.get("status") in ("completed", "completed_with_quarantine", "failed", "cancelled"):
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
async def stream_transfer_job(job_id: str, request: Request):
    """Server-sent events for live transfer job progress."""

    # Pre-check workspace access before entering the stream loop.
    try:
        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)
        if not job or not _can_access_job(request, job):
            raise HTTPException(status_code=404, detail="Job not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    async def event_generator():
        mongo = get_mongodb_service()
        while True:
            job = mongo.get_job(job_id)
            if not job:
                yield f"event: error\ndata: {json.dumps({'error': 'Job not found'}, default=json_default)}\n\n"
                break
            for key in ("created_at", "updated_at", "started_at", "completed_at"):
                if job.get(key) and hasattr(job[key], "isoformat"):
                    job[key] = job[key].isoformat()
            yield f"data: {json.dumps(job, default=json_default)}\n\n"
            if job.get("status") in ("completed", "completed_with_quarantine", "failed", "cancelled"):
                break
            await asyncio.sleep(0.4)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/jobs/{job_id}/quarantine")
async def get_job_quarantine(job_id: str, request: Request):
    """Return quarantined rows for a job with their rejection reasons.

    Includes write-time rejects and preflight integrity findings (encoding, etc.)
    so Inspect Quarantine is never empty when Validate/Run reported bad cells.
    """
    from services.quarantine_from_preflight import merge_job_quarantine

    mongo = get_mongodb_service()
    job = mongo.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if not _can_access_job(request, job):
        raise HTTPException(status_code=403, detail="Workspace access denied")

    details = merge_job_quarantine(job)
    row_ids = {d.get("row") for d in details if isinstance(d, dict) and d.get("row") is not None}
    rejected_rows = int(job.get("rejected_rows") or 0) or (len(row_ids) if row_ids else len(details))
    source = "write" if (job.get("rejected_details") or (job.get("destination_summary") or {}).get("rejected_details")) else (
        "preflight" if details else "none"
    )
    ds = job.get("destination_summary") if isinstance(job.get("destination_summary"), dict) else {}
    dest_q = ds.get("dest_quarantine") if isinstance(ds.get("dest_quarantine"), dict) else {}
    dest_dlq: dict[str, Any] = {
        "table": ds.get("dest_quarantine_table") or dest_q.get("table"),
        "rows_written": ds.get("dest_quarantine_rows") or dest_q.get("rows_written"),
        "ok": dest_q.get("ok"),
        "skipped": dest_q.get("skipped"),
        "reason": dest_q.get("reason"),
        "error": ds.get("dest_quarantine_error") or dest_q.get("error"),
    }
    # Live open-row count when we have a saved transfer request + SQL dest.
    payload = job.get("transfer_request")
    if payload and dest_dlq.get("table"):
        try:
            from services.dest_quarantine import count_open_dlq_rows
            from ..transfer.models import transfer_request_from_dict

            treq = transfer_request_from_dict(payload)
            open_info = count_open_dlq_rows(treq.destination, job_id=job_id)
            dest_dlq["open_rows"] = open_info.get("open_rows")
            dest_dlq["supported"] = open_info.get("supported")
        except Exception:
            pass

    return {
        "job_id": job_id,
        "rejected_rows": rejected_rows,
        "issue_count": len(details),
        "source": source,
        "quarantine": details,
        "dest_dlq": dest_dlq,
    }


@router.post("/jobs/{job_id}/quarantine/export")
async def export_job_quarantine(job_id: str, request: Request):
    """Export quarantined rows to a CSV in the exports folder and return a download URL."""
    import uuid
    from pathlib import Path

    mongo = get_mongodb_service()
    job = mongo.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if not _can_access_job(request, job):
        raise HTTPException(status_code=403, detail="Workspace access denied")

    from services.quarantine_from_preflight import merge_job_quarantine

    details = merge_job_quarantine(job)
    if not details:
        return {"success": True, "row_count": 0, "download_url": "", "filename": ""}

    from services.format_converter import convert_rows

    headers = ["row", "column", "target", "value", "reason", "policy", "suggested_transform"]
    rows = [
        [
            str(d.get("row", "")),
            str(d.get("column", "")),
            str(d.get("target", "")),
            str(d.get("value", "")),
            str(d.get("reason", "")),
            str(d.get("policy", "")),
            str(d.get("suggested_transform", "")),
        ]
        for d in details
    ]
    content, _ = convert_rows(headers, rows, source_format="csv", target_format="csv")

    api_root = Path(__file__).resolve().parents[2]
    export_dir = api_root / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    filename = f"quarantine_{job_id}_{uuid.uuid4().hex[:8]}.csv"
    export_path = export_dir / filename
    export_path.write_bytes(content)

    return {
        "success": True,
        "row_count": len(details),
        "download_url": f"/api/v1/transfer/download/{filename}",
        "filename": filename,
    }


class QuarantineReplayRequest(BaseModel):
    """Replay quarantined rows through the destination writer with optional edits."""

    rows: list[dict] = Field(default_factory=list, description="Edited rejected_details; empty = all quarantine rows")
    transform_overrides: dict = Field(default_factory=dict, description="Optional per-column transform overrides keyed by source column")


def _quarantine_details_to_records(details: list[dict], transform_overrides: Optional[dict] = None) -> tuple[list[dict], list[str]]:
    """Group rejected_details by row index into source-shaped records for rewrite."""
    by_row: dict[int, dict] = {}
    order: list[int] = []
    for detail in details:
        try:
            row_num = int(detail.get("row") or 0)
        except (TypeError, ValueError):
            row_num = 0
        if row_num not in by_row:
            by_row[row_num] = {}
            order.append(row_num)
        base = detail.get("values") if isinstance(detail.get("values"), dict) else {}
        if base:
            for k, v in base.items():
                by_row[row_num].setdefault(str(k), "" if v is None else str(v))
        col = str(detail.get("column") or "").strip()
        if col:
            by_row[row_num][col] = "" if detail.get("value") is None else str(detail.get("value"))
    records = [by_row[n] for n in order if by_row[n]]
    columns: list[str] = []
    seen: set[str] = set()
    for rec in records:
        for k in rec:
            if k not in seen:
                seen.add(k)
                columns.append(k)
    _ = transform_overrides  # applied to mappings by caller
    return records, columns


@router.post("/jobs/{job_id}/quarantine/replay")
async def replay_job_quarantine(job_id: str, body: QuarantineReplayRequest, request: Request):
    """Rewrite quarantined (optionally edited) rows through the destination with the original mapping.

    Creates a child tracked job, writes synchronously, and returns rows_written / rejected
    plus the new job_id for audit.
    """
    from ..transfer.adapters import write_destination_database
    from ..transfer.engine import get_transfer_engine
    from ..transfer.models import transfer_request_from_dict

    mongo = get_mongodb_service()
    job = mongo.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if not _can_access_job(request, job):
        raise HTTPException(status_code=403, detail="Workspace access denied")

    payload = job.get("transfer_request")
    if not payload:
        raise HTTPException(
            status_code=400,
            detail="This job has no saved configuration — cannot replay quarantine.",
        )

    stored_details = job.get("rejected_details") or job.get("destination_summary", {}).get("rejected_details") or []
    details = body.rows if body.rows else list(stored_details)
    if not details:
        raise HTTPException(status_code=400, detail="No quarantine rows to replay")

    records, columns = _quarantine_details_to_records(details, body.transform_overrides)
    if not records:
        raise HTTPException(status_code=400, detail="Could not reconstruct rows from quarantine details")

    transfer_req = transfer_request_from_dict(payload)
    mappings = list(transfer_req.mappings or [])
    if body.transform_overrides:
        for m in mappings:
            src = m.get("source") or m.get("source_column") or ""
            if src in body.transform_overrides:
                m["transform"] = body.transform_overrides[src]
    if not mappings:
        mappings = [{"source": c, "target": c, "confidence": 0.95} for c in columns]

    schema = dict(transfer_req.column_types or {})
    for c in columns:
        schema.setdefault(c, "string")

    engine = get_transfer_engine()
    # Child job: append-only rewrite of remediations (never full-refresh overwrite).
    child_payload = dict(payload)
    child_payload["sync_mode"] = "full_refresh_append"
    child_payload["skip_preflight"] = True
    child_req = transfer_request_from_dict(child_payload)
    child_job_id = engine._create_pending_job(child_req)
    mongo.update_job_status(
        child_job_id,
        "running",
        retry_of=job_id,
        phase="writing",
        message=f"Quarantine replay of job {job_id}",
        operation="quarantine_replay",
    )

    try:
        dest = transfer_req.destination
        if dest.kind == "file_export":
            raise HTTPException(status_code=400, detail="Quarantine replay is not supported for file_export destinations")

        rows_written, ddl_log, dest_summary = write_destination_database(
            dest,
            records,
            columns,
            schema,
            mappings,
            validation_mode=transfer_req.validation_mode or "balanced",
            backfill_new_fields=bool(transfer_req.backfill_new_fields),
            write_mode="upsert" if any(m.get("source", "").lower() in {"id", "_id"} for m in mappings) else "insert",
            conflict_columns=[
                m.get("target") or m.get("target_column") or m.get("source")
                for m in mappings
                if (m.get("source") or "").lower() in {"id", "_id"}
            ] or None,
        )
        rejected = int(dest_summary.get("rejected_rows") or 0)
        status = "completed_with_quarantine" if rejected > 0 else "completed"
        promote_meta: dict[str, Any] = {}
        if rejected == 0:
            try:
                from services.dest_quarantine import mark_dlq_promoted

                qids = [
                    str(d.get("_df_qid") or "")
                    for d in details
                    if isinstance(d, dict) and d.get("_df_qid")
                ]
                # Prefer qids; when absent, stamp all open DLQ rows for this parent job.
                promote_meta = mark_dlq_promoted(
                    dest, qids=qids, job_id=job_id
                )
            except Exception as exc:
                promote_meta = {"error": str(exc)[:300]}
        mongo.update_job_status(
            child_job_id,
            status,
            phase="completed",
            message=f"Quarantine replay wrote {rows_written} row(s)",
            records_processed=rows_written,
            progress_pct=100,
            rejected_rows=rejected,
            rejected_details=dest_summary.get("rejected_details") or [],
            destination_summary={**dest_summary, "dest_dlq_promoted": promote_meta},
            ddl_log=ddl_log,
        )
        try:
            from services.audit_log import append_audit_event
            from services.quarantine_dlq import append_dlq_event

            append_dlq_event(
                job_id=job_id,
                action="replay",
                rows=rows_written,
                child_job_id=child_job_id,
                workspace_id=str(job.get("workspace_id") or ""),
                details={"rejected": rejected, "status": status},
            )
            actor = getattr(getattr(request, "state", None), "user", None)
            append_audit_event(
                action="quarantine.replay",
                resource=f"job:{job_id}",
                actor=str(actor or "system"),
                level="info",
                correlation_id=child_job_id,
                details={
                    "parent_job_id": job_id,
                    "child_job_id": child_job_id,
                    "rows_written": rows_written,
                    "rejected": rejected,
                    "status": status,
                },
            )
        except Exception:
            pass
        return {
            "success": True,
            "job_id": child_job_id,
            "parent_job_id": job_id,
            "rows_written": rows_written,
            "rejected": rejected,
            "rows_attempted": len(records),
            "status": status,
            "destination_summary": dest_summary,
            "dest_dlq_promoted": promote_meta,
        }
    except HTTPException:
        mongo.update_job_status(child_job_id, "failed", phase="failed", message="Quarantine replay failed")
        try:
            from services.audit_log import append_audit_event
            from services.quarantine_dlq import append_dlq_event

            append_dlq_event(
                job_id=job_id,
                action="replay_failed",
                rows=0,
                child_job_id=child_job_id,
                workspace_id=str(job.get("workspace_id") or ""),
            )
            append_audit_event(
                action="quarantine.replay_failed",
                resource=f"job:{job_id}",
                actor="system",
                level="error",
                correlation_id=child_job_id,
                details={"parent_job_id": job_id, "child_job_id": child_job_id},
            )
        except Exception:
            pass
        raise
    except Exception as e:
        mongo.update_job_status(child_job_id, "failed", phase="failed", message=str(e), error=str(e))
        try:
            from services.audit_log import append_audit_event
            from services.quarantine_dlq import append_dlq_event

            append_dlq_event(
                job_id=job_id,
                action="replay_failed",
                rows=0,
                child_job_id=child_job_id,
                workspace_id=str(job.get("workspace_id") or ""),
                details={"error": str(e)[:500]},
            )
            append_audit_event(
                action="quarantine.replay_failed",
                resource=f"job:{job_id}",
                actor="system",
                level="error",
                correlation_id=child_job_id,
                details={"parent_job_id": job_id, "error": str(e)[:500]},
            )
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e)) from e


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
async def upload_file(
    file: UploadFile = File(...),
    enable_ocr: str = Form("false"),
):
    """Upload and parse a file"""
    try:
        content = await file.read()
        use_ocr = enable_ocr.lower() in ("true", "1", "yes")
        result = FileParser.parse(content, file.filename, enable_ocr=use_ocr)

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

        from services.pdf_ocr import ocr_dependency_status

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
            "ocr_used": bool(result.ocr_used),
            "ocr_page_count": int(result.ocr_page_count or 0),
            "ocr_status": ocr_dependency_status(),
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
    priority_column: str = Form(""),
    priority_direction: str = Form("desc"),
    limit: str = Form("0"),
    backfill_new_fields: str = Form("false"),
    stream_contracts_json: str = Form(""),
    mappings_json: str = Form(""),
    data_region: str = Form(""),
    request: Request = None,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Universal file transfer — delegates to UniversalTransferEngine."""
    try:
        from ..transfer.background import run_transfer_async
        from ..transfer.engine import get_transfer_engine
        from ..transfer.models import EndpointConfig, TransferRequest

        workspace_id = _require_write_workspace(request, workspace_id)

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

        region = (
            data_region.strip()
            or getattr(request.state, "data_region", "")
            or "us-east-1"
        )
        request_obj = TransferRequest(
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
            priority_column=priority_column,
            priority_direction=priority_direction,
            limit=int(limit) if limit.isdigit() else 0,
            workspace_id=workspace_id,
            data_region=region,
            backfill_new_fields=backfill_new_fields.lower() in ("true", "1", "yes"),
        )
        if stream_contracts_json.strip():
            try:
                import json as _json
                parsed = _json.loads(stream_contracts_json)
                if isinstance(parsed, list):
                    request_obj.stream_contracts = parsed
            except Exception:
                pass
        if mappings_json.strip():
            try:
                import json as _json
                parsed = _json.loads(mappings_json)
                if isinstance(parsed, list):
                    request_obj.mappings = parsed
            except Exception:
                pass
        engine = get_transfer_engine()
        job_id = engine._create_pending_job(request_obj)

        if async_mode.lower() in ("true", "1", "yes"):
            background_tasks.add_task(run_transfer_async, job_id, request_obj)
            return {
                "success": True,
                "async": True,
                "job_id": job_id,
                "status": "running",
                "operation": request_obj.operation,
                "source": {"type": "file", "filename": file.filename, "file_type": src_fmt},
            }

        result = engine.execute_tracked(request_obj, job_id)
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
