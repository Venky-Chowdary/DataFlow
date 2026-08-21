"""
Datawrap — Connectors API Router
Manage connector configurations and data transfers
"""

import asyncio
import json
import logging
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
from pydantic import BaseModel, ConfigDict, Field
from pymongo.errors import PyMongoError
from services.runtime_estimate import estimate_for_job_doc
from services.team_store import can_read_workspace, can_write_workspace
from services.value_serializer import json_default

from ..services.file_parser import FileParser
from ..services.mongodb_service import get_mongodb_service
from ..transfer.connector_capabilities import resolve_driver_type
from ..transfer.connector_registry import run_probe

router = APIRouter(prefix="/connectors", tags=["Connectors"])
logger = logging.getLogger(__name__)


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
    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(..., description="Display name for this connector")
    type: str = Field(..., description="Connector type (mongodb, postgresql, mysql, etc.)")
    host: str = Field(..., description="Host address")
    port: int = Field(..., description="Port number")
    database: str = Field(default="", description="Database name")
    db_schema: Optional[str] = Field(default=None, alias="schema", description="Schema or dataset name")
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
    private_key: Optional[str] = Field(default=None, description="PEM private key (Snowflake key-pair / SFTP)")
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
    model_config = ConfigDict(populate_by_name=True)

    type: str
    host: Optional[str] = ""
    port: Optional[int] = None
    database: str = ""
    db_schema: Optional[str] = Field(default=None, alias="schema")
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
    private_key: Optional[str] = None


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
    from ..transfer.connector_capabilities import file_source_types
    from ..transfer.connector_registry import humanize_connection_error, probe_file_source

    try:
        driver = resolve_driver_type(request.type)
        # Resolve catalog twins (excel_workbook → excel) before the file-source
        # check. Checking the raw tile id skipped probe_file_source and claimed
        # "No connectivity probe" for Excel/CSV upload aliases.
        if (request.type or "").lower() in file_source_types() or driver in file_source_types():
            path = (request.connection_string or request.host or request.database or "").strip()
            kind = driver if driver in file_source_types() else (request.type or "")
            ok, msg = probe_file_source(kind, path)
            return {
                "success": ok,
                "message": msg,
                "details": {"format": kind, "mode": "file_source", "path": path},
            }

        from services.connector_auth import engine_login_role, infer_auth_mode, validate_probe_auth

        auth_mode = infer_auth_mode(
            auth_mode=request.auth_mode or "",
            connection_string=request.connection_string or "",
            service_account=request.service_account or "",
            api_key=request.api_key or "",
            username=request.username or "",
            password=request.password or "",
            private_key=getattr(request, "private_key", None) or "",
            driver=driver,
        )
        auth_error = validate_probe_auth(
            driver=driver,
            auth_mode=auth_mode,
            host=request.host or "",
            port=int(request.port or 0),
            database=request.database or "",
            username=request.username or "",
            password=request.password or "",
            connection_string=request.connection_string or "",
            service_account=request.service_account or "",
            api_key=request.api_key or "",
            private_key=getattr(request, "private_key", None) or "",
        )
        if auth_error:
            return {
                "success": False,
                "message": auth_error,
                "driver": driver,
                "auth_source": request.auth_source or "",
            }

        cfg = {
            "host": request.host or "",
            "port": request.port or 0,
            "database": request.database or "",
            "username": request.username or "",
            "password": request.password or "",
            "schema": request.db_schema or "",
            "connection_string": request.connection_string or "",
            "ssl": bool(request.ssl) if request.ssl is not None else False,
            "warehouse": request.warehouse or "",
            "type": request.type,
            "auth_mode": auth_mode,
            "auth_role": engine_login_role(request.auth_role),
            "role": engine_login_role(request.auth_role),
            "api_key": request.api_key or "",
            "service_account": request.service_account or "",
            "private_key": getattr(request, "private_key", None) or "",
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
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
        "schema": config.db_schema,
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
        "private_key": config.private_key,
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
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

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
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

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
    """True if the actor may *read* this job.

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


def _can_mutate_job(request: Request, job: dict) -> bool:
    """True if the actor may resume / retry / replay / export this job."""
    workspace_id = (job.get("workspace_id") or "").strip()
    if not workspace_id:
        # Legacy unscoped jobs: still require an authenticated actor (router RBAC).
        return bool(_actor_email(request))
    return can_write_workspace(workspace_id, _actor_email(request))


@router.get("/jobs")
async def list_transfer_jobs(
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """List recent transfer jobs scoped to a workspace, with whole-history counts.

    ``jobs`` is the most recent page; ``total`` and ``status_counts`` are counted
    over the entire scoped history in the store. Counting the page instead made the
    Jobs header read "All (50)" for a 90-job history and disagree with Pilot.

    Degrades gracefully when the job store is unavailable: returns an empty
    list flagged ``degraded`` (HTTP 200) so Job Theater still renders instead
    of erroring. The ``/health`` endpoint continues to report the outage, so
    the infrastructure problem is not hidden.
    """
    workspace_id = _resolve_workspace(request, workspace_id)
    try:
        mongo = get_mongodb_service()
        jobs = await asyncio.to_thread(mongo.list_jobs, workspace_id=workspace_id)
        counts = await asyncio.to_thread(mongo.count_jobs, workspace_id=workspace_id)
        return {
            "jobs": jobs,
            "count": len(jobs),
            "total": int(counts.get("total") or 0),
            "status_counts": counts.get("by_status") or {},
            "degraded": False,
        }
    except (PyMongoError, ConnectionError) as e:
        return {
            "jobs": [],
            "count": 0,
            "total": 0,
            "status_counts": {},
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
        from ..transfer.models import sanitize_job_for_api

        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)

        if not job or not _can_access_job(request, job):
            raise HTTPException(status_code=404, detail="Job not found")

        for key in ("created_at", "updated_at", "started_at", "completed_at"):
            if job.get(key) and hasattr(job[key], "isoformat"):
                job[key] = job[key].isoformat()
        safe = sanitize_job_for_api(job)
        safe["runtime_estimate"] = estimate_for_job_doc(job)
        return safe
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

        if not _can_mutate_job(request, job):
            raise HTTPException(status_code=403, detail="Workspace write access required")
        if not mongo.update_job_fields(job_id, {"name": name, "name_key": _job_name_key(name)}):
            raise HTTPException(status_code=500, detail="Failed to update job")
        from ..transfer.models import sanitize_job_for_api

        updated = mongo.get_job(job_id) or {**job, "name": name}
        for key in ("created_at", "updated_at", "started_at", "completed_at"):
            if updated.get(key) and hasattr(updated[key], "isoformat"):
                updated[key] = updated[key].isoformat()
        return sanitize_job_for_api(updated)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/retry")
async def retry_transfer_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    request: Request,
    force: bool = False,
):
    """Re-run a failed transfer from the beginning as a new job (no checkpoint).

    Use ``/resume`` to continue the *same* job from its last committed batch.
    A from-zero retry re-reads the whole source, so it is refused when the
    failed attempt already committed rows under a sync mode that has no key to
    collapse a second copy. ``force=true`` is the operator's explicit
    acknowledgement that the duplicates are acceptable and is recorded on the
    new job.
    """
    try:
        from ..transfer.background import run_transfer_async
        from ..transfer.engine import get_transfer_engine
        from ..transfer.models import transfer_request_from_dict

        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)
        if not job or not _can_access_job(request, job):
            raise HTTPException(status_code=404, detail="Job not found")
        if not _can_mutate_job(request, job):
            raise HTTPException(status_code=403, detail="Workspace write access required")

        payload = job.get("transfer_request")
        if not payload:
            raise HTTPException(
                status_code=400,
                detail="This job has no saved configuration — re-run from Transfer Studio.",
            )
        xfer_req = transfer_request_from_dict(payload)
        from services.transfer_file_staging import (
            file_source_bytes_available,
            hydrate_file_source,
        )

        hydrate_file_source(xfer_req)
        if xfer_req.source.kind == "file" and not file_source_bytes_available(xfer_req):
            raise HTTPException(
                status_code=400,
                detail="File uploads must be re-submitted from Transfer Studio.",
            )

        from services.execution_engine_contract import (
            committed_rows_of,
            decide_retry_from_start,
        )

        rows_committed, rows_known = committed_rows_of(job)
        retry_decision = decide_retry_from_start(
            status=job.get("status"),
            sync_mode=getattr(xfer_req, "sync_mode", ""),
            rows_committed=rows_committed,
            rows_committed_known=rows_known,
        )
        if not retry_decision["allowed"] and not force:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "Retry from start refused — it would duplicate committed rows",
                    "reason": retry_decision["reason"],
                    "rows_committed": rows_committed if rows_known else None,
                    "rows_committed_known": rows_known,
                    "primary_action": "resume",
                    "resume_url": f"/api/v1/connectors/jobs/{job_id}/resume",
                    "override": "Re-send with force=true to accept the duplicates.",
                },
            )
        # Retries from start also re-run preflight — never inherit skip_preflight.
        xfer_req.skip_preflight = False
        engine = get_transfer_engine()
        new_job_id = engine._create_pending_job(xfer_req)
        forced = bool(force and not retry_decision["allowed"])
        mongo.update_job_status(
            new_job_id,
            "pending",
            retry_of=job_id,
            message=(
                f"Retry from start of job {job_id} (no checkpoint)"
                + (
                    " — operator accepted duplicate rows: "
                    f"{retry_decision['reason']}"
                    if forced
                    else ""
                )
            ),
            duplicate_risk_acknowledged=forced,
            retry_decision=retry_decision,
        )

        # From-zero: do not copy parent checkpoint / resume_from_job_id.
        background_tasks.add_task(run_transfer_async, new_job_id, xfer_req, resume=False)
        return {
            "success": True,
            "async": True,
            "job_id": new_job_id,
            "retry_of": job_id,
            "status": "running",
            "resume": False,
            "duplicate_risk_acknowledged": forced,
            "retry_decision": retry_decision,
            "message": (
                "Retry from start — new job, source re-read from the beginning "
                "(at-least-once)."
            ),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/resume")
def _resume_restarts_from_scratch(xfer_req: Any) -> bool:
    """True when Resume should re-run the transfer rather than continue it.

    A full refresh replaces the destination instead of adding to it, so there is
    no partial state to resume into and re-running is idempotent by definition.
    Continuing mid-stream is the case that needs an identity key, to make the
    interrupted batch's replay idempotent — and a keyless source such as an
    ordinary CSV export can never supply one, which left Resume as an action
    that could only ever refuse itself.
    """
    from services.sync_cursor import is_overwrite_sync

    contracts = getattr(xfer_req, "stream_contracts", None) or []
    for contract in contracts:
        if isinstance(contract, dict) and contract.get("sync_mode"):
            if not is_overwrite_sync(str(contract.get("sync_mode"))):
                return False
    return is_overwrite_sync(str(getattr(xfer_req, "sync_mode", "") or "")) or bool(
        contracts
        and all(
            isinstance(c, dict) and is_overwrite_sync(str(c.get("sync_mode") or ""))
            for c in contracts
        )
    )


async def resume_transfer_job(job_id: str, background_tasks: BackgroundTasks, request: Request):
    """Resume a failed or paused transfer from its last durable checkpoint."""
    try:
        from ..transfer.background import run_transfer_async
        from ..transfer.models import transfer_request_from_dict

        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)
        if not job or not _can_access_job(request, job):
            raise HTTPException(status_code=404, detail="Job not found")
        if not _can_mutate_job(request, job):
            raise HTTPException(status_code=403, detail="Workspace write access required")

        payload = job.get("transfer_request")
        if not payload:
            raise HTTPException(
                status_code=400,
                detail="This job has no saved configuration — re-run from Transfer Studio.",
            )
        xfer_req_probe = transfer_request_from_dict(payload)
        from services.transfer_file_staging import (
            file_source_bytes_available,
            hydrate_file_source,
        )

        hydrate_file_source(xfer_req_probe)
        if xfer_req_probe.source.kind == "file" and not file_source_bytes_available(
            xfer_req_probe
        ):
            raise HTTPException(
                status_code=400,
                detail="File uploads must be re-submitted from Transfer Studio.",
            )

        from services.checkpoint_service import Checkpoint, evaluate_resume_safety

        cp_raw = job.get("checkpoint")
        safety = evaluate_resume_safety(
            Checkpoint.from_dict(cp_raw) if isinstance(cp_raw, dict) else cp_raw,
            job=job,
        )
        if not safety.get("ok"):
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "Resume refused — checkpoint not safe",
                    "reasons": safety.get("reasons") or [],
                    "warnings": safety.get("warnings") or [],
                    "age_hours": safety.get("age_hours"),
                    "honesty": safety.get("honesty"),
                },
            )

        xfer_req = xfer_req_probe
        # Resume must never inherit a stale skip_preflight flag — gates re-run.
        xfer_req.skip_preflight = False
        # A full refresh has nothing to resume *into*: it replaces the
        # destination rather than adding to it, so the safe continuation is to
        # run it again from the top. Continuing mid-file needs an identity key to
        # make the replay idempotent, and a keyless source — an ordinary CSV
        # export — can never supply one. Operators hit exactly that wall: Resume
        # was the only action offered on a failed 1M-row overwrite, and it
        # answered by demanding a key the sync mode never needed.
        # A CDC cursor gap is the same class: the durable cursor is the problem,
        # so Resume restarts (when_needed snapshots current keys) rather than
        # polling the purged LSN from the last checkpoint.
        restart_full_refresh = _resume_restarts_from_scratch(xfer_req) or bool(
            safety.get("gap_restart")
        )
        # Resume is the one sanctioned exit from a terminal status, and it must
        # also drop any stale cancel request or the resumed run would abort at
        # its first checkpoint.
        mongo.clear_job_cancel(job_id)
        mongo.update_job_status(
            job_id,
            "pending",
            message=f"Resume requested for job {job_id}",
            allow_terminal_exit=True,
        )
        background_tasks.add_task(
            run_transfer_async, job_id, xfer_req, resume=not restart_full_refresh
        )
        return {
            "success": True,
            "async": True,
            "job_id": job_id,
            "status": "running",
            "resume": not restart_full_refresh,
            "restarted": restart_full_refresh,
            "message": (
                "CDC cursor-gap recovery restarted. Purged-window events are gone. "
                "when_needed snapshots current source keys, then streams from the new tip. "
                "At-least-once upsert — not continuous CDC, not migration_proven."
                if safety.get("gap_restart")
                else "Full refresh restarted from the beginning — it replaces the "
                "destination, so there is nothing to resume into."
                if restart_full_refresh
                else "Resume started from last committed checkpoint (at-least-once upsert)."
            ),
            "checkpoint_age_hours": safety.get("age_hours"),
            "warnings": safety.get("warnings") or [],
            "honesty": safety.get("honesty"),
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
        if not _can_mutate_job(request, job):
            raise HTTPException(status_code=403, detail="Workspace write access required")
        if job.get("status") in ("completed", "completed_with_quarantine", "failed", "cancelled"):
            return {"success": True, "job_id": job_id, "status": job.get("status"), "message": "Job already terminal"}
        # Durable intent flag first, status second. The flag is what the worker
        # loop actually consults, and unlike `status` nothing in the progress
        # path ever overwrites it — so a cancel cannot be lost to a race with
        # the worker's next chunk update.
        mongo.request_job_cancel(job_id)
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
        from ..transfer.models import sanitize_job_for_api

        mongo = get_mongodb_service()
        while True:
            job = mongo.get_job(job_id)
            if not job:
                yield f"event: error\ndata: {json.dumps({'error': 'Job not found'}, default=json_default)}\n\n"
                break
            for key in ("created_at", "updated_at", "started_at", "completed_at"):
                if job.get(key) and hasattr(job[key], "isoformat"):
                    job[key] = job[key].isoformat()
            safe = sanitize_job_for_api(job)
            safe["runtime_estimate"] = estimate_for_job_doc(job)
            yield f"data: {json.dumps(safe, default=json_default)}\n\n"
            if safe.get("status") in ("completed", "completed_with_quarantine", "failed", "cancelled"):
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
    from services.quarantine_dlq import (
        evaluate_replay_closure,
        job_quarantine_closure,
    )

    stored_closure = job_quarantine_closure(job)
    closure = evaluate_replay_closure(details, last_replay=(stored_closure or {}).get("last_replay"))
    if stored_closure:
        closure = {
            **stored_closure,
            "open_count": closure["open_count"],
            "promoted_count": closure["promoted_count"],
            "failed_count": closure["failed_count"],
            "durable_count": closure["durable_count"],
            "verdict": closure["verdict"],
            "next_action": closure["next_action"],
            "note": closure["note"],
            "migration_proven": False,
        }
    open_n = int(closure.get("open_count") or 0)
    row_ids = {d.get("row") for d in details if isinstance(d, dict) and d.get("row") is not None}
    rejected_rows = int(
        job.get("rejected_details_total")
        or job.get("rejected_rows")
        or 0
    ) or (len(row_ids) if row_ids else len(details))
    has_write = bool(
        job.get("rejected_details")
        or (job.get("destination_summary") or {}).get("rejected_details")
    )
    source = "write" if has_write else ("preflight" if details else "none")
    # DLQ hydrate when job sample was truncated / incomplete.
    if details and (
        job.get("rejected_details_truncated")
        or int(job.get("rejected_details_total") or 0) > len(job.get("rejected_details") or [])
        or len(details) > len(job.get("rejected_details") or [])
    ):
        source = "dlq" if has_write or source == "none" else source
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
    # Control-plane JSONL durability is independent of the destination-table
    # DLQ. Module 5: persist failure fail-closes the job (quarantine_durable=False
    # never pairs with a successful terminal status when rejects exist).
    quarantine_durable = ds.get("quarantine_durable")
    if quarantine_durable is None and rejected_rows:
        # Older jobs wrote rejects without the flag — treat as unknown, not lost.
        quarantine_durable = None
    quarantine_dlq_error = ds.get("quarantine_dlq_error")
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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    return {
        "job_id": job_id,
        "rejected_rows": rejected_rows,
        "issue_count": len(details),
        "open_count": open_n,
        "source": source,
        "quarantine": details,
        "dest_dlq": dest_dlq,
        "quarantine_durable": quarantine_durable,
        "quarantine_dlq_error": quarantine_dlq_error,
        "quarantine_closure": {
            "verdict": closure.get("verdict"),
            "open_count": open_n,
            "promoted_count": int(closure.get("promoted_count") or 0),
            "failed_count": int(closure.get("failed_count") or 0),
            "durable_count": int(closure.get("durable_count") or 0),
            "next_action": closure.get("next_action") or "",
            "note": closure.get("note") or "",
            "migration_proven": False,
        },
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
    if not _can_mutate_job(request, job):
        raise HTTPException(status_code=403, detail="Workspace write access required")

    from services.quarantine_from_preflight import merge_job_quarantine
    from services.pii_guard import redact_destination_summary

    details = merge_job_quarantine(job)
    if not details:
        return {"success": True, "row_count": 0, "download_url": "", "filename": ""}

    # Export only redacted samples — never cleartext PII from dual-stamped scraps.
    mappings = (job.get("transfer_request") or {}).get("mappings") or []
    redacted = redact_destination_summary({"rejected_details": details}, mappings)
    details = redacted.get("rejected_details") or details

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
    """Group rejected_details by row index into records for rewrite.

    Values may be source-shaped (transform quarantine) or target-shaped
    (write-matrix quarantine). Callers canonicalize to source keys before
    ``write_destination_database``.
    """
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
        # Prefer explicit source_values when dual-stamped (Wave 32).
        base = detail.get("source_values") if isinstance(detail.get("source_values"), dict) else None
        if not base:
            base = detail.get("values") if isinstance(detail.get("values"), dict) else {}
        if base:
            from connectors.writer_common import quarantine_cell_wire

            for k, v in base.items():
                by_row[row_num].setdefault(str(k), quarantine_cell_wire(v))
        col = str(detail.get("column") or "").strip()
        if col:
            from connectors.writer_common import quarantine_cell_wire

            by_row[row_num][col] = quarantine_cell_wire(detail.get("value"))
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


def _canonicalize_quarantine_records_to_source(
    records: list[dict],
    mappings: list[dict],
) -> tuple[list[dict], list[str]]:
    """Project quarantine values onto source column names for rewrite.

    Write quarantine stamps target keys; transform quarantine stamps source
    keys. Mapping rewrite always reads source → target, so target-only scraps
    would otherwise insert NULL for every mapped field.
    """
    pairs: list[tuple[str, str]] = []
    for m in mappings or []:
        src = str(m.get("source") or m.get("source_column") or "").strip()
        tgt = str(m.get("target") or m.get("target_column") or "").strip() or src
        if src:
            pairs.append((src, tgt))
    if not pairs:
        cols: list[str] = []
        seen: set[str] = set()
        for rec in records:
            for k in rec:
                if k not in seen:
                    seen.add(k)
                    cols.append(k)
        return records, cols

    out: list[dict] = []
    for rec in records:
        from connectors.writer_common import quarantine_cell_wire

        shaped: dict[str, str] = {}
        for src, tgt in pairs:
            if src in rec:
                shaped[src] = quarantine_cell_wire(rec[src])
            elif tgt in rec:
                shaped[src] = quarantine_cell_wire(rec[tgt])
            else:
                # Keep absence visible to refuse-incomplete (key missing).
                continue
        # Preserve unmapped extras (operator edits) under their original keys.
        for k, v in rec.items():
            if k not in shaped and all(k != t for _, t in pairs):
                shaped[str(k)] = quarantine_cell_wire(v)
        out.append(shaped)
    columns: list[str] = []
    seen_c: set[str] = set()
    for rec in out:
        for k in rec:
            if k not in seen_c:
                seen_c.add(k)
                columns.append(k)
    # Ensure mapped source columns appear in column order even when sparse.
    for src, _tgt in pairs:
        if src not in seen_c:
            columns.append(src)
            seen_c.add(src)
    return out, columns


def _refuse_incomplete_quarantine_replay(
    records: list[dict],
    mappings: list[dict],
) -> None:
    """Fail-closed when quarantine scraps cannot reconstruct a mapped row.

    Transform quarantine stamps full source-shaped ``values``; write matrix
    holdouts stamp target-shaped ``values`` (``append_write_quarantine_detail``).
    Accept either shape so target-keyed scraps are not false-refused — then
    canonicalize to source before rewrite. Single-column scraps would insert
    NULL/empty for other mapped fields — refuse rather than silent data loss.
    """
    pairs: list[tuple[str, str]] = []
    for m in mappings or []:
        src = str(m.get("source") or m.get("source_column") or "").strip()
        tgt = str(m.get("target") or m.get("target_column") or "").strip() or src
        if src:
            pairs.append((src, tgt))
    if len(pairs) < 2:
        return
    min_required = max(2, (len(pairs) + 1) // 2)
    for i, rec in enumerate(records):
        # Empty-string cells are valid (Oracle '' / intentional blank) — key must exist.
        present = sum(1 for src, tgt in pairs if src in rec or tgt in rec)
        if present < min_required:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Quarantine row {i + 1} is incomplete for replay "
                    f"({present}/{len(pairs)} mapped columns). "
                    "Re-run the parent transfer so quarantine stamps full "
                    "``values``, then edit and replay."
                ),
            )


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
    if not _can_mutate_job(request, job):
        raise HTTPException(status_code=403, detail="Workspace write access required")

    payload = job.get("transfer_request")
    if not payload:
        raise HTTPException(
            status_code=400,
            detail="This job has no saved configuration — cannot replay quarantine.",
        )

    stored_details = job.get("rejected_details") or job.get("destination_summary", {}).get("rejected_details") or []
    from services.quarantine_from_preflight import merge_job_quarantine
    from services.quarantine_dlq import (
        compact_replay_closure,
        job_quarantine_closure,
        open_quarantine_details,
        quarantine_sample_incomplete,
        record_replay,
        replay_row_identity,
        VERDICT_CLOSED,
    )

    hydrated = merge_job_quarantine(job)
    durable = hydrated or list(stored_details)
    incomplete = quarantine_sample_incomplete(job, durable)
    if incomplete:
        raise HTTPException(status_code=400, detail=incomplete)

    promoted_ids = {
        replay_row_identity(d)
        for d in durable
        if str(d.get("retry_status") or "").lower() == "promoted"
    }
    if body.rows:
        details = [
            d for d in body.rows
            if isinstance(d, dict) and replay_row_identity(d) not in promoted_ids
        ]
    else:
        details = open_quarantine_details(durable)
    if not details:
        raise HTTPException(
            status_code=400,
            detail=(
                "Quarantine ledger is closed — remediations already Gate-8 promoted. "
                "Replay would only re-upsert already-landed keys."
                if promoted_ids or str((job_quarantine_closure(job) or {}).get("verdict") or "") == VERDICT_CLOSED
                else "No quarantine rows to replay"
            ),
        )
    prior_closure = job_quarantine_closure(job) or {}

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

    _refuse_incomplete_quarantine_replay(records, mappings)
    from services.cdc_exactly_once import (
        ExactlyOnceRouteError,
        assert_cdc_eos_quarantine_replay,
        dest_view_from_job_summary,
    )

    try:
        assert_cdc_eos_quarantine_replay(
            details=list(details),
            dest=dest_view_from_job_summary(job),
        )
    except ExactlyOnceRouteError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    records, columns = _canonicalize_quarantine_records_to_source(records, mappings)

    schema = dict(transfer_req.column_types or {})
    for c in columns:
        schema.setdefault(c, "string")

    dest = transfer_req.destination
    if dest.kind == "file_export":
        raise HTTPException(status_code=400, detail="Quarantine replay is not supported for file_export destinations")

    # Prefer stream-contract / identity PK — never hardcode only id/_id
    # (Mongo→SQL users use `_id`→`id`, but many routes use user_id / code).
    conflict_columns: list[str] = []
    try:
        from services.primary_key import resolve_primary_key_target
        from services.sync_cursor import map_source_to_target, resolve_sync_contract

        contract = resolve_sync_contract(transfer_req.stream_contracts)
        if contract and contract.primary_key:
            conflict_columns = [
                map_source_to_target(col, mappings)
                for col in contract.primary_key_columns()
            ]
        if not conflict_columns:
            pk_tgt = resolve_primary_key_target(
                mappings,
                (dest.format or "").lower(),
                validation_mode=transfer_req.validation_mode or "balanced",
            )
            if pk_tgt:
                conflict_columns = [pk_tgt]
        if not conflict_columns:
            conflict_columns = [
                m.get("target") or m.get("target_column") or m.get("source")
                for m in mappings
                if (m.get("source") or "").lower() in {"id", "_id"}
                or (m.get("target") or "").lower() in {"id", "_id"}
            ]
        conflict_columns = [c for c in conflict_columns if c]
    except Exception as exc:
        logger.warning("quarantine replay PK resolve failed: %s", exc, exc_info=exc)
        conflict_columns = []

    write_mode = "upsert" if conflict_columns else "insert"
    if write_mode == "insert":
        # Refuse silent insert replay — partial loads + insert duplicates rows.
        # Operator must set primary key on Map / stream contract first.
        raise HTTPException(
            status_code=400,
            detail=(
                "Quarantine replay needs a primary key (stream contract or "
                "id/_id mapping) so rows upsert instead of duplicating. "
                "Set primary_key on Map, then replay."
            ),
        )

    engine = get_transfer_engine()
    # Child job: upsert remediations (never full-refresh overwrite).
    # sync_mode must match write_mode so Gate-8 allow_extra / sample proof
    # treats this as keyed upsert — not soft append.
    child_payload = dict(payload)
    child_payload["sync_mode"] = "incremental_deduped"
    child_payload["skip_preflight"] = True
    child_req = transfer_request_from_dict(child_payload)
    from src.transfer.contract_engine import enforce_bound_contract, stamp_request_contract

    try:
        from services.data_contract import ContractViolation
    except ImportError:  # pragma: no cover
        from src.services.data_contract import ContractViolation

    try:
        stamp_request_contract(
            child_req,
            explicit_id=str(getattr(transfer_req, "contract_id", "") or ""),
            explicit_require=bool(getattr(transfer_req, "require_signed_contract", False)),
        )
        enforce_bound_contract(
            child_req,
            schema=schema,
            mappings=mappings,
        )
    except (ValueError, ContractViolation) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
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
        rows_written, ddl_log, dest_summary = write_destination_database(
            dest,
            records,
            columns,
            schema,
            mappings,
            validation_mode=transfer_req.validation_mode or "balanced",
            backfill_new_fields=bool(transfer_req.backfill_new_fields),
            write_mode=write_mode,
            conflict_columns=conflict_columns or None,
        )
        rejected = int(dest_summary.get("rejected_rows") or 0)
        # Gate-8 on the child job — Airbyte-class post-write integrity (count +
        # checksum / keyed sample). Upsert into a live table may have extras;
        # sync_mode + reconcile_sample make allow_extra + sample proof honest.
        dest_summary = dict(dest_summary or {})
        dest_summary["sync_mode"] = "incremental_deduped"
        dest_summary["source_row_count"] = len(records)
        dest_summary["reconcile_sample"] = records[:50]
        recon: dict[str, Any] = {}
        try:
            from ..transfer.reconcile_step import run_reconciliation

            recon = run_reconciliation(
                endpoint=dest,
                records=records,
                columns=columns,
                rows_written=rows_written,
                writer_checksum=str(
                    dest_summary.get("checksum")
                    or dest_summary.get("active_checksum")
                    or ""
                ),
                dest_summary=dest_summary,
                mappings=mappings,
                source_schema=schema,
                validation_mode=transfer_req.validation_mode or "balanced",
            )
        except Exception as exc:
            logger.warning(
                "quarantine replay Gate-8 failed closed: %s", exc, exc_info=exc
            )
            recon = {
                "passed": False,
                "message": f"Gate-8 reconciliation error: {exc}",
                "phase": "post_write_failed",
            }

        if rejected > 0:
            status = "completed_with_quarantine"
        elif not recon.get("passed", True):
            status = "failed"
        else:
            status = "completed"

        recorded = record_replay(
            durable,
            attempted=details,
            child_rejected=list(dest_summary.get("rejected_details") or []),
            gate8_passed=bool(recon.get("passed")),
            child_job_id=child_job_id,
            rows_written=int(rows_written or 0),
            rejected=rejected,
            prior=prior_closure,
        )
        compact = compact_replay_closure(recorded)
        stamped_findings = list(recorded.get("findings") or [])
        landed_ids = set(recorded.get("promoted_identities") or [])

        promote_meta: dict[str, Any] = {}
        if recon.get("passed") and landed_ids:
            try:
                from services.dest_quarantine import mark_dlq_promoted

                qids = [
                    str(d.get("_df_qid") or "")
                    for d in details
                    if isinstance(d, dict)
                    and d.get("_df_qid")
                    and replay_row_identity(d) in landed_ids
                ]
                # Full close without qids: stamp every open dest DLQ row for this job.
                # Partial close without qids: refuse dest stamp rather than mark poison pills.
                failed_ids = set(recorded.get("failed_identities") or [])
                if qids:
                    promote_meta = mark_dlq_promoted(dest, qids=qids, job_id=job_id)
                elif not failed_ids:
                    promote_meta = mark_dlq_promoted(dest, qids=[], job_id=job_id)
                else:
                    promote_meta = {
                        "updated": 0,
                        "skipped": True,
                        "reason": "partial promote needs _df_qid on each finding",
                    }
            except Exception as exc:
                promote_meta = {"error": str(exc)[:300]}
        phase = "completed" if status != "failed" else "failed"
        mongo.update_job_status(
            child_job_id,
            status,
            phase=phase,
            message=(
                recon.get("message")
                if status == "failed"
                else f"Quarantine replay wrote {rows_written} row(s)"
            ),
            records_processed=rows_written,
            progress_pct=100 if status != "failed" else 99,
            rejected_rows=rejected,
            rejected_details=dest_summary.get("rejected_details") or [],
            destination_summary={**dest_summary, "dest_dlq_promoted": promote_meta},
            reconciliation=recon,
            ddl_log=ddl_log,
            error=recon.get("message") if status == "failed" else None,
        )
        parent_ds = dict(job.get("destination_summary") or {})
        parent_ds["quarantine_closure"] = compact
        parent_status = str(job.get("status") or "completed_with_quarantine")
        try:
            mongo.update_job_status(
                job_id,
                parent_status,
                quarantine_closure=compact,
                rejected_details=stamped_findings[:2000],
                destination_summary=parent_ds,
            )
        except Exception as exc:
            logger.warning("parent quarantine_closure persist failed: %s", exc, exc_info=exc)
        try:
            from services.audit_log import append_audit_event
            from services.quarantine_dlq import append_dlq_event

            append_dlq_event(
                job_id=job_id,
                action="replay",
                rows=rows_written,
                child_job_id=child_job_id,
                workspace_id=str(job.get("workspace_id") or ""),
                details={
                    "rejected": rejected,
                    "status": status,
                    "gate8_passed": bool(recon.get("passed")),
                    "verdict": compact.get("verdict"),
                    "open_count": compact.get("open_count"),
                },
            )
            append_dlq_event(
                job_id=job_id,
                action="replay_closure",
                rows=int(compact.get("promoted_count") or 0),
                child_job_id=child_job_id,
                workspace_id=str(job.get("workspace_id") or ""),
                details={
                    "verdict": compact.get("verdict"),
                    "open_count": compact.get("open_count"),
                    "promoted_count": compact.get("promoted_count"),
                    "failed_count": compact.get("failed_count"),
                    "gate8_passed": bool(recon.get("passed")),
                    "promoted_identities": list(recorded.get("promoted_identities") or []),
                    "failed_identities": list(recorded.get("failed_identities") or []),
                    "migration_proven": False,
                },
            )
            actor = getattr(getattr(request, "state", None), "user", None)
            append_audit_event(
                action="quarantine.replay",
                resource=f"job:{job_id}",
                actor=str(actor or "system"),
                level="info" if status != "failed" else "error",
                correlation_id=child_job_id,
                details={
                    "parent_job_id": job_id,
                    "child_job_id": child_job_id,
                    "rows_written": rows_written,
                    "rejected": rejected,
                    "status": status,
                    "gate8_passed": bool(recon.get("passed")),
                    "gate8_phase": recon.get("phase"),
                    "verdict": compact.get("verdict"),
                    "open_count": compact.get("open_count"),
                },
            )
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        return {
            "success": status != "failed",
            "job_id": child_job_id,
            "parent_job_id": job_id,
            "rows_written": rows_written,
            "rejected": rejected,
            "rows_attempted": len(records),
            "status": status,
            "destination_summary": dest_summary,
            "dest_dlq_promoted": promote_meta,
            "reconciliation": recon,
            "quarantine_closure": {
                "verdict": compact.get("verdict"),
                "open_count": compact.get("open_count"),
                "promoted_count": compact.get("promoted_count"),
                "failed_count": compact.get("failed_count"),
                "durable_count": compact.get("durable_count"),
                "next_action": compact.get("next_action") or "",
                "note": compact.get("note") or "",
                "migration_proven": False,
            },
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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/{connector_id}")
async def get_connector(connector_id: str):
    """Get a specific connector"""
    try:
        from services.mongodb_service import redact_connector_secrets

        mongo = get_mongodb_service()
        connector = mongo.get_connector(connector_id)

        if not connector:
            raise HTTPException(status_code=404, detail="Connector not found")

        return redact_connector_secrets(connector)

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
    read_options_json: str = Form(""),
):
    """Upload and parse a file, through the declared read window if any."""
    from services.read_options import ReadOptionsError, parse_read_options_payload

    try:
        read_options = parse_read_options_payload(read_options_json)
    except ReadOptionsError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        content = await file.read()
        use_ocr = enable_ocr.lower() in ("true", "1", "yes")
        result = FileParser.parse(
            content,
            file.filename,
            enable_ocr=use_ocr,
            read_options=read_options,
        )

        if not result.success:
            raise HTTPException(status_code=400, detail=result.error)

        if result.row_count == 0:
            detail = "File contains no records"
            if not read_options.is_default:
                detail += f" through the declared read window ({read_options.describe()})"
            raise HTTPException(status_code=400, detail=detail)

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

        sheets: list[dict] = []
        if result.file_type == "excel":
            from services.excel_parser import list_excel_sheets

            sheets = list_excel_sheets(content)

        return {
            "success": True,
            "filename": file.filename,
            "file_type": result.file_type,
            "sheets": sheets,
            "read_options": read_options.to_wire(),
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
            skip_preflight=False,
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
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        if mappings_json.strip():
            try:
                import json as _json
                parsed = _json.loads(mappings_json)
                if isinstance(parsed, list):
                    request_obj.mappings = parsed
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
