"""
Datawrap — Connectors API Router
Manage connector configurations and data transfers
"""

import asyncio
import json
import logging
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
from pydantic import BaseModel, ConfigDict, Field, model_validator
from pymongo.errors import PyMongoError
from services.runtime_estimate import estimate_for_job_doc
from services.workspace_access import (
    assert_resource_workspace,
    resolve_write_workspace,
    tenant_scope,
)
from services.value_serializer import json_default
from services.upload_limits import PayloadTooLarge, read_upload
from services.connector_telemetry import connector_operation, driver_family
from services.rbac import require_admin_connector_access

from ..services.file_parser import FileParser
from ..services.mongodb_service import get_mongodb_service
from ..transfer.connector_capabilities import resolve_driver_type
from ..transfer.connector_registry import run_probe

router = APIRouter(prefix="/connectors", tags=["Connectors"])
from services.ops_events import emit_event

logger = logging.getLogger(__name__)


def _refresh_saved_connectors(xfer_req: Any) -> None:
    """Replay a stored request against the saved connectors' current settings (D172)."""
    from ..transfer.adapters import refresh_saved_connectors

    try:
        refresh_saved_connectors(xfer_req)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


def _job_contract_flags(xfer_req: Any) -> tuple[bool, bool]:
    """Whether the saved transfer request declares a merge key or a cursor.

    The rerun/retry duplicate gate must check the *contract*, not the mode
    family (C20): a declared primary/merge key collapses a second copy, and a
    declared cursor means an incremental rerun continues from the persisted
    watermark instead of re-reading committed rows.
    """
    try:
        from services.sync_cursor import resolve_sync_contract

        contract = resolve_sync_contract(
            list(getattr(xfer_req, "stream_contracts", None) or [])
        )
    except Exception:  # noqa: BLE001 - a malformed contract must not 500 the route
        contract = None
    read_options = getattr(xfer_req, "read_options", None) or {}
    source_filter = getattr(xfer_req, "source_filter", None) or {}

    def _field(*names: str) -> str:
        for holder in (contract, read_options, source_filter):
            if holder is None:
                continue
            for name in names:
                try:
                    if isinstance(holder, dict):
                        val = holder.get(name)
                    else:
                        val = getattr(holder, name, None)
                except Exception:  # noqa: BLE001
                    val = None
                if val:
                    return str(val)
        return ""

    has_key = bool(
        _field("primary_key", "dedupe_key", "upsert_key", "merge_key")
        or (contract and getattr(contract, "primary_key", None))
    )
    has_cursor = bool(_field("cursor_field", "cursor", "cursor_column"))
    return has_key, has_cursor


def _actor_email(request: Request) -> str:
    return getattr(request.state, "user_email", None) or "anonymous"


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
    group_id: Optional[str] = Field(
        default=None,
        description="Kafka source consumer group (empty: a group per pipeline route)",
    )

    @model_validator(mode="after")
    def _request_bounds(self) -> "ConnectorConfig":
        from services.connector_request_limits import connector_request_errors

        err = connector_request_errors(
            name=self.name,
            db_type=self.type,
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            schema=self.db_schema,
            connection_string=self.connection_string,
            require_name=True,
        )
        if err:
            raise ValueError(err)
        return self


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
    # SFTP host-key trust and SQL Server self-signed TLS — the saved connector
    # stores these under ``extra``; the in-form probe must see them too or the
    # Test button fails where a saved transfer would pass.
    host_key: Optional[str] = None
    known_hosts: Optional[str] = None
    host_key_policy: Optional[str] = None
    trust_server_certificate: Optional[bool] = None


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
async def test_connection(request: TestConnectionRequest, http_request: Request):
    require_admin_connector_access(http_request, request.type)
    with connector_operation(
        "probe",
        driver_family="unknown",
        surface="connectors_test",
    ) as op:
        return await _test_connection_impl(request, op)


async def _test_connection_impl(request: TestConnectionRequest, op):
    """Test a connector configuration before saving"""
    from ..transfer.connector_capabilities import file_source_types
    from ..transfer.connector_registry import humanize_connection_error, probe_file_source

    try:
        driver = resolve_driver_type(request.type)
        op.update(driver_family=driver_family(driver))
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
            op.failed(category="auth_config")
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
            "host_key": request.host_key or "",
            "known_hosts": request.known_hosts or "",
            "host_key_policy": request.host_key_policy or "",
            "trust_server_certificate": bool(request.trust_server_certificate),
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
        op.errored(e)
        return {
            "success": False,
            "message": humanize_connection_error(
                resolve_driver_type(request.type or ""), e,
                auth_mode=request.auth_mode or "",
            ),
        }


def _refuse_unsafe_egress(data: dict[str, Any]) -> None:
    """Refuse to persist a connector whose dial targets break the egress policy.

    Audit-first for private hosts and never applied to an unchanged target
    (``connector_save_refusal``); metadata and link-local always refuse.
    """
    from services.egress_guard import connector_save_refusal

    refusal = connector_save_refusal(data)
    if refusal:
        raise HTTPException(status_code=400, detail=refusal)


@router.post("/", response_model=ConnectorResponse)
async def create_connector(
    config: ConnectorConfig,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Create and save a new connector configuration (file store + MongoDB when available)."""
    workspace_id = resolve_write_workspace(request, workspace_id)
    require_admin_connector_access(request, config.type)
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
        "group_id": config.group_id,
        "workspace_id": workspace_id,
        "status": "configured",
    }
    _refuse_unsafe_egress(connector_data)

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
    """List saved connectors the caller's tenant scope covers (D154)."""
    scope = tenant_scope(request, workspace_id)
    try:
        import sys
        from pathlib import Path
        _api_root = Path(__file__).resolve().parents[2]
        if str(_api_root) not in sys.path:
            sys.path.insert(0, str(_api_root))
        from services.connector_store import connector_ui_status, list_connectors as fs_list, normalize_connector_role
        items = [c for c in fs_list() if scope.allows(c.workspace_id)]
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
                        "status": connector_ui_status(c),
                        "created_at": c.created_at,
                        "last_test_ok": c.last_test_ok,
                        "last_tested_at": c.last_tested_at,
                        "last_transfer_ok_at": c.last_transfer_ok_at,
                        "workspace_id": c.workspace_id or "",
                        "role": normalize_connector_role(
                            getattr(c, "type", "") or "", getattr(c, "role", None)
                        ),
                    }
                    for c in items
                ],
                "count": len(items),
            }
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    try:
        from services.connector_store import connector_ui_status, normalize_connector_role

        mongo = get_mongodb_service()
        connectors = mongo.list_connectors()

        result = []
        for c in connectors:
            if not scope.allows(c.get("workspace_id")):
                continue
            created = c.get("created_at")
            result.append({
                "id": c["_id"],
                "name": c["name"],
                "type": c["type"],
                "host": c.get("host", ""),
                "port": c.get("port", 0),
                "database": c.get("database", ""),
                "status": connector_ui_status(c),
                "created_at": created.isoformat() if created and hasattr(created, "isoformat") else created,
                "last_test_ok": c.get("last_test_ok"),
                "last_tested_at": c.get("last_tested_at"),
                "last_transfer_ok_at": c.get("last_transfer_ok_at"),
                "workspace_id": c.get("workspace_id", ""),
                "role": normalize_connector_role(
                    str(c.get("type") or ""), c.get("role")
                ),
            })

        return {"connectors": result, "count": len(result)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _can_access_job(request: Request, job: dict) -> bool:
    """True if the actor may *read* this job — the same scope ``list_jobs`` uses.

    Unscoped legacy jobs follow the tenant scope too: while isolation is
    enforced only platform admins see them (D154).
    """
    return tenant_scope(request).allows(job.get("workspace_id"))


def _can_mutate_job(request: Request, job: dict) -> bool:
    """True if the actor may resume / retry / replay / export this job."""
    return tenant_scope(request).can_write(job.get("workspace_id"))


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
    scope = tenant_scope(request, workspace_id).filter
    try:
        mongo = get_mongodb_service()
        jobs = await asyncio.to_thread(mongo.list_jobs, workspace_filter=scope)
        counts = await asyncio.to_thread(mongo.count_jobs, workspace_filter=scope)
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


@router.post("/jobs/{job_id}/sync")
async def sync_transfer_job(job_id: str, request: Request):
    """Stage Sync now or a full re-sync. Confirm the returned ack to start one child job."""
    from services.sync_now import SyncNowRefused, prepare_sync

    try:
        try:
            raw = await request.json()
        except Exception:
            raw = {}
        if not isinstance(raw, dict):
            raw = {}
        mongo = get_mongodb_service()
        job = mongo.get_job(job_id)
        if not job or not _can_access_job(request, job):
            raise HTTPException(status_code=404, detail="Job not found")
        if not _can_mutate_job(request, job):
            raise HTTPException(status_code=403, detail="Workspace write access required")
        payload = job.get("transfer_request")
        if not isinstance(payload, dict) or not payload:
            raise HTTPException(
                status_code=400,
                detail="This job has no saved configuration — re-run from Transfer Studio.",
            )
        mode = str(raw.get("mode") or "sync")
        try:
            staged = prepare_sync(
                mode=mode,
                parent_request=payload,
                parent_id=job_id,
                source_connected=True,
                destination_connected=True,
                connectivity_checked=False,
                drift=None,
            )
        except SyncNowRefused as exc:
            raise HTTPException(
                status_code=409,
                detail={"error": str(exc), **exc.details},
            ) from exc
        return staged
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
        _refresh_saved_connectors(xfer_req)
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

        status = str(job.get("status") or "").strip().lower()
        if status in {"completed", "completed_with_quarantine", "success", "succeeded"}:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": (
                        f"Job is {status}. A finished run is re-run with "
                        f"POST /api/v1/connectors/jobs/{job_id}/rerun, which keeps preflight."
                    ),
                    "primary_action": "rerun",
                    "rerun_url": f"/api/v1/connectors/jobs/{job_id}/rerun",
                },
            )
        if status not in {"failed", "cancelled"}:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": (
                        f"Job is {status or 'unknown'}; only a failed or cancelled "
                        "job can be retried from zero."
                    ),
                },
            )
        rows_committed, rows_known = committed_rows_of(job)
        _has_key, _has_cursor = _job_contract_flags(xfer_req)
        retry_decision = decide_retry_from_start(
            status=status,
            sync_mode=getattr(xfer_req, "sync_mode", ""),
            rows_committed=rows_committed,
            rows_committed_known=rows_known,
            operator_requested=True,
            has_key_contract=_has_key,
            has_cursor_contract=_has_cursor,
            destination_engine=str(
                getattr(getattr(xfer_req, "destination", None), "format", "") or ""
            ),
            source_engine=str(
                getattr(getattr(xfer_req, "source", None), "format", "") or ""
            ),
        )
        if not retry_decision["allowed"] and not force:
            emit_event(
                logger, "job.retry.refused", outcome="held", level=logging.INFO,
                job_id=job_id, workspace_id=job.get("workspace_id") or None,
                reason="duplicate_risk",
            )
            from services.execution_engine_contract import rerun_refusal_detail

            detail = rerun_refusal_detail(retry_decision["reason"])
            detail["rows_committed"] = rows_committed if rows_known else None
            detail["rows_committed_known"] = rows_known
            detail["primary_action"] = "resume"
            detail["resume_url"] = f"/api/v1/connectors/jobs/{job_id}/resume"
            raise HTTPException(status_code=409, detail=detail)
        # Retries from start also re-run preflight — never inherit skip_preflight.
        xfer_req.skip_preflight = False
        # D6: the audit trail must name who started this attempt and what they
        # acknowledged — a blank triggered_by / null governance hides it.
        actor = _actor_email(request)
        xfer_req.triggered_by = actor or f"retry of {job_id}"
        forced = bool(force and not retry_decision["allowed"])
        if forced:
            xfer_req.acknowledgment_actor = actor
            xfer_req.acknowledgment_reason = (
                f"Operator accepted duplicate-row risk on retry of {job_id}."
            )
            # success=true must never ship a refused decision (D5).
            retry_decision = {
                **retry_decision,
                "kind": "forced_override",
                "allowed": True,
                "forced": True,
                "refusal_overridden": retry_decision["reason"],
            }
        engine = get_transfer_engine()
        new_job_id = engine._create_pending_job(xfer_req)
        from datetime import datetime, timezone

        confirmation = {
            "actor": actor or "operator",
            "action": "retry",
            "of": job_id,
            "forced": forced,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        mongo.update_job_status(
            new_job_id,
            "pending",
            retry_of=job_id,
            message=(
                f"Retry from start of job {job_id} (no checkpoint)"
                + (
                    " — operator accepted duplicate rows: "
                    f"{retry_decision['refusal_overridden']}"
                    if forced
                    else ""
                )
            ),
            duplicate_risk_acknowledged=forced,
            retry_decision=retry_decision,
            confirmation=confirmation,
            triggered_by=xfer_req.triggered_by,
        )

        # From-zero: do not copy parent checkpoint / resume_from_job_id.
        background_tasks.add_task(run_transfer_async, new_job_id, xfer_req, resume=False)
        emit_event(
            logger, "job.retry.requested", outcome="succeeded",
            job_id=new_job_id, retry_of=job_id,
            workspace_id=job.get("workspace_id") or None,
            duplicate_risk_acknowledged=forced,
        )
        return {
            "success": True,
            "async": True,
            "job_id": new_job_id,
            "retry_of": job_id,
            "status": "running",
            "resume": False,
            "duplicate_risk_acknowledged": forced,
            "retry_decision": retry_decision,
            "source": getattr(xfer_req.source, "table", "") or getattr(xfer_req.source, "collection", "") or "",
            "destination": getattr(xfer_req.destination, "table", "") or getattr(xfer_req.destination, "collection", "") or "",
            "rows_committed": rows_committed if rows_known else None,
            "message": (
                "Retry from start — new job, source re-read from the beginning "
                "(at-least-once)."
            ),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class RerunRequest(BaseModel):
    """Optional body of a rerun: what the operator accepts for this run."""

    force: bool = False
    #: ``{approved_by, reason, execution_policy, on_cast_failure?, columns?}`` -
    #: the same acceptance ``plan_transfer`` takes. Signs a continue-policy Risk
    #: Contract on the replayed mappings so rows that no longer cast (the source
    #: changed since the first run) are quarantined instead of refusing the run.
    risk_acceptance: dict[str, Any] | None = None


def _sign_rerun_risk_acceptance(xfer_req: Any, acceptance: dict[str, Any]) -> list[str]:
    """Sign the operator's acceptance onto the replayed request's mappings (D170).

    Returns the columns that gained a contract. Refuses (400) rather than
    silently ignoring an acceptance that signs nothing.
    """
    from ..ai.copilot.transfer_tools import _sign_required_risk_contracts

    mappings = [dict(m) for m in (xfer_req.mappings or []) if isinstance(m, dict)]
    if not mappings:
        raise HTTPException(
            status_code=400,
            detail=(
                "This job has no saved column mappings to attach a Risk Contract "
                "to — re-plan with plan_transfer (risk_acceptance) instead."
            ),
        )
    named = {
        str(c).strip() for c in (acceptance.get("columns") or []) if str(c or "").strip()
    }
    table = str(
        getattr(xfer_req.destination, "table", "")
        or getattr(xfer_req.destination, "collection", "")
        or ""
    )
    try:
        signed = _sign_required_risk_contracts(
            mappings, acceptance, table=table, force_columns=named or None
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    gained = [
        str(new.get("source") or new.get("target") or "")
        for old, new in zip(mappings, signed)
        if new.get("risk_contract") and not old.get("risk_contract")
    ]
    if not gained:
        raise HTTPException(
            status_code=400,
            detail=(
                "risk_acceptance signed nothing: no mapped column needs a Risk "
                "Contract (name the columns that fail in risk_acceptance.columns)."
            ),
        )
    xfer_req.mappings = signed
    return gained


@router.post("/jobs/{job_id}/rerun")
async def rerun_transfer_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    request: Request,
    force: bool = False,
    body: RerunRequest | None = None,
):
    """Start a finished or cancelled job again as a new job.

    The new job records ``rerun_of`` and keeps preflight (``skip_preflight``
    stays false), so Execute re-validates. Allowed for completed,
    completed-with-quarantine, and cancelled. Append that already committed
    rows is refused unless ``force=true``. A failed job uses ``/retry``. An
    optional ``risk_acceptance`` body signs a continue-policy Risk Contract so
    rows that no longer cast are quarantined instead of refusing the run (D170).
    """
    force = bool(force or (body is not None and body.force))
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
        _refresh_saved_connectors(xfer_req)
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
            decide_job_rerun,
            rerun_refusal_detail,
        )

        rows_committed, rows_known = committed_rows_of(job)
        _has_key, _has_cursor = _job_contract_flags(xfer_req)
        decision = decide_job_rerun(
            status=job.get("status"),
            sync_mode=getattr(xfer_req, "sync_mode", ""),
            rows_committed=rows_committed,
            rows_committed_known=rows_known,
            has_key_contract=_has_key,
            has_cursor_contract=_has_cursor,
        )
        if not decision["allowed"] and not force:
            detail = rerun_refusal_detail(decision["reason"])
            detail["rows_committed"] = rows_committed if rows_known else None
            detail["rows_committed_known"] = rows_known
            raise HTTPException(status_code=409, detail=detail)
        xfer_req.skip_preflight = False
        # D6: record who triggered the rerun and what they acknowledged.
        actor = _actor_email(request)
        xfer_req.triggered_by = actor or f"rerun of {job_id}"
        risk_columns: list[str] = []
        if body is not None and body.risk_acceptance:
            risk_columns = _sign_rerun_risk_acceptance(xfer_req, body.risk_acceptance)
        forced = bool(force and not decision["allowed"])
        if forced:
            xfer_req.acknowledgment_actor = actor
            xfer_req.acknowledgment_reason = (
                f"Operator accepted duplicate-row risk on rerun of {job_id}."
            )
            # success=true must never ship a refused decision (D5).
            decision = {
                **decision,
                "kind": "forced_override",
                "allowed": True,
                "forced": True,
                "refusal_overridden": decision["reason"],
            }
        engine = get_transfer_engine()
        new_job_id = engine._create_pending_job(xfer_req)
        from datetime import datetime, timezone

        confirmation = {
            "actor": actor or "operator",
            "action": "rerun",
            "of": job_id,
            "forced": forced,
            "risk_accepted_columns": risk_columns,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        mongo.update_job_status(
            new_job_id,
            "pending",
            rerun_of=job_id,
            message=(
                f"Re-run of job {job_id} (preflight kept)"
                + (
                    " — operator accepted duplicate rows: "
                    f"{decision['refusal_overridden']}"
                    if forced
                    else ""
                )
            ),
            duplicate_risk_acknowledged=forced,
            retry_decision=decision,
            confirmation=confirmation,
            triggered_by=xfer_req.triggered_by,
        )
        background_tasks.add_task(run_transfer_async, new_job_id, xfer_req, resume=False)
        emit_event(
            logger, "job.rerun.requested", outcome="succeeded",
            job_id=new_job_id, rerun_of=job_id,
            workspace_id=job.get("workspace_id") or None,
            duplicate_risk_acknowledged=forced,
        )
        return {
            "success": True,
            "async": True,
            "job_id": new_job_id,
            "rerun_of": job_id,
            "status": "running",
            "resume": False,
            "skip_preflight": False,
            "duplicate_risk_acknowledged": forced,
            "retry_decision": decision,
            "source": getattr(xfer_req.source, "table", "") or getattr(xfer_req.source, "collection", "") or "",
            "destination": getattr(xfer_req.destination, "table", "") or getattr(xfer_req.destination, "collection", "") or "",
            "rows_committed": rows_committed if rows_known else None,
            "message": (
                "Re-run — new job linked with rerun_of. Preflight runs again "
                "(at-least-once)."
            ),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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


#: Resume asks whether a CDC stream can attach before it re-queues the job; a
#: source that does not answer inside this window refuses instead of leaving the
#: job "running" at 5% with a frozen ``updated_at`` (D176).
RESUME_ATTACH_DEADLINE_SECONDS = 20.0


async def _cdc_resume_attach_refusal(xfer_req: Any) -> str:
    """Why a CDC job's change-log stream cannot attach now; empty when it can.

    Runs the same g9c log-capture check Validate runs, so Resume and Validate
    cannot disagree, bounded by :data:`RESUME_ATTACH_DEADLINE_SECONDS`.
    """
    from services.preflight_population import GateStatus, _cdc_log_capture_gates
    from services.sync_cursor import normalize_sync_mode

    from ..transfer.adapters import resolve_connector_config
    from ..transfer.cdc_transfer import cdc_source_type

    if normalize_sync_mode(getattr(xfer_req, "sync_mode", "") or "", default="") != "cdc":
        return ""
    source = xfer_req.source
    if getattr(source, "kind", "") in {"file", "cloud"}:
        return ""
    try:
        gates = await asyncio.wait_for(
            asyncio.to_thread(
                _cdc_log_capture_gates,
                list(getattr(xfer_req, "stream_contracts", None) or []),
                source_type=cdc_source_type(source),
                source_config=resolve_connector_config(source),
                source_table=getattr(source, "table", "") or getattr(source, "collection", "") or "",
                catalog_primary_key_columns=None,
                mappings=getattr(xfer_req, "mappings", None),
            ),
            timeout=RESUME_ATTACH_DEADLINE_SECONDS,
        )
    except asyncio.TimeoutError:
        return (
            f"The CDC stream did not attach within {RESUME_ATTACH_DEADLINE_SECONDS:g}s. "
            "Check that the source is reachable and the user holds REPLICATION SLAVE and "
            "REPLICATION CLIENT, then resume again."
        )
    return " ".join(
        str(g.get("message") or "") for g in gates if g.get("status") == GateStatus.BLOCK.value
    ).strip()


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
        if not _can_mutate_job(request, job):
            raise HTTPException(status_code=403, detail="Workspace write access required")

        payload = job.get("transfer_request")
        if not payload:
            raise HTTPException(
                status_code=400,
                detail="This job has no saved configuration — re-run from Transfer Studio.",
            )
        xfer_req_probe = transfer_request_from_dict(payload)
        _refresh_saved_connectors(xfer_req_probe)
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
        # A CDC job re-enters with the stored failure on it. Refuse while the
        # stream still cannot attach (the failure stays visible); otherwise a
        # resume sat "running" with the old error and no progress (D176).
        attach_refusal = await _cdc_resume_attach_refusal(xfer_req)
        if attach_refusal:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "Resume refused — the CDC stream cannot attach",
                    "reasons": [attach_refusal],
                },
            )
        mongo.clear_job_cancel(job_id)
        mongo.update_job_status(
            job_id,
            "pending",
            message=f"Resume requested for job {job_id}",
            error="",
            allow_terminal_exit=True,
        )
        background_tasks.add_task(
            run_transfer_async, job_id, xfer_req, resume=not restart_full_refresh
        )
        emit_event(
            logger, "job.resume.requested", outcome="succeeded",
            job_id=job_id, workspace_id=job.get("workspace_id") or None,
            restarted=restart_full_refresh, gap_restart=bool(safety.get("gap_restart")),
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
        from services.job_status import is_terminal

        if is_terminal(job.get("status")):
            emit_event(
                logger, "job.cancel.requested", outcome="skipped",
                job_id=job_id, workspace_id=job.get("workspace_id") or None,
                reason="already_terminal", status=str(job.get("status")),
            )
            return {"success": False, "job_id": job_id, "status": job.get("status"), "message": "Job already terminal — nothing was cancelled"}
        # Durable intent flag first, status second. The flag is what the worker
        # loop actually consults, and unlike `status` nothing in the progress
        # path ever overwrites it — so a cancel cannot be lost to a race with
        # the worker's next chunk update.
        mongo.request_job_cancel(job_id)
        queue_release: dict[str, Any] = {"queue": "not_attempted"}
        try:
            from services.worker_fleet import cancel_queued_job

            queue_release = cancel_queued_job(job_id)
        except Exception as exc:
            logger.warning("Queue cancel failed for %s: %s", job_id, exc)
            queue_release = {"queue": "unavailable"}
        recorded = mongo.update_job_status(
            job_id, "cancelled",
            phase="cancelled",
            message="Transfer cancelled by user",
            progress_pct=job.get("progress_pct", 0),
            operator_command=True,
        )
        if not recorded:
            current = mongo.get_job(job_id) or {}
            actual = str(current.get("status") or job.get("status") or "")
            if actual != "cancelled":
                emit_event(
                    logger, "job.cancel.requested", outcome="held",
                    level=logging.INFO, job_id=job_id,
                    workspace_id=job.get("workspace_id") or None,
                    reason="status_not_changed", status=actual or None,
                    queue_release=str(queue_release.get("queue") or "") or None,
                )
                return {
                    "success": False,
                    "job_id": job_id,
                    "status": actual,
                    "message": (
                        "Cancel was requested but the job status was not changed "
                        f"(it is {actual or 'unknown'}). The running worker stops "
                        "at its next checkpoint; nothing was rolled back."
                    ),
                    "cancel_requested": True,
                    "queue_release": queue_release,
                }
        # A cancelled one-shot keeps its slot so the next run resumes from the
        # log (release_finished_cdc_slot, E3-006); the response names it so the
        # operator can drop an abandoned route's slot by hand.
        slot_release: dict[str, Any] = {"released": False, "reason": "not_attempted"}
        try:
            from services.cdc_catchup import release_finished_cdc_slot

            slot_release = release_finished_cdc_slot(
                job, reason="cancelled", job_id=job_id
            )
        except Exception as exc:
            logger.warning("CDC slot release on cancel failed for %s: %s", job_id, exc)
            slot_release = {"released": False, "reason": "release_failed"}
        emit_event(
            logger, "job.cancel.requested", outcome="succeeded",
            job_id=job_id, workspace_id=job.get("workspace_id") or None,
            queue_release=str(queue_release.get("queue") or "") or None,
            cdc_slot_released=bool(slot_release.get("released")),
        )
        return {
            "success": True,
            "job_id": job_id,
            "status": "cancelled",
            "message": "Cancellation requested",
            "cdc_slot_release": slot_release,
            "queue_release": queue_release,
        }
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
            from services.job_status import is_terminal

            if is_terminal(safe.get("status")):
                break
            await asyncio.sleep(0.4)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _operator_quarantine_rows(details: list) -> list:
    """Response copy. Stored rejected_details keep the internal NULL wire."""
    from services.dest_quarantine import project_operator_quarantine_details

    return project_operator_quarantine_details(details)


def _operator_csv_cell(value: object) -> str:
    """CSV has no NULL token. SQL NULL is an empty field; ``""`` stays empty."""
    from services.dest_quarantine import operator_quarantine_json_cell

    cell = operator_quarantine_json_cell(value)
    if cell is None:
        return ""
    return str(cell)


@router.get("/jobs/{job_id}/quarantine")
async def get_job_quarantine(job_id: str, request: Request):
    """Return quarantined rows for a job with their rejection reasons.

    Includes write-time rejects and preflight integrity findings (encoding, etc.)
    so Inspect Quarantine is never empty when Validate/Run reported bad cells.
    """
    from services.quarantine_from_preflight import (
        merge_job_quarantine,
        quarantine_evidence_source,
    )

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
    finding_rows = len(row_ids) if row_ids else len(details)
    rejected_rows = int(
        job.get("rejected_details_total")
        or job.get("rejected_rows")
        or 0
    ) or finding_rows
    summary = job.get("destination_summary")
    has_write = bool(summary.get("rejected_details")) if isinstance(summary, dict) else False
    source = quarantine_evidence_source(job, details)
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
    # A refused write unit rolls back rows that carry no finding of their own.
    # Those rows are not reviewable quarantine scraps, so they are named here
    # instead of inflating a total Inspect cannot show a single row for.
    rows_rolled_back = int(ds.get("rows_rolled_back") or 0)
    rows_refused_unit = int(ds.get("rows_refused_unit") or 0)
    rows_unaccounted = max(0, rejected_rows - finding_rows - rows_rolled_back)
    # `quarantine_durable` speaks for the control plane only. A destination DLQ
    # table write can fail in the same run, so the two stores are reported
    # separately instead of one flag reading `true` beside a failed write.
    dest_dlq_durable: bool | None = None
    if dest_dlq.get("table") or dest_dlq.get("error") or dest_dlq.get("ok") is not None:
        dest_dlq_durable = bool(dest_dlq.get("ok")) and not dest_dlq.get("error")
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
        "finding_rows": finding_rows,
        "rows_rolled_back": rows_rolled_back,
        "rows_refused_unit": rows_refused_unit,
        "rows_unaccounted": rows_unaccounted,
        "open_count": open_n,
        "source": source,
        "quarantine": _operator_quarantine_rows(details),
        "dest_dlq": dest_dlq,
        "quarantine_durable": quarantine_durable,
        "dest_dlq_durable": dest_dlq_durable,
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
    from services.csv_safety import neutralize_formula_cell

    rows = [
        [
            str(d.get("row", "")),
            str(d.get("column", "")),
            str(d.get("target", "")),
            _operator_csv_cell(d.get("value", "")),
            str(d.get("reason", "")),
            str(d.get("policy", "")),
            str(d.get("suggested_transform", "")),
        ]
        for d in details
    ]
    rows = [[neutralize_formula_cell(cell) for cell in row] for row in rows]
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


_QUARANTINE_DETAIL_KEYS = frozenset({
    "column", "value", "values", "source_values", "row", "reason",
    "failure_reason", "policy", "retry_status", "source_pk", "cell",
    "transform", "gate", "target", "quarantine",
})


def _is_record_payload_row(d: Any) -> bool:
    """An edited *row payload* carries cell columns only — no finding keys.

    ``{id: 4, flt: '4.0'}`` is a replacement row; ``{row: 2, column: 'flt',
    value: 'not-a-number'}`` is a finding scrap. The endpoint used to feed
    both through the detail path and lose every payload cell (QA Q05).
    """
    return isinstance(d, dict) and bool(d) and not any(
        k in d for k in _QUARANTINE_DETAIL_KEYS
    )


def _merge_edited_row_payloads(
    open_details: list[dict],
    base_records: list[dict],
    edits: list[dict],
    *,
    job_id: str = "",
) -> tuple[list[dict], list[dict]]:
    """Bind each edited row payload to the open finding it remediates.

    The edit replaces cells in the stored row — an edit like
    ``{id: 4, flt: '4.0'}`` is not a complete record by itself, so replaying
    it raw would write NULLs for every unmapped field. Binding: a base
    record is a candidate when at least one shared cell agrees and no
    shared cell disagrees *outside* the finding's quarantined column —
    the quarantined cell is exactly what the edit is allowed to change.
    Zero or ambiguous candidates refuse: writing the wrong row is worse
    than asking the operator to be more specific.
    """
    if len(base_records) != len(open_details):
        # Details grouped by row can collapse — rebuild 1:1 so each edit
        # binds to exactly one finding.
        rebuilt: list[dict] = []
        for d in open_details:
            recs, _cols = _quarantine_details_to_records([d])
            rebuilt.append(recs[0] if recs else {})
        base_records = rebuilt
    # A cell finding quarantines one named column; a shape-divert finding
    # quarantines the whole record (``row`` is a dict), so its marker is "*" —
    # the edit may rewrite any cell and binds on the cells that still agree.
    quarantined_cols = {
        i: (
            "*"
            if isinstance(d.get("row"), dict)
            else str(d.get("column") or d.get("target") or "").strip()
        )
        for i, d in enumerate(open_details)
    }
    bound: list[tuple[int, dict]] = []
    used: set[int] = set()

    def _mask_preview(value: Any) -> bool:
        text = str(value or "")
        return "…" in text or "***" in text

    for edit in edits:
        candidates: list[int] = []
        edit_row = edit.get("row")
        if edit_row is not None:
            try:
                want_row = int(edit_row)
            except (TypeError, ValueError):
                want_row = None
            if want_row is not None:
                for i, detail in enumerate(open_details):
                    if i in used:
                        continue
                    row_val = detail.get("row")
                    try:
                        # Dict-row findings address themselves by the source
                        # row index they were diverted from.
                        have_row = int(
                            detail.get("source_row_index")
                            if isinstance(row_val, dict)
                            else (row_val or -1)
                        )
                    except (TypeError, ValueError):
                        continue
                    if have_row == want_row:
                        candidates.append(i)
        if len(candidates) == 1:
            idx = candidates[0]
            used.add(idx)
            merged = dict(base_records[idx] if idx < len(base_records) else {})
            merged.update({str(k): v for k, v in edit.items() if k != "row"})
            bound.append((idx, merged))
            continue
        candidates = []
        for i, base in enumerate(base_records):
            if i in used:
                continue
            shared = [k for k in edit if k in base]
            if not shared:
                continue
            qcol = quarantined_cols.get(i) or ""
            agreeing = [
                k for k in shared
                if str(edit[k]) == str(base[k]) and not _mask_preview(base.get(k))
            ]
            disagreeing = (
                []
                if qcol == "*"
                else [
                    k for k in shared
                    if k not in agreeing and k != qcol and not _mask_preview(base.get(k))
                ]
            )
            if agreeing and not disagreeing:
                candidates.append(i)
        if not candidates:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Edited row does not match any open quarantine finding — "
                    "keep the unchanged cells from get_job's quarantined row "
                    "so the edit binds to exactly one finding."
                ),
            )
        if len(candidates) > 1:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Edited row matches {len(candidates)} open findings — "
                    "include the finding's ``row`` index or enough unchanged "
                    "cells to make the match unique."
                ),
            )
        idx = candidates[0]
        used.add(idx)
        merged = dict(base_records[idx])
        merged.update({str(k): v for k, v in edit.items()})
        bound.append((idx, merged))
    # Replay only the bound findings — unedited open rows stay open.
    return (
        [merged for _, merged in bound],
        [open_details[i] for i, _ in bound],
    )


def _quarantine_details_to_records(details: list[dict], transform_overrides: Optional[dict] = None) -> tuple[list[dict], list[str]]:
    """Group rejected_details by row index into records for rewrite.

    Values may be source-shaped (transform quarantine) or target-shaped
    (write-matrix quarantine). Callers canonicalize to source keys before
    ``write_destination_database``.
    """
    by_row: dict[int, dict] = {}
    order: list[int] = []
    for detail in details:
        raw_row = detail.get("row")
        if isinstance(raw_row, dict):
            # Shape-divert findings carry the rejected record in ``row``
            # itself — a dict, not a source row index. Hydrate it as its own
            # record under a synthetic negative key so it never merges with a
            # real row number; cell-scrap fields below can still refine it.
            from connectors.writer_common import quarantine_cell_wire

            row_num = -(len(order) + 1)
            by_row[row_num] = {
                str(k): quarantine_cell_wire(v) for k, v in raw_row.items()
            }
            order.append(row_num)
        else:
            try:
                row_num = int(raw_row or 0)
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

    from services.quarantine_dlq import replay_parent_not_committed

    parent_refusal = replay_parent_not_committed(job)
    if parent_refusal:
        raise HTTPException(status_code=409, detail=parent_refusal)

    payload = job.get("transfer_request")
    if not payload:
        raise HTTPException(
            status_code=400,
            detail="This job has no saved configuration — cannot replay quarantine.",
        )

    stored_details = job.get("rejected_details") or job.get("destination_summary", {}).get("rejected_details") or []
    from services.quarantine_dlq import (
        compact_replay_closure,
        job_quarantine_closure,
        open_quarantine_details,
        quarantine_sample_incomplete,
        record_replay,
        overlay_cell_edit,
        replay_row_identity,
        restore_replay_source_cells,
        snapshot_quarantine_cells,
        VERDICT_CLOSED,
    )

    from services.quarantine_dlq import MaskedQuarantineReplayError, canonical_replay_details

    try:
        hydrated = canonical_replay_details(job)
    except MaskedQuarantineReplayError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    durable = hydrated or list(stored_details)
    incomplete = quarantine_sample_incomplete(job, durable)
    if incomplete:
        raise HTTPException(status_code=400, detail=incomplete)

    promoted_ids = {
        replay_row_identity(d)
        for d in durable
        if str(d.get("retry_status") or "").lower() == "promoted"
    }
    record_rows = False
    if body.rows:
        raw_rows = [d for d in body.rows if isinstance(d, dict)]
        if raw_rows and all(_is_record_payload_row(d) for d in raw_rows):
            # The operator sent edited *row payloads* — {id: 4, flt: '4.0'} —
            # not quarantine-detail scrap. Bind each edit to the open finding
            # it remediates and merge over the stored cells (QA Q05). Only
            # bound findings join the replay attempt — unedited rows stay open.
            record_rows = True
            open_details = open_quarantine_details(durable)
            base_records, _ = _quarantine_details_to_records(open_details)
            records, details = _merge_edited_row_payloads(
                open_details, base_records, raw_rows, job_id=job_id
            )
            columns = list(dict.fromkeys(k for r in records for k in r))
        else:
            details = [
                overlay_cell_edit(d, durable)
                for d in body.rows
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
    # Snapshot before the write. Transforms and the child quarantine image
    # must not replace these cells on the parent ledger (R4-D04).
    source_cell_snaps = snapshot_quarantine_cells(details)

    if not record_rows:
        records, columns = _quarantine_details_to_records(details, body.transform_overrides)
    if not records:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Cannot replay: none of the {len(details)} open finding(s) name a column "
                "or carry stored cell values, so there is no row payload to rewrite. "
                "Export the CSV, fix the mapping or the source, then re-run the transfer."
            ),
        )

    transfer_req = transfer_request_from_dict(payload)
    mappings = list(transfer_req.mappings or [])
    if body.transform_overrides:
        from services.transform_engine import transform_override_refusal

        refusal = transform_override_refusal(
            dict(body.transform_overrides),
            [str(m.get("source") or m.get("source_column") or "") for m in mappings] or columns,
        )
        if refusal:
            logger.warning("Quarantine replay of job %s refused: %s", job_id, refusal)
            raise HTTPException(status_code=400, detail=refusal)
        for m in mappings:
            src = m.get("source") or m.get("source_column") or ""
            if src in body.transform_overrides:
                m["transform"] = body.transform_overrides[src]
                m["transform_override"] = True
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

    # Replay must never carry the parent's overwrite semantics. A parent that
    # ran full_refresh_overwrite stamps overwrite-class flags on the request
    # destination extra (e.g. pg_overwrite_in_transaction); replayed through
    # the writer those flags truncate the table even though the write below is
    # a keyed upsert — deleting every previously landed row (P0-R1).
    _overwrite_extras = {
        "pg_overwrite_in_transaction",
        "overwrite_backup",
        "overwrite_backup_engine",
        "route_backups",
    }
    dest_extra = dict(getattr(dest, "extra", None) or {})
    if _overwrite_extras & dest_extra.keys():
        import copy as _copy

        dest = _copy.copy(dest)
        dest.extra = {k: v for k, v in dest_extra.items() if k not in _overwrite_extras}

    # The stored mappings carry the target types the parent job planned with.
    # The remedy for a hold-out is often to fix the destination column, so the
    # replay is judged against the destination as it is now (D185).
    if dest.kind == "database" and dest.table:
        from services.dest_physical_types import refresh_mapping_target_types

        from ..transfer.adapters import resolve_connector_config

        dest_cfg = resolve_connector_config(dest)
        mappings = refresh_mapping_target_types(
            mappings,
            resolve_driver_type(str(dest_cfg.get("type") or dest.format or "")).lower(),
            dest_cfg,
            table=dest.table,
        )

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
        if conflict_columns and dest.kind == "database" and dest.table:
            from ..transfer.ladder_batch_scope import replay_conflict_key

            conflict_columns = replay_conflict_key(
                resolve_driver_type(str(dest_cfg.get("type") or dest.format or "")).lower(),
                dest_cfg,
                schema=str(dest.schema or dest_cfg.get("schema") or ""),
                table_name=str(dest.table),
                columns=[str(m.get("target") or "") for m in mappings if m.get("target")],
                resolved=conflict_columns,
            )
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
            # Replay is a keyed upsert of the remediated rows. It is never a
            # full-refresh overwrite, and a failure must not replace an object
            # that already landed (R11-001).
            sync_mode="incremental_deduped",
            protect_landed_object=True,
        )
        rejected = int(dest_summary.get("rejected_rows") or 0)
        # Gate-8 on the child job — Airbyte-class post-write integrity (count +
        # checksum / keyed sample). Upsert into a live table may have extras;
        # sync_mode + reconcile_sample make allow_extra + sample proof honest.
        dest_summary = dict(dest_summary or {})
        dest_summary["sync_mode"] = "incremental_deduped"
        dest_summary["source_row_count"] = len(records)
        # C21: the ladder must scope its destination read-back to the
        # replayed keys, not the whole table. Without conflict_columns the
        # scope is empty and a 1-row replay is digested against every row
        # the destination holds — a guaranteed mismatch that stamped
        # "verified" nowhere but reported diverging on a correct write.
        if conflict_columns:
            dest_summary["conflict_columns"] = list(conflict_columns)
        # reconcile_sample is operator-facing: records carry the
        # ``__DF_SQL_NULL__`` wire sentinel for the writer, but the sample
        # must show real NULL — the destination holds real NULL too (R4).
        from services.value_serializer import (
            SQL_NULL_SENTINEL as _SQL_NULL,
            unescape_wire_text as _unesc,
        )

        dest_summary["reconcile_sample"] = [
            {
                k: (None if isinstance(v, str) and v == _SQL_NULL else _unesc(v))
                for k, v in rec.items()
            }
            for rec in records[:50]
        ]
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

        if int(rows_written or 0) == 0 and records:
            # A replay that lands nothing is not a success (QA R2-061).
            status = "failed"
            recon["passed"] = False
            recon["message"] = (
                "Quarantined values were not loaded — replay wrote 0 rows."
            )
        elif rejected > 0:
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
        stamped_findings = restore_replay_source_cells(
            list(recorded.get("findings") or []),
            source_cell_snaps,
            list(dest_summary.get("rejected_details") or []),
        )
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
                logger.warning(
                    "quarantine replay %s: DLQ promote stamp failed: %s", job_id, exc, exc_info=exc
                )
                promote_meta = {"error": str(exc)[:300]}
        from services.quarantine_dlq import closure_with_promotion_outcome

        compact = closure_with_promotion_outcome(compact, promote_meta, job_id=job_id)
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
                "promotion_error": compact.get("promotion_error") or "",
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
async def get_connector(connector_id: str, request: Request):
    """Get a specific connector"""
    try:
        from services.mongodb_service import redact_connector_secrets

        mongo = get_mongodb_service()
        connector = mongo.get_connector(connector_id)

        if not connector:
            raise HTTPException(status_code=404, detail="Connector not found")

        assert_resource_workspace(request, str(connector.get("workspace_id") or ""))
        return redact_connector_secrets(connector)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{connector_id}")
async def delete_connector(connector_id: str, request: Request):
    """Delete a connector"""
    try:
        mongo = get_mongodb_service()
        connector = mongo.get_connector(connector_id)
        if not connector:
            raise HTTPException(status_code=404, detail="Connector not found")
        assert_resource_workspace(request, str(connector.get("workspace_id") or ""), write=True)
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
        content = await read_upload(file)
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

        stored_file_id = ""
        try:
            from services.file_parser import store_upload

            stored = store_upload(file.filename or "upload.csv", content)
            stored_file_id = str(stored.get("file_id") or "")
        except Exception:
            stored_file_id = ""

        return {
            "success": True,
            "filename": file.filename,
            "file_id": stored_file_id or None,
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

    except (HTTPException, PayloadTooLarge):
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

        workspace_id = resolve_write_workspace(request, workspace_id)

        from connectors.sql_identifiers import unsafe_destination_identifier_reason

        ident_err = unsafe_destination_identifier_reason(dest_type, destination_collection)
        if ident_err:
            raise HTTPException(status_code=400, detail=ident_err)

        content = await read_upload(file)
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
    except (HTTPException, PayloadTooLarge):
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
