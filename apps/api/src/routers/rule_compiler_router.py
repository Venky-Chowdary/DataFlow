"""Import a customer rule workbook and compile it onto Transform + Map."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile

from services.rule_compiler import RuleIngestError, compile_rule_workbook
from services.rule_compiler.ingest import MAX_FILE_BYTES

router = APIRouter(prefix="/transfer/rules", tags=["Transfer rules"])


def _form_json(raw: str, field: str) -> Any:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail=f"Form field {field!r} is not valid JSON.",
        ) from None


def _json_object(raw: str, field: str = "source_types") -> dict[str, str]:
    data = _form_json(raw, field)
    if data is None:
        return {}
    if isinstance(data, dict):
        return {str(k): str(v) for k, v in data.items() if str(k).strip()}
    raise HTTPException(
        status_code=400,
        detail=f"Form field {field!r} must be a JSON object.",
    )


def _json_catalog(raw: str, field: str = "source_catalog") -> dict[str, list[str]]:
    data = _form_json(raw, field)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise HTTPException(
            status_code=400,
            detail=f"Form field {field!r} must be a JSON object.",
        )
    out: dict[str, list[str]] = {}
    for key, value in data.items():
        name = str(key).strip()
        if not name:
            continue
        if isinstance(value, list):
            out[name] = [str(item).strip() for item in value if str(item).strip()]
        elif isinstance(value, str) and value.strip():
            out[name] = [part.strip() for part in value.split(",") if part.strip()]
    return out


def _json_list(raw: str, field: str = "source_columns") -> list[str]:
    data = _form_json(raw, field)
    if data is None:
        return []
    if isinstance(data, list):
        return [str(item) for item in data if str(item).strip()]
    raise HTTPException(
        status_code=400,
        detail=f"Form field {field!r} must be a JSON array.",
    )


@router.post("/import")
async def import_rule_workbook(
    request: Request,
    file: UploadFile = File(...),
    source_columns: str = Form(""),
    dest_columns: str = Form(""),
    source_table: str = Form(""),
    dest_table: str = Form(""),
    source_tables: str = Form(""),
    source_catalog: str = Form(""),
    dest_tables: str = Form(""),
    dest_catalog: str = Form(""),
    source_types: str = Form(""),
    dest_types: str = Form(""),
    sync_mode: str = Form(""),
) -> dict[str, Any]:
    """Read Excel / CSV / JSON rules. Does not write a destination."""
    payload = await file.read(MAX_FILE_BYTES + 1)
    if len(payload) > MAX_FILE_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Rule files are limited to {MAX_FILE_BYTES // (1024 * 1024)} MB.",
        )
    filename = file.filename or "rules"
    try:
        report = compile_rule_workbook(
            filename,
            payload,
            source_columns=_json_list(source_columns, "source_columns"),
            dest_columns=_json_list(dest_columns, "dest_columns"),
            source_table=source_table,
            dest_table=dest_table,
            source_tables=_json_list(source_tables, "source_tables"),
            source_catalog=_json_catalog(source_catalog, "source_catalog"),
            dest_tables=_json_list(dest_tables, "dest_tables"),
            dest_catalog=_json_catalog(dest_catalog, "dest_catalog"),
            source_types=_json_object(source_types, "source_types"),
            dest_types=_json_object(dest_types, "dest_types"),
            sync_mode=sync_mode,
        )
    except RuleIngestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    from services.workspace_access import authorized_workspace_id
    from services.rule_compiler.audit import record_rule_report_compiled

    workspace_id = authorized_workspace_id(request)
    report["audit_event_hash"] = record_rule_report_compiled(
        report=report,
        filename=filename,
        actor=getattr(request.state, "user_email", None) or "anonymous",
        workspace_id=workspace_id,
    )
    return report
