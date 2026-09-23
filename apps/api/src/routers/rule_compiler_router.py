"""Import a customer rule workbook and compile it onto Transform + Map."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from services.rule_compiler import RuleIngestError, compile_rule_workbook

router = APIRouter(prefix="/transfer/rules", tags=["Transfer rules"])


def _json_object(raw: str) -> dict[str, str]:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        return {str(k): str(v) for k, v in data.items() if str(k).strip()}
    return {}


def _json_catalog(raw: str) -> dict[str, list[str]]:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
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


def _json_list(raw: str) -> list[str]:
    text = (raw or "").strip()
    if not text:
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return [part.strip() for part in text.split(",") if part.strip()]
    if isinstance(data, list):
        return [str(item) for item in data if str(item).strip()]
    return []


@router.post("/import")
async def import_rule_workbook(
    file: UploadFile = File(...),
    source_columns: str = Form(""),
    dest_columns: str = Form(""),
    source_table: str = Form(""),
    dest_table: str = Form(""),
    source_tables: str = Form(""),
    source_catalog: str = Form(""),
    source_types: str = Form(""),
    dest_types: str = Form(""),
    sync_mode: str = Form(""),
) -> dict[str, Any]:
    """Read Excel / CSV / JSON rules. Does not write a destination."""
    payload = await file.read()
    filename = file.filename or "rules"
    try:
        return compile_rule_workbook(
            filename,
            payload,
            source_columns=_json_list(source_columns),
            dest_columns=_json_list(dest_columns),
            source_table=source_table,
            dest_table=dest_table,
            source_tables=_json_list(source_tables),
            source_catalog=_json_catalog(source_catalog),
            source_types=_json_object(source_types),
            dest_types=_json_object(dest_types),
            sync_mode=sync_mode,
        )
    except RuleIngestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
