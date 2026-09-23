"""Import a customer rule workbook and compile it onto Transform + Map."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from services.rule_compiler import RuleIngestError, compile_rule_workbook

router = APIRouter(prefix="/transfer/rules", tags=["Transfer rules"])


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
        )
    except RuleIngestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
