"""Capture-once journals: list, inspect, delete, prune.

Replay is an ordinary transfer: POST /api/v1/transfer/execute with
``source = {"kind": "file", "extra": {"journal_id": "<id>"}}`` — once per
destination. The engine verifies the journal and runs the full
map → preflight → quarantine → Gate-8 pipeline on it.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from services.transfer_journal import (
    JournalError,
    delete_journal,
    find_journal,
    list_journals,
    prune_journals,
)

router = APIRouter(prefix="/journals", tags=["journals"])


def _replay_hint(manifest: dict[str, Any]) -> dict[str, Any]:
    streams = manifest.get("streams") or []
    extra: dict[str, Any] = {"journal_id": manifest.get("journal_id")}
    if len(streams) > 1:
        extra["journal_stream"] = "<one of: " + ", ".join(str(s.get("name")) for s in streams) + ">"
    return {"endpoint": "POST /api/v1/transfer/execute", "source": {"kind": "file", "extra": extra}}


@router.get("")
async def get_journals() -> dict[str, Any]:
    items = list_journals()
    return {"journals": items, "total": len(items)}


@router.get("/{journal_id}")
async def get_journal(journal_id: str) -> dict[str, Any]:
    try:
        _dir, manifest = find_journal(journal_id)
    except JournalError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"journal": manifest, "replay": _replay_hint(manifest)}


@router.delete("/{journal_id}")
async def remove_journal(journal_id: str) -> dict[str, Any]:
    try:
        delete_journal(journal_id)
    except JournalError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"deleted": journal_id}


@router.post("/prune")
async def prune() -> dict[str, Any]:
    return prune_journals()
