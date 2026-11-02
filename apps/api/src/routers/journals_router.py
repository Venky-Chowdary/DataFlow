"""Capture-once journals: list, inspect, delete, prune.

Replay is an ordinary transfer: POST /api/v1/transfer/execute with
``source = {"kind": "file", "extra": {"journal_id": "<id>"}}`` — once per
destination. The engine verifies the journal and runs the full
map → preflight → quarantine → Gate-8 pipeline on it.
"""

from __future__ import annotations

from typing import Any

from pathlib import Path

from fastapi import APIRouter, HTTPException

from services.transfer_journal import (
    JournalError,
    delete_journal,
    find_journal,
    list_journals,
    prune_journals,
    resolve_replay,
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
        _dir, manifest = find_journal(journal_id, hydrate=False)
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


@router.post("/{journal_id}/open")
def open_journal(journal_id: str, stream: str = "") -> dict[str, Any]:
    """Verify a journal stream and profile it as the Studio's Source step.

    The response is the ``/connectors/upload`` shape plus ``journal``; the
    transfer then sends ``source.extra.journal_id`` so Execute re-verifies and
    replays the journal itself, never this profile's copy.
    """
    from .connectors_router import profile_file_upload

    try:
        resolved = resolve_replay(journal_id, stream)
    except JournalError as exc:
        status = 404 if "not found" in str(exc) else 409
        raise HTTPException(status_code=status, detail=str(exc)) from exc
    entry = resolved["stream"]
    manifest = resolved["manifest"]
    profile = profile_file_upload(Path(resolved["path"]).read_bytes(), resolved["filename"])
    captured_order = [str(c) for c in entry.get("columns") or []]
    if captured_order and set(captured_order) == set(profile.get("columns") or []):
        # JSON objects carry no column order; the manifest kept the source's.
        profile["columns"] = captured_order
    profile["journal"] = {
        "journal_id": manifest.get("journal_id"),
        "stream": entry.get("name"),
        "rows": entry.get("rows"),
        "sha256": entry.get("sha256"),
        "encodings": entry.get("encodings") or {},
        "source": manifest.get("source") or {},
        "captured_at": manifest.get("captured_at"),
        "expires_at": manifest.get("expires_at"),
        "storage": manifest.get("storage") or {},
    }
    return profile
