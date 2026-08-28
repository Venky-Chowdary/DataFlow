"""Audit log API — real workspace events + tip anchors + scoped export."""

from __future__ import annotations

import csv
import io
import json

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

router = APIRouter(prefix="/audit", tags=["Audit"])


def _scope(request: Request) -> tuple[str, str]:
    from services.audit_log import workspace_id_from_request
    from services.tenant_store import get_tenant_for_workspace

    workspace_id = workspace_id_from_request(request)
    tenant_id = ""
    if workspace_id:
        tenant = get_tenant_for_workspace(workspace_id)
        tenant_id = str(tenant.id) if tenant else ""
    return workspace_id, tenant_id


@router.get("/events")
async def list_events(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    level: str | None = Query(None, description="info | success | warn | error | all"),
):
    from services.audit_log import list_audit_events

    workspace_id, tenant_id = _scope(request)
    # Scope by the workspace the caller is looking at. Do not AND tenant_id:
    # events stamped with this workspace before a tenant existed would vanish,
    # and a shared tenant must not pull sibling-workspace rows.
    events = list_audit_events(
        limit=limit,
        level=level,
        workspace_id=workspace_id or None,
    )
    return {
        "events": events,
        "count": len(events),
        "workspace_id": workspace_id or None,
        "tenant_id": tenant_id or None,
    }


@router.get("/export")
async def export_events(
    request: Request,
    format: str = Query("csv", description="csv | json"),
    limit: int = Query(5000, ge=1, le=20000),
    level: str | None = Query(None),
    since: str | None = Query(None, description="ISO-8601 inclusive lower bound"),
    until: str | None = Query(None, description="ISO-8601 inclusive upper bound"),
):
    """Workspace-scoped audit download for an auditor sample.

    Requires ``X-Workspace-Id``. Only events stamped with that workspace
    are included. This is evidence, not a SOC 2 / HIPAA letter.
    """
    from services.audit_log import latest_event_hash, list_audit_events

    workspace_id, tenant_id = _scope(request)
    if not workspace_id:
        raise HTTPException(
            status_code=400,
            detail="X-Workspace-Id is required for audit export. "
            "This download is a workspace sample, not a global dump.",
        )
    events = list_audit_events(
        limit=limit,
        level=level,
        workspace_id=workspace_id,
        since=since,
        until=until,
    )
    tip = latest_event_hash()
    honesty = (
        "Workspace-scoped audit export. HMAC-SHA256 chain is diligence, "
        "NOT a SOC 2 Type II letter, GDPR DPA, or HIPAA BAA attestation."
    )
    attestation = {
        "official": False,
        "kind": "workspace_audit_sample",
        "note": honesty,
    }
    fmt = (format or "csv").strip().lower()
    if fmt not in ("csv", "json"):
        raise HTTPException(status_code=400, detail="format must be csv or json")
    if fmt == "json":
        return JSONResponse(
            {
                "events": events,
                "count": len(events),
                "workspace_id": workspace_id,
                "tenant_id": tenant_id or None,
                "tip_hash": tip,
                "hash_alg": "HMAC-SHA256",
                "honesty": honesty,
                "attestation": attestation,
            },
            headers={
                "Content-Disposition": (
                    f'attachment; filename="datawrap-audit-{workspace_id}.json"'
                ),
            },
        )

    columns = [
        "id", "time", "actor", "action", "resource", "level",
        "workspace_id", "tenant_id", "event_hash", "prev_hash", "hash_alg",
        "details",
    ]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    for ev in events:
        row = [ev.get(col, "") for col in columns[:-1]]
        details = ev.get("details") or {}
        row.append(json.dumps(details, ensure_ascii=False, default=str) if details else "")
        writer.writerow(row)
    body = (
        f"# {honesty}\n"
        f"# workspace_id={workspace_id} tenant_id={tenant_id or ''} "
        f"count={len(events)} tip_hash={tip or ''}\n"
    ) + buf.getvalue()
    filename = f"datawrap-audit-{workspace_id}.csv"
    return PlainTextResponse(
        body,
        media_type="text/csv",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{filename}"; workspace-id={workspace_id}'
            ),
        },
    )


@router.get("/tip")
async def get_audit_tip():
    """Return the HMAC chain tip plus the latest external anchor receipt."""
    from services.audit_anchor import latest_anchor, list_anchors
    from services.audit_log import latest_event_hash

    tip = latest_event_hash()
    anchor = latest_anchor()
    return {
        "tip_hash": tip,
        "anchor": anchor,
        "anchors_recent": list_anchors(limit=5),
        "matched": bool(tip and anchor and anchor.get("tip_hash") == tip),
        "honesty": (
            "Tip is HMAC-SHA256 chain head. Anchor is a diligence seal "
            "(stub by default — not auditor WORM / TSA until provider configured)."
        ),
    }


@router.post("/tip/anchor")
async def force_anchor_tip():
    """Manually seal the current tip (ops / compliance export)."""
    from services.audit_anchor import anchor_tip, latest_anchor
    from services.audit_log import latest_event_hash

    tip = latest_event_hash()
    if not tip:
        return {"ok": False, "error": "No audit events yet", "anchor": None}
    receipt = anchor_tip(tip)
    return {"ok": bool(receipt), "tip_hash": tip, "anchor": receipt or latest_anchor()}
