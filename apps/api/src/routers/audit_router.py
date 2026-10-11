"""Audit log API — real workspace events + tip anchors + scoped export."""

from __future__ import annotations

import csv
import io
import json
import os
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

router = APIRouter(prefix="/audit", tags=["Audit"])


def audit_export_honesty() -> dict[str, object]:
    """Claims the audit download is allowed to make — not a signed letter.

    Pilot reads this so "do you have SOC 2" / "can you sign a HIPAA BAA"
    cannot drift into an attestation the export route does not issue.
    """
    return {
        "official": False,
        "kind": "workspace_audit_sample",
        "signed_soc2": False,
        "signed_hipaa_baa": False,
        "signed_gdpr_dpa": False,
        "note": (
            "Workspace-scoped audit export. HMAC-SHA256 chain is diligence, "
            "NOT a SOC 2 Type II letter, GDPR DPA, or HIPAA BAA attestation."
        ),
    }


def _scope(request: Request) -> tuple[str, str]:
    from services.audit_log import workspace_id_from_request
    from services.tenant_store import get_tenant_for_workspace

    workspace_id = workspace_id_from_request(request)
    tenant_id = ""
    if workspace_id:
        tenant = get_tenant_for_workspace(workspace_id)
        tenant_id = str(tenant.id) if tenant else ""
    return workspace_id, tenant_id


def _next_after_seq(
    events: list[dict[str, object]], after_seq: int | None
) -> tuple[int | None, dict[str, str]]:
    if after_seq is None:
        return None, {}
    next_seq = int(events[-1].get("chain_seq") or after_seq) if events else after_seq
    return next_seq, {"X-Next-After-Seq": str(next_seq)}


def _cef_header_escape(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("|", "\\|")


def _cef_extension_escape(value: object) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("=", "\\=")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\n", "\\n")
    )


def _cef_time_ms(value: object) -> int:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return 0


def _cef_line(event: dict[str, object]) -> str:
    """CEF intentionally omits details; the NDJSON export includes them."""
    action = str(event.get("action") or "")
    severity = {"info": 3, "success": 1, "warn": 6, "error": 8}.get(
        str(event.get("level") or "info"), 3
    )
    header_action = _cef_header_escape(action)
    extension = [
        f"rt={_cef_time_ms(event.get('time'))}",
        f"suser={_cef_extension_escape(event.get('actor') or '')}",
        f"act={_cef_extension_escape(action)}",
        f"request={_cef_extension_escape(event.get('resource') or '')}",
        "cs1Label=workspace_id",
        f"cs1={_cef_extension_escape(event.get('workspace_id') or '')}",
        "cs2Label=event_hash",
        f"cs2={_cef_extension_escape(event.get('event_hash') or '')}",
        "cs3Label=prev_hash",
        f"cs3={_cef_extension_escape(event.get('prev_hash') or '')}",
        "cn1Label=chain_seq",
        f"cn1={event.get('chain_seq') or 0}",
        f"externalId={_cef_extension_escape(event.get('id') or '')}",
    ]
    device_version = os.getenv("DATAFLOW_VERSION", "1")
    return (
        f"CEF:0|Datawrap|Datawrap|{_cef_header_escape(device_version)}|"
        f"{header_action}|{header_action}|{severity}|{' '.join(extension)}"
    )


@router.get("/events")
async def list_events(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    level: str | None = Query(None, description="info | success | warn | error | all"),
    after_seq: int | None = Query(None, ge=0),
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
        after_seq=after_seq,
    )
    next_seq, cursor_headers = _next_after_seq(events, after_seq)
    return JSONResponse(
        {
            "events": events,
            "count": len(events),
            "workspace_id": workspace_id or None,
            "tenant_id": tenant_id or None,
            "next_after_seq": next_seq,
        },
        headers=cursor_headers,
    )


@router.get("/export")
async def export_events(
    request: Request,
    format: str = Query("csv", description="csv | json | ndjson | cef"),
    limit: int = Query(5000, ge=1, le=20000),
    level: str | None = Query(None),
    since: str | None = Query(None, description="ISO-8601 inclusive lower bound"),
    until: str | None = Query(None, description="ISO-8601 inclusive upper bound"),
    after_seq: int | None = Query(None, ge=0),
):
    """Workspace-scoped audit download for an auditor sample.

    Requires ``X-Workspace-Id``. Only events stamped with that workspace
    are included. This is evidence, not a SOC 2 / HIPAA letter. CEF omits
    event details intentionally; use NDJSON when those details are needed.
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
        after_seq=after_seq,
    )
    next_seq, cursor_headers = _next_after_seq(events, after_seq)
    tip = latest_event_hash()
    honesty = audit_export_honesty()
    attestation = {
        "official": honesty["official"],
        "kind": honesty["kind"],
        "note": honesty["note"],
    }
    fmt = (format or "csv").strip().lower()
    if fmt not in ("csv", "json", "ndjson", "cef"):
        raise HTTPException(status_code=400, detail="format must be csv, json, ndjson, or cef")
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
                "next_after_seq": next_seq,
            },
            headers={
                "Content-Disposition": (
                    f'attachment; filename="datawrap-audit-{workspace_id}.json"'
                ),
                **cursor_headers,
            },
        )
    if fmt == "ndjson":
        body = "".join(
            json.dumps(event, ensure_ascii=False, default=str) + "\n"
            for event in events
        )
        return Response(
            body,
            media_type="application/x-ndjson",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="datawrap-audit-{workspace_id}.ndjson"'
                ),
                **cursor_headers,
            },
        )
    if fmt == "cef":
        body = "".join(_cef_line(event) + "\n" for event in events)
        return PlainTextResponse(
            body,
            media_type="text/plain",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="datawrap-audit-{workspace_id}.cef"'
                ),
                **cursor_headers,
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
            **cursor_headers,
        },
    )


@router.get("/tip")
async def get_audit_tip():
    """Return the HMAC chain tip plus the latest external anchor receipt."""
    from services.audit_anchor import latest_anchor, list_anchors
    from services.audit_coverage import audit_access_failure_count
    from services.audit_log import latest_event_hash

    tip = latest_event_hash()
    anchor = latest_anchor()
    return {
        "tip_hash": tip,
        "anchor": anchor,
        "anchors_recent": list_anchors(limit=5),
        "matched": bool(tip and anchor and anchor.get("tip_hash") == tip),
        "audit_access_failures": audit_access_failure_count(),
        "honesty": (
            "Tip is HMAC-SHA256 chain head. Anchor is a diligence seal "
            "(stub by default — not auditor WORM / TSA until provider configured)."
        ),
    }


@router.get("/verify")
async def verify_audit_chain(
    request: Request,
    limit: int = Query(5000, ge=1, le=20000, description="Records to walk, oldest first"),
):
    """Re-walk the evidence chain. The walk is platform-wide (HMAC links every
    record). Findings named in the response are this workspace's when
    ``X-Workspace-Id`` is set — another tenant's event ids are withheld.

    Isolation on and no header is a 400, same as audit export: an unscoped
    verify would list every workspace's broken event ids.
    """
    from services.evidence_chain import verify_chain
    from services.team_store import require_workspace_isolation

    workspace_id, _tenant_id = _scope(request)
    if require_workspace_isolation() and not workspace_id:
        raise HTTPException(
            status_code=400,
            detail="X-Workspace-Id is required for chain verification. "
            "The walk is platform-wide; findings named here are this workspace.",
        )
    return verify_chain(limit=limit, workspace_id=workspace_id)


@router.post("/verify-pack")
async def verify_proof_pack_against_chain(pack: dict):
    """Verify an exported proof pack, and whether the chain still holds its record.

    Three separable questions, answered separately: is the pack's own signature
    intact, does it still hold the content its chain record was filed for, and
    is that record still in the store.
    """
    from services.evidence_chain import find_anchor
    from services.signed_proof_pack import (
        pack_body_digest_excluding_anchor,
        verify_signed_proof_pack,
    )

    if not isinstance(pack, dict) or not pack:
        raise HTTPException(status_code=400, detail="Body must be an exported proof pack object")
    result = verify_signed_proof_pack(pack)
    anchor = pack.get("chain_anchor") if isinstance(pack.get("chain_anchor"), dict) else {}
    digest = pack_body_digest_excluding_anchor(pack)
    chain_record = find_anchor(digest) if anchor.get("anchored") else None
    return {
        **result,
        "evidence_sha256": digest,
        "chain_anchor": anchor or None,
        "chain_record_found": bool(chain_record),
        "chain_record": (
            {
                "id": chain_record.get("id"),
                "time": chain_record.get("time"),
                "actor": chain_record.get("actor"),
                "event_hash": chain_record.get("event_hash"),
                "prev_hash": chain_record.get("prev_hash"),
            }
            if chain_record
            else None
        ),
        "honesty": (
            "Signature intact means the pack was not edited since it was sealed. "
            "A missing chain record means the store no longer holds the record filed "
            "for this pack — which retention alone can cause, so read /audit/verify "
            "for the retention checkpoints before calling it tampering."
            if anchor.get("anchored")
            else "This pack was exported without a chain anchor, so there is no "
            "chain record to compare it against."
        ),
    }


@router.post("/retention/purge")
async def purge_audit_retention(
    request: Request,
    dry_run: bool = Query(False),
):
    from services.audit_log import (
        AuditConfigError,
        append_audit_event,
        purge_expired_audit_events,
    )
    from services.audit_log import actor_from_request, workspace_id_from_request

    try:
        result = purge_expired_audit_events(dry_run=dry_run)
    except AuditConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    append_audit_event(
        action="audit.retention.purge",
        resource=request.url.path,
        actor=actor_from_request(request),
        level="warn" if result["removed"] else "info",
        correlation_id=request.headers.get("X-Correlation-ID"),
        workspace_id=workspace_id_from_request(request),
        details=result,
    )
    return result


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
