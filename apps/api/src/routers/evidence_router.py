"""Evidence Ledger routes: signing keys, the transparency log, receipts, verification, withdrawal.

Reads are ``audit.read``. Creating, rotating or retiring a workspace's signing
keys is ``workspace.manage`` with write access to that workspace; anchoring the
log's checkpoint is a platform action (platform administrators only);
withdrawing a pack is ``job.manage`` with write access to the pack's
workspace. An entry or key of another workspace answers 404.

``POST /evidence/public/verify`` needs no session: a caller presenting a pack
and its receipt gets the verdict and nothing else (no workspace data, no
ledger search), behind its own per-address rate limit.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Literal

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from services.audit_log import actor_from_request
from services.mcp_rate_limit import TokenBucketStore

router = APIRouter(tags=["Evidence Ledger"])

NOT_FOUND_ENTRY = "Evidence entry not found"
NOT_FOUND_KEY = "Signing key not found"
PUBLIC_MAX_BYTES = 8 * 1024 * 1024
PUBLIC_BURST = 20.0
PUBLIC_REFILL_PER_SEC = 0.2
_public_buckets = TokenBucketStore()


class WithdrawBody(BaseModel):
    model_config = {"extra": "forbid"}

    reason: str = Field(min_length=1, max_length=500)
    superseded_by: str | None = Field(default=None, pattern=r"^[0-9a-fA-F]{64}$")


def _http(exc: Exception) -> HTTPException:
    return HTTPException(status_code=int(getattr(exc, "status_code", 400)), detail=str(exc))


def _errors() -> tuple[type[Exception], ...]:
    from services.ledger_keys import LedgerKeyError
    from services.ledger_log import LedgerError

    return (LedgerError, LedgerKeyError)


async def _run(fn: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        return await asyncio.to_thread(fn, *args, **kwargs)
    except _errors() as exc:
        raise _http(exc) from exc


def _scope(request: Request, workspace_id: str = "") -> Any:
    from services.workspace_access import tenant_scope

    return tenant_scope(request, workspace_id)


def _read_workspace(request: Request, workspace_id: str) -> str:
    from services.workspace_access import resolve_read_workspace

    return resolve_read_workspace(request, workspace_id)


def _write_workspace(request: Request, workspace_id: str) -> str:
    from services.workspace_access import assert_resource_workspace, resolve_write_workspace

    target = resolve_write_workspace(request, workspace_id)
    assert_resource_workspace(request, target or None, write=True, not_found_detail="Workspace not found")
    return target


# ----------------------------------------------------------------------- keys


def _key_listing(workspace_id: str) -> dict[str, Any]:
    from services import ledger_keys

    scope = ledger_keys.scope_for(workspace_id)
    return {"workspace_id": workspace_id or None, "signature_scheme": ledger_keys.signature_scheme(scope),
            "post_quantum": ledger_keys.post_quantum_status(), "keys": ledger_keys.list_keys(scope),
            "log_keys": ledger_keys.list_keys(ledger_keys.LOG_SCOPE)}


def _key_set(workspace_id: str, keys: list[dict[str, Any]]) -> dict[str, Any]:
    from services import ledger_keys

    return {"workspace_id": workspace_id or None,
            "signature_scheme": ledger_keys.signature_scheme(ledger_keys.scope_for(workspace_id)), "keys": keys}


@router.get("/evidence/keys")
async def list_evidence_keys(request: Request, workspace_id: str = Header(default="", alias="X-Workspace-Id")):
    return await _run(_key_listing, _read_workspace(request, workspace_id))


@router.post("/evidence/keys", status_code=201)
async def create_evidence_keys(request: Request, workspace_id: str = Header(default="", alias="X-Workspace-Id")):
    """Create the workspace's first signing key set (409 when it already has one)."""
    from services import ledger_keys

    target = _write_workspace(request, workspace_id)
    keys = await _run(ledger_keys.create, ledger_keys.scope_for(target), actor=actor_from_request(request))
    return _key_set(target, keys)


@router.post("/evidence/keys/rotate")
async def rotate_evidence_keys(request: Request, workspace_id: str = Header(default="", alias="X-Workspace-Id")):
    """A new key set signs from now on; the previous one is retired and still verifies."""
    from services import ledger_keys

    target = _write_workspace(request, workspace_id)
    keys = await _run(ledger_keys.rotate, ledger_keys.scope_for(target), actor=actor_from_request(request))
    return _key_set(target, keys)


@router.post("/evidence/keys/{key_id}/retire")
async def retire_evidence_key(key_id: str, request: Request,
                              workspace_id: str = Header(default="", alias="X-Workspace-Id")):
    from services import ledger_keys

    target = _write_workspace(request, workspace_id)
    key = await _run(ledger_keys.retire, ledger_keys.scope_for(target), key_id, actor=actor_from_request(request))
    return {"key": key}


@router.get("/evidence/keys/jwks")
async def evidence_jwks(request: Request, workspace_id: str = Header(default="", alias="X-Workspace-Id")):
    """The JWK Set the offline verifier trusts: this workspace's keys and the log's, active and retired."""
    from services import ledger_keys

    workspace = _read_workspace(request, workspace_id)
    return await _run(ledger_keys.jwks, [ledger_keys.scope_for(workspace), ledger_keys.LOG_SCOPE])


@router.get("/evidence/keys/{key_id}")
async def get_evidence_key(key_id: str, request: Request, format: Literal["jwk", "pem"] = Query(default="jwk")):
    from services import ledger_keys

    found = await _run(ledger_keys.find_key, key_id)
    if not found:
        raise HTTPException(status_code=404, detail=NOT_FOUND_KEY)
    entry, scope = found
    if scope != ledger_keys.LOG_SCOPE and not _scope(request).allows(scope[3:] or None):
        raise HTTPException(status_code=404, detail=NOT_FOUND_KEY)
    if format == "pem":
        return PlainTextResponse(await _run(ledger_keys.pem, entry))
    return ledger_keys.jwk(entry)


# ------------------------------------------------------------------------ log


def _checkpoint_view(record: dict[str, Any] | None) -> dict[str, Any]:
    record = record or {}
    return {"checkpoint": record.get("checkpoint"), "checkpoint_sha256": record.get("checkpoint_sha256"),
            "anchor": record.get("anchor")}


@router.get("/evidence/checkpoint")
async def get_evidence_checkpoint(tree_size: int | None = Query(default=None, ge=1)):
    """The latest signed checkpoint (or the one of ``tree_size``) and its anchor receipt."""
    from services import ledger_log

    if tree_size is None:
        return _checkpoint_view(await _run(ledger_log.checkpoint))
    record = await _run(ledger_log.checkpoint, tree_size, create=False)
    if not record:
        raise HTTPException(status_code=404, detail="No checkpoint of that size")
    return _checkpoint_view(record)


@router.post("/evidence/checkpoint/anchor")
async def anchor_evidence_checkpoint(request: Request):
    """Anchor the log's latest checkpoint now. Platform administrators only: the log is the deployment's."""
    from services.ledger_anchor import anchor_latest
    from services.workspace_access import require_platform_admin

    require_platform_admin(request, _scope(request), "evidence.checkpoint.anchor")
    return await _run(anchor_latest)


@router.get("/evidence/entries")
async def list_evidence_entries(
    request: Request, workspace_id: str = Header(default="", alias="X-Workspace-Id"),
    limit: int = Query(default=50, ge=1, le=200), before: int | None = Query(default=None, ge=0),
    kind: Literal["", "proof_pack", "parity_report", "exit_evidence", "withdrawal", "key_set"] = Query(default=""),
    subject: str = Query(default="", max_length=300),
):
    from services import ledger_log

    scope = _scope(request, workspace_id)
    return await _run(ledger_log.list_entries, allows=scope.allows, limit=limit, before=before, kind=kind,
                      subject=subject)


def _visible_entry(index: int, request: Request, *, write: bool = False) -> dict[str, Any]:
    from services import ledger_log
    from services.workspace_access import assert_resource_workspace

    entry = ledger_log.get_entry(index)
    if not entry:
        raise HTTPException(status_code=404, detail=NOT_FOUND_ENTRY)
    if not write and ledger_log.visible(entry, lambda _workspace: False):
        return entry  # one of the log's own key events: the deployment's public keys
    assert_resource_workspace(request, entry.get("workspace_id") or None, write=write,
                              not_found_detail=NOT_FOUND_ENTRY)
    return entry


@router.get("/evidence/entries/{index}")
async def get_evidence_entry(index: int, request: Request):
    from services import ledger_log

    entry = await asyncio.to_thread(_visible_entry, index, request)
    return await _run(ledger_log.public_entry, entry)


@router.get("/evidence/entries/{index}/proof")
async def get_inclusion_proof(index: int, tree_size: int | None = Query(default=None, ge=1)):
    """The audit path of a leaf (hashes only, so it is not workspace data)."""
    from services import ledger_log

    return await _run(ledger_log.inclusion_proof, index, tree_size)


@router.get("/evidence/consistency")
async def get_consistency_proof(first: int = Query(alias="from", ge=1), second: int = Query(alias="to", ge=1)):
    from services import ledger_log

    return await _run(ledger_log.consistency_proof, first, second)


@router.get("/evidence/receipts/{content_sha256}")
async def get_evidence_receipt(
    content_sha256: str, request: Request,
    kind: Literal["", "proof_pack", "parity_report", "exit_evidence", "withdrawal", "key_set"] = Query(default=""),
):
    """The receipt of a pack by its content digest, against the latest checkpoint."""
    from services import ledger_log

    def build() -> dict[str, Any]:
        entry = ledger_log.find_entry(content_sha256.lower(), kind)
        if not entry or not _scope(request).allows(entry.get("workspace_id") or None):
            raise HTTPException(status_code=404, detail=NOT_FOUND_ENTRY)
        return ledger_log.receipt(entry)

    return await _run(build)


@router.get("/evidence/log/audit")
async def audit_evidence_log():
    """Re-derive the log from the store and compare it with every stored checkpoint (hashes only)."""
    from services import ledger_log

    return await _run(ledger_log.audit_log)


# --------------------------------------------------------------------- verify


def _pack_and_receipt(body: Any) -> tuple[dict[str, Any], dict[str, Any] | None]:
    from services.proof_pack_request import pack_from_body

    receipt = body.get("receipt") if isinstance(body, dict) and "content_sha256" not in body else None
    if receipt is not None and not isinstance(receipt, dict):
        raise HTTPException(status_code=422, detail={"error": "receipt must be an object.", "missing": []})
    return pack_from_body(body), receipt


@router.post("/evidence/verify")
async def verify_evidence(request: Request, body: Any = Body(...)):
    """Verify a pack online: its HMAC as before, and every Evidence Ledger check.

    Accepts ``{"pack": ..., "receipt": ...}`` or the pack itself. A pack with no
    receipt is looked up by its digest within the caller's workspaces.
    """
    from services.ledger_verify import verify_online
    from services.signed_proof_pack import LEDGER_KEY, verify_signed_proof_pack

    pack, receipt = _pack_and_receipt(body)
    scope = _scope(request)

    def run() -> dict[str, Any]:
        hmac = verify_signed_proof_pack({k: v for k, v in pack.items() if k != LEDGER_KEY})
        evidence = verify_online(pack, receipt, search=True, allows=scope.allows)
        return {"kind": hmac["kind"], "content_sha256": hmac["content_sha256"],
                "hmac": {"ok": hmac["ok"], "errors": hmac["errors"]}, "evidence": evidence}

    return await _run(run)


@router.post("/evidence/public/verify")
async def verify_evidence_public(request: Request):
    """The verdict for a caller presenting a pack and its receipt. No session; verdicts only."""
    from services.client_ip import client_ip_from_request
    from services.ledger_verify import verify_online

    outcome = _public_buckets.consume(f"evidence-verify:{client_ip_from_request(request) or 'unknown'}",
                                      capacity=PUBLIC_BURST, refill_per_sec=PUBLIC_REFILL_PER_SEC)
    if not outcome.get("allowed"):
        retry = max(1, int(float(outcome.get("retry_after_sec") or 1)))
        return JSONResponse(status_code=429, headers={"Retry-After": str(retry)},
                            content={"error": "rate_limited", "detail": "Too many verification requests."})
    declared = request.headers.get("content-length") or "0"
    if declared.isdigit() and int(declared) > PUBLIC_MAX_BYTES:
        raise HTTPException(status_code=413, detail="The pack is too large to verify here (8 MB at most).")
    raw = await request.body()
    if len(raw) > PUBLIC_MAX_BYTES:
        raise HTTPException(status_code=413, detail="The pack is too large to verify here (8 MB at most).")
    try:
        body = json.loads(raw)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail={"error": "Body must be JSON.", "missing": []}) from exc
    pack, receipt = _pack_and_receipt(body)
    return {"evidence": await _run(verify_online, pack, receipt, public=True)}


# ------------------------------------------------------------------- withdraw


@router.post("/evidence/entries/{index}/withdraw", status_code=201)
async def withdraw_evidence(index: int, request: Request, body: WithdrawBody):
    """Withdraw (or supersede) a pack: a new entry is appended, the original stays."""
    from services.ledger_packs import withdraw

    entry = await asyncio.to_thread(_visible_entry, index, request, write=True)
    return await _run(withdraw, entry, reason=body.reason, superseded_by=body.superseded_by,
                      actor=actor_from_request(request))
