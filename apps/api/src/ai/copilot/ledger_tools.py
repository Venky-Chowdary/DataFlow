"""Pilot / MCP tools for the Evidence Ledger: list the entries, verify a pack.

Read only, and scoped to the caller's workspace like the REST reads they twin
(``GET /evidence/entries``, ``POST /evidence/verify``). The verdict is the one
the service and the offline verifier compute (:mod:`services.ledger_verify`);
the assistant words it and never changes it. Signing keys, withdrawal and
anchoring stay REST actions a person takes on the Evidence page.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .tools import ToolResult

KINDS = ("proof_pack", "parity_report", "exit_evidence", "withdrawal", "key_set")

LEDGER_TOOL_DEFINITIONS: list[dict] = [
    {
        "name": "list_evidence_entries",
        "description": (
            "Entries of the Evidence Ledger in this workspace, newest first: each signed pack (proof pack, "
            "Parity report, exit evidence) with its log index, digest, signature scheme and whether it was "
            "later withdrawn, and each signing-key event (key_set). Use for 'what evidence have we issued', "
            "'was this pack withdrawn', 'when were the signing keys rotated', 'show the ledger'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "kind": {"type": "string", "enum": list(KINDS), "description": "Only entries of this kind."},
                "limit": {"type": "integer", "description": "How many entries (1-50, default 20)."},
            },
            "required": [],
        },
    },
    {
        "name": "verify_evidence",
        "description": (
            "Verify a signed pack against the Evidence Ledger and say which checks passed, failed or were "
            "not checked: content digest, statement, public-key signatures, inclusion in the log, the "
            "checkpoint signature, consistency with the log, withdrawal, anchor. Give a job id (its signed "
            "Parity report), a rehearsal id (its Exit Evidence Pack) or a pack's content_sha256."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string", "description": "Job id: verifies its signed Parity report pack."},
                "rehearsal_id": {"type": "string", "description": "Rehearsal id (exr_...): its Exit Evidence Pack."},
                "content_sha256": {"type": "string", "description": "A pack's content digest (64 hex)."},
            },
            "required": [],
        },
    },
]

NOTE = ("The verdict comes from deterministic checks. A pack with a receipt is independently verifiable with "
        "the published key. The ledger shows that a record was issued, has not been altered and is in the "
        "log; it does not show that the recorded facts are true. A verdict is 'verified' when every check "
        "passed, and 'incomplete' when one was not made. Keys, withdrawal and anchoring are done on the "
        "Evidence page.")
LINK = "/#/evidence"


def _result(name: str, *, output: Any = None, error: str = "") -> "ToolResult":
    from .tools import ToolResult

    return ToolResult(name=name, success=not error, output=None if error else output, error=error or None)


def list_evidence_entries(kind: str = "", limit: int = 20) -> "ToolResult":
    from services import ledger_log
    from services.workspace_filter import bound_allows

    name = "list_evidence_entries"
    kind = str(kind or "").strip().lower()
    if kind and kind not in KINDS:
        return _result(name, error=f"Kind must be one of: {', '.join(KINDS)}.")
    try:
        count = max(1, min(int(limit or 20), 50))
    except (TypeError, ValueError):
        count = 20
    page = ledger_log.list_entries(allows=bound_allows, limit=count, kind=kind)
    rows = [{k: e.get(k) for k in ("index", "kind", "subject", "content_sha256", "issued_at", "issued_by",
                                    "signature_scheme", "status")}
            | ({"withdrawn_at": e["withdrawal"]["withdrawn_at"], "withdrawal_reason": e["withdrawal"]["reason"]}
               if e.get("withdrawal") else {})
            for e in page["entries"]]
    return _result(name, output={"entries": rows, "more": page["next_before"] is not None, "open": LINK,
                                 "note": NOTE})


def _stored_pack(job_id: str, rehearsal_id: str) -> tuple[dict[str, Any] | None, str]:
    """(the stored signed pack, error) for a job's Parity report or a rehearsal's exit evidence."""
    from services.workspace_filter import bound_allows

    if job_id:
        from .lifecycle_tools import resolve_job

        job, error = resolve_job(job_id, "")
        if job is None:
            return None, error
        from services.parity_signoff import signed_pack

        pack = signed_pack(job)
        return pack, "" if pack else ("That job's Parity report is not signed yet, so there is no pack to "
                                      "verify. A proof pack is verified by its content_sha256.")
    from services import exit_store
    from services.exit_evidence import signed_pack as exit_pack

    doc = exit_store.get(exit_store.REHEARSALS, rehearsal_id)
    if not doc or not bound_allows(doc.get("workspace_id")):
        return None, "That rehearsal was not found in this workspace."
    pack = exit_pack(doc)
    return pack, "" if pack else "That rehearsal is not signed yet, so there is no pack to verify."


def verify_evidence(content_sha256: str = "", job_id: str = "", rehearsal_id: str = "") -> "ToolResult":
    from services import ledger_log
    from services.ledger_verify import verify_online
    from services.workspace_filter import bound_allows

    name = "verify_evidence"
    digest, job_id, rehearsal_id = (str(v or "").strip() for v in (content_sha256, job_id, rehearsal_id))
    if job_id or rehearsal_id:
        pack, error = _stored_pack(job_id, rehearsal_id)
        if pack is None:
            return _result(name, error=error)
        evidence = verify_online(pack, search=True, allows=bound_allows)
        subject = {"kind": pack.get("kind") or "proof_pack", "content_sha256": pack.get("content_sha256")}
    elif digest:
        entry = ledger_log.find_entry(digest.lower())
        if not entry or not bound_allows(entry.get("workspace_id") or None):
            return _result(name, error="No pack with that digest is in this workspace's ledger entries.")
        # Only the ledger's record is at hand: the pack's own content is "not checked".
        evidence = verify_online(None, ledger_log.receipt(entry))
        subject = {"kind": entry.get("kind"), "content_sha256": entry.get("content_sha256")}
    else:
        return _result(name, error="Which pack? Give a job id, a rehearsal id (exr_...) or a content_sha256.")
    return _result(name, output={
        **subject, "verdict": evidence["verdict"], "summary": evidence["summary"],
        "signature_scheme": evidence["signature_scheme"], "issuance": evidence["issuance"],
        "checks": evidence["checks"], "counts": evidence["counts"], "leaf_index": evidence["leaf_index"],
        "withdrawal": evidence["withdrawal"], "open": LINK, "note": NOTE,
    })
