"""Agentic pipeline repair — detect → diagnose → propose → human approve → apply.

Builds on :mod:`services.validation_assistant` with a closed loop and audit trail.
Auto-apply is limited to additive/safe fixes; breaking changes require approval.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from services.validation_assistant import explain_validation
from services.workspace_filter import (
    bound_allows,
    current_workspace_filter,
    current_workspace_stamp,
    norm_workspace_id,
)

_LOCK = threading.RLock()


def _data_dir() -> str:
    # Resolve per call — an import-time snapshot would pin the dev data dir
    # before test isolation or a reconfigured volume can take effect.
    from services.platform_config import data_dir

    return str(data_dir())


def _audit_path() -> str:
    return os.path.join(_data_dir(), "agentic_repair_audit.jsonl")


def _proposals_path() -> str:
    return os.path.join(_data_dir(), "agentic_repair_proposals.json")
_MONGO_COLL = "repair_proposals"
_MONGO_AUDIT = "repair_audit"


def _proposals_coll():
    from services.control_plane_store import mongo_collection

    return mongo_collection(_MONGO_COLL)


def _audit_coll():
    from services.control_plane_store import mongo_collection

    return mongo_collection(_MONGO_AUDIT)


@dataclass
class RepairProposal:
    id: str
    job_id: str = ""
    source: str = "preflight"  # preflight | quarantine | contract
    status: str = "proposed"  # proposed | approved | rejected | applied | failed
    confidence: str = "medium"  # high | medium | low
    auto_applicable: bool = False
    summary: str = ""
    actions: list[dict[str, Any]] = field(default_factory=list)
    diagnosis: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    decided_at: float = 0.0
    decided_by: str = ""
    apply_result: dict[str, Any] = field(default_factory=dict)
    # The workspace of the proposal's job (see :func:`_workspace_for_new`).
    # ``""`` is unscoped: only a caller whose tenant scope includes unassigned
    # rows (a platform administrator, or a deployment without isolation).
    workspace_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── tenancy (audit H-3) ──────────────────────────────────────────────────────
#
# A proposal belongs to the workspace of its job. Reads and decisions apply the
# caller's bound tenant scope, exactly as the job store's readers do, so another
# workspace's proposal does not exist to this caller. Nothing bound (workers, a
# background re-sync settling a Parity Studio fix) is system scope.


def _jobs_by_id(job_ids: list[str]) -> dict[str, dict[str, Any]] | None:
    """The stored jobs, read in system scope; ``None`` when the store cannot answer."""
    try:
        from services.mongodb_service import get_mongodb_service

        return get_mongodb_service().get_jobs([j for j in job_ids if j])
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        return None


def _workspace_for_new(job_id: str) -> str:
    """The workspace a new proposal belongs to.

    Its job's, when the caller can see that job. A job id the caller cannot see
    is treated exactly like one that does not exist: the proposal lands in the
    caller's own workspace, never in the job owner's review queue.
    """
    job = (_jobs_by_id([job_id]) or {}).get(job_id) if job_id else None
    if job is not None:
        owner = norm_workspace_id(job.get("workspace_id"))
        if bound_allows(owner):
            return owner
    return current_workspace_stamp()


def _adopt_legacy_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Give rows stored before the field existed their job's workspace.

    Returns the rows it stamped (the caller persists them, so each is resolved
    once). A row whose job cannot be found stays without the field and reads as
    unscoped: visible to a platform administrator only while isolation is on.
    """
    legacy = [r for r in rows if "workspace_id" not in r and r.get("job_id")]
    if not legacy:
        return []
    jobs = _jobs_by_id([str(r["job_id"]) for r in legacy]) or {}
    adopted = []
    for row in legacy:
        job = jobs.get(str(row["job_id"]))
        if job is not None:
            row["workspace_id"] = norm_workspace_id(job.get("workspace_id"))
            adopted.append(row)
    return adopted


def _in_scope(row: dict[str, Any]) -> bool:
    return bound_allows(row.get("workspace_id"))


def _audit(event: str, payload: dict[str, Any]) -> None:
    row = {"ts": time.time(), "event": event, **payload}
    coll = _audit_coll()
    if coll is not None:
        try:
            coll.insert_one(dict(row))
            return
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    os.makedirs(_data_dir(), exist_ok=True)
    with _LOCK:
        with open(_audit_path(), "a", encoding="utf-8") as f:
            f.write(json.dumps(row, default=str) + "\n")


def _row_from_doc(doc: dict[str, Any]) -> dict[str, Any]:
    row = dict(doc)
    row.pop("_id", None)
    if not row.get("id") and doc.get("_id") is not None:
        row["id"] = str(doc["_id"])
    return row


def _adopt_legacy_docs(coll: Any) -> None:
    """Mongo twin of the file path's legacy adoption (so scoped queries match)."""
    docs = list(coll.find({"workspace_id": {"$exists": False}, "job_id": {"$nin": ["", None]}}).limit(500))
    for row in _adopt_legacy_rows(docs):
        coll.update_one({"_id": row["_id"]}, {"$set": {"workspace_id": row["workspace_id"]}})


def _load_proposals() -> list[dict[str, Any]]:
    """Every stored proposal (system scope); readers filter with :func:`_in_scope`."""
    coll = _proposals_coll()
    if coll is not None:
        try:
            _adopt_legacy_docs(coll)
            docs = list(coll.find().sort("created_at", -1).limit(200))
            return [_row_from_doc(d) for d in docs]
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    if not os.path.isfile(_proposals_path()):
        return []
    try:
        with open(_proposals_path(), encoding="utf-8") as f:
            rows = list(json.load(f).get("proposals") or [])
    except Exception:
        return []
    if _adopt_legacy_rows(rows):
        _write_proposals_file(rows)
    return rows


def _write_proposals_file(rows: list[dict[str, Any]]) -> None:
    os.makedirs(_data_dir(), exist_ok=True)
    tmp = _proposals_path() + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"proposals": rows[-200:]}, f, indent=2, default=str)
    os.replace(tmp, _proposals_path())


def _save_proposals(rows: list[dict[str, Any]]) -> None:
    """Persist full proposal list (file fallback) or upsert each row (Mongo)."""
    coll = _proposals_coll()
    if coll is not None:
        for row in rows[-200:]:
            pid = str(row.get("id") or "")
            if not pid:
                continue
            doc = dict(row)
            doc["_id"] = pid
            coll.replace_one({"_id": pid}, doc, upsert=True)
        return
    _write_proposals_file(rows)


def _upsert_proposal(proposal: RepairProposal) -> None:
    row = proposal.to_dict()
    coll = _proposals_coll()
    if coll is not None:
        doc = dict(row)
        doc["_id"] = proposal.id
        coll.replace_one({"_id": proposal.id}, doc, upsert=True)
        return
    with _LOCK:
        rows = _load_proposals()
        rows = [r for r in rows if r.get("id") != proposal.id]
        rows.append(row)
        _save_proposals(rows)


def _action_is_safe(action: dict[str, Any]) -> bool:
    kind = str(action.get("kind") or "")
    # Additive / non-destructive only
    return kind in {
        "change_target_type",  # widen
        "add_transform",
        "set_validation_mode_balanced",
        "map_column",
    }


def propose_from_preflight(
    preflight_result: dict[str, Any],
    *,
    job_id: str = "",
    coercion_report: dict[str, Any] | None = None,
) -> RepairProposal:
    """Diagnose a preflight failure and emit a human-reviewable repair proposal."""
    # Attach coercion report into the preflight payload when provided separately.
    payload = dict(preflight_result or {})
    if coercion_report and "coercion_report" not in payload:
        payload["coercion_report"] = coercion_report
    explained = explain_validation(payload, use_llm=False)
    actions = list(explained.get("suggested_actions") or [])
    mutative = [a for a in actions if action_is_mapping_mutative(a)]
    safe_actions = [a for a in actions if _action_is_safe(a)]
    auto = bool(safe_actions) and len(safe_actions) == len(actions) and all(
        a.get("kind") in ("add_transform", "change_target_type") for a in safe_actions
    )
    conf = "high" if auto else ("medium" if mutative else "low")
    issues = list(explained.get("issues") or [])
    has_dup = any(
        a.get("kind") == "fix_source_keys" for a in actions
    )
    proposal = RepairProposal(
        id=f"repair_{uuid.uuid4().hex[:12]}",
        job_id=job_id,
        source="preflight",
        workspace_id=_workspace_for_new(job_id),
        confidence=conf,
        auto_applicable=auto,
        summary=str(explained.get("narrative") or explained.get("summary") or "Repair proposed"),
        actions=actions,
        diagnosis={
            "issues": issues,
            "column_fixes": explained.get("column_fixes") or explained.get("fixes") or [],
            "narrative": explained.get("narrative") or "",
            "mapping_applyable": bool(mutative),
            "root_cause": "duplicate_identity_keys" if has_dup else None,
        },
    )
    with _LOCK:
        _upsert_proposal(proposal)
    _audit("propose", {"proposal_id": proposal.id, "job_id": job_id, "confidence": conf})
    return proposal


def propose_from_quarantine(
    rejected_details: list[dict[str, Any]],
    *,
    job_id: str = "",
) -> RepairProposal:
    """Propose transforms/type widens from quarantine rejected_details."""
    actions: list[dict[str, Any]] = []
    seen: set[tuple] = set()
    for d in rejected_details or []:
        col = str(d.get("column") or d.get("target") or "")
        reason = str(d.get("reason") or "").lower()
        if not col:
            continue
        if "decimal" in reason or "number" in reason or "int" in reason:
            key = ("change_target_type", col, "VARCHAR")
            if key not in seen:
                seen.add(key)
                actions.append({
                    "kind": "change_target_type",
                    "column": col,
                    "to_type": "VARCHAR",
                    "label": f"Widen '{col}' to VARCHAR to accept non-numeric values",
                })
        elif "date" in reason or "timestamp" in reason:
            key = ("add_transform", col, "to_timestamp")
            if key not in seen:
                seen.add(key)
                actions.append({
                    "kind": "add_transform",
                    "column": col,
                    "transform": "to_timestamp",
                    "label": f"Apply timestamp transform to '{col}'",
                })
        else:
            key = ("add_transform", col, "to_text")
            if key not in seen:
                seen.add(key)
                actions.append({
                    "kind": "add_transform",
                    "column": col,
                    "transform": "to_text",
                    "label": f"Cast '{col}' to text",
                })
    proposal = RepairProposal(
        id=f"repair_{uuid.uuid4().hex[:12]}",
        job_id=job_id,
        source="quarantine",
        workspace_id=_workspace_for_new(job_id),
        confidence="medium" if actions else "low",
        auto_applicable=False,  # quarantine always needs human approve
        summary=f"{len(actions)} repair action(s) from {len(rejected_details or [])} quarantined detail(s)",
        actions=actions,
        diagnosis={"rejected_count": len(rejected_details or [])},
    )
    with _LOCK:
        _upsert_proposal(proposal)
    _audit("propose_quarantine", {"proposal_id": proposal.id, "job_id": job_id})
    return proposal


def _proposal_from_row(r: dict[str, Any]) -> RepairProposal:
    return RepairProposal(
        id=str(r.get("id") or ""),
        job_id=str(r.get("job_id") or ""),
        source=str(r.get("source") or "preflight"),
        status=str(r.get("status") or "proposed"),
        confidence=str(r.get("confidence") or "medium"),
        auto_applicable=bool(r.get("auto_applicable")),
        summary=str(r.get("summary") or ""),
        actions=list(r.get("actions") or []),
        diagnosis=dict(r.get("diagnosis") or {}),
        created_at=float(r.get("created_at") or 0),
        decided_at=float(r.get("decided_at") or 0),
        decided_by=str(r.get("decided_by") or ""),
        apply_result=dict(r.get("apply_result") or {}),
        workspace_id=norm_workspace_id(r.get("workspace_id")),
    )


def _stored_row(proposal_id: str) -> dict[str, Any] | None:
    """The stored row in system scope (legacy rows adopted); callers scope it."""
    coll = _proposals_coll()
    if coll is not None:
        try:
            doc = coll.find_one({"_id": proposal_id}) or coll.find_one({"id": proposal_id})
            if doc:
                for adopted in _adopt_legacy_rows([doc]):
                    coll.update_one(
                        {"_id": adopted["_id"]}, {"$set": {"workspace_id": adopted["workspace_id"]}}
                    )
                row = _row_from_doc(doc)
                row.setdefault("id", proposal_id)
                return row
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    with _LOCK:
        for r in _load_proposals():
            if r.get("id") == proposal_id:
                return r
    return None


def get_proposal(proposal_id: str) -> RepairProposal | None:
    """A proposal of the caller's tenant scope; another workspace's does not exist."""
    row = _stored_row(proposal_id)
    if row is None or not _in_scope(row):
        return None
    return _proposal_from_row(row)


def list_proposals(*, job_id: str = "", status: str = "") -> list[RepairProposal]:
    """The caller's tenant scope's proposals, newest first."""
    coll = _proposals_coll()
    if coll is not None:
        try:
            _adopt_legacy_docs(coll)
            scope = current_workspace_filter()
            # The workspace predicate is part of the query, so the 200-row page
            # is this caller's newest 200, not the deployment's.
            query: dict[str, Any] = dict(scope.mongo_query()) if scope is not None else {}
            if job_id:
                query["job_id"] = job_id
            if status:
                query["status"] = status
            docs = list(coll.find(query).sort("created_at", -1).limit(200))
            return [_proposal_from_row(row) for row in map(_row_from_doc, docs) if _in_scope(row)]
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    with _LOCK:
        rows = _load_proposals()
    out = [_proposal_from_row(r) for r in rows if _in_scope(r)]
    if job_id:
        out = [p for p in out if p.job_id == job_id]
    if status:
        out = [p for p in out if p.status == status]
    return sorted(out, key=lambda p: p.created_at, reverse=True)


def decide_proposal(
    proposal_id: str,
    *,
    approve: bool,
    actor: str = "user",
    apply_fn: Any | None = None,
) -> RepairProposal:
    """Approve/reject a proposal; on approve optionally run ``apply_fn(actions)``.

    Transitions:
    - proposed → rejected
    - proposed → approved (audit-only, no apply_fn)
    - proposed|approved → applied (when apply_fn succeeds)
    Already terminal statuses (applied/rejected/failed) raise ValueError.
    """
    with _LOCK:
        # Prefer Mongo document when available so multi-replica reads are consistent.
        existing = get_proposal(proposal_id)
        if existing is None:
            raise KeyError(f"Unknown proposal: {proposal_id}")
        row = existing.to_dict()
        current = str(row.get("status") or "proposed")

        if not approve:
            if current not in ("proposed", "approved"):
                raise ValueError(f"Cannot reject proposal in status {current}")
            row["status"] = "rejected"
            row["decided_at"] = time.time()
            row["decided_by"] = actor
            decided = _proposal_from_row(row)
            _upsert_proposal(decided)
            _audit("reject", {"proposal_id": proposal_id, "actor": actor})
            return decided

        if current not in ("proposed", "approved"):
            raise ValueError(f"Cannot approve proposal in status {current}")

        row["decided_at"] = time.time()
        row["decided_by"] = actor
        apply_result: dict[str, Any] = dict(row.get("apply_result") or {}) or {"applied": False}
        if apply_fn is not None:
            try:
                apply_result = dict(apply_fn(list(row.get("actions") or [])) or {})
                # Honesty: only mark applied when mutative work actually happened.
                really_applied = bool(apply_result.get("applied"))
                if really_applied:
                    row["status"] = "applied"
                else:
                    row["status"] = "approved"
                    apply_result.setdefault(
                        "reason",
                        apply_result.get("reason") or "no_mutative_actions",
                    )
            except Exception as exc:
                row["status"] = "failed"
                apply_result = {"applied": False, "error": str(exc)}
        else:
            row["status"] = "approved"
            apply_result = {"applied": False}
        row["apply_result"] = apply_result
        decided = _proposal_from_row(row)
        _upsert_proposal(decided)
        _audit("approve", {"proposal_id": proposal_id, "actor": actor, "status": row["status"]})
        return decided


# ── Parity Studio fixes: the same proposal / decision record ─────────────────

#: Proposals Parity Studio registers for the fixes a person decides on a job's
#: Trust Report. They are decided there — where the fix is applied through the
#: transfer path and re-checked — never through ``/repair/proposals/{id}/decide``.
PARITY_SOURCE = "parity"


def parity_proposal_id(job_id: str, fix_id: str) -> str:
    import hashlib

    return "repair_parity_" + hashlib.sha256(f"{job_id}|{fix_id}".encode("utf-8")).hexdigest()[:16]


def propose_from_parity(*, job_id: str, fix: dict[str, Any]) -> RepairProposal:
    """Register a Parity Studio fix as a repair proposal (idempotent per job and fix)."""
    pid = parity_proposal_id(job_id, str(fix.get("id") or ""))
    stored = _stored_row(pid)
    if stored is not None:
        if not _in_scope(stored):
            # The id is derived from the job: registering it again from outside
            # the job's workspace must not replace the owner's record.
            raise KeyError(f"Unknown job: {job_id}")
        return _proposal_from_row(stored)
    proposal = RepairProposal(
        id=pid,
        job_id=job_id,
        source=PARITY_SOURCE,
        workspace_id=_workspace_for_new(job_id),
        confidence="high",  # chosen by deterministic rules from the parity evidence
        auto_applicable=False,  # Parity Studio applies nothing without a person's approval
        summary=str(fix.get("title") or "Parity Studio fix"),
        actions=[{
            "kind": str(fix.get("action") or ""),
            "fix_id": str(fix.get("id") or ""),
            "label": str(fix.get("title") or ""),
            "difference_ids": list(fix.get("difference_ids") or []),
            "preview": fix.get("preview"),
        }],
        diagnosis={"detail": str(fix.get("detail") or "")},
    )
    with _LOCK:
        _upsert_proposal(proposal)
    _audit("propose_parity", {"proposal_id": pid, "job_id": job_id, "fix_id": fix.get("id")})
    return proposal


def record_outcome(proposal_id: str, *, applied: bool, result: str, actor: str = "") -> RepairProposal | None:
    """Close an approved proposal whose apply finished later (a background re-sync)."""
    with _LOCK:
        existing = get_proposal(proposal_id)
        if existing is None or existing.status not in ("approved",):
            return existing
        row = existing.to_dict()
        row["status"] = "applied" if applied else "failed"
        row["apply_result"] = {**dict(row.get("apply_result") or {}), "applied": applied, "result": result[:500]}
        decided = _proposal_from_row(row)
        _upsert_proposal(decided)
    _audit("outcome", {"proposal_id": proposal_id, "actor": actor, "status": row["status"]})
    return decided


MUTATIVE_ACTION_KINDS = frozenset({"change_target_type", "add_transform", "map_column"})


def action_is_mapping_mutative(action: dict[str, Any]) -> bool:
    """True when Approve & apply can change Studio mappings for this action."""
    kind = str(action.get("kind") or "")
    if kind not in MUTATIVE_ACTION_KINDS:
        return False
    if action.get("mapping_applyable") is False:
        return False
    # Live destination DDL — Map destType cannot ALTER Snowflake/MySQL/PG.
    # Stamping a wider type would re-Validate against the same live carrier.
    if action.get("requires_ddl") and kind == "change_target_type":
        return False
    if kind == "change_target_type" and action.get("apply_proven") is False:
        return False
    return bool(action.get("column") or action.get("source"))


def apply_actions_to_mappings(
    mappings: list[dict[str, Any]],
    actions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Deterministic apply: mutate mapping list from approved actions."""
    out = [dict(m) for m in mappings]
    by_source = {(m.get("source") or m.get("source_column") or ""): m for m in out}
    for action in actions:
        if not action_is_mapping_mutative(action):
            continue
        kind = action.get("kind")
        col = action.get("column") or action.get("source")
        if not col:
            continue
        m = by_source.get(col)
        if m is None:
            m = {"source": col, "destination": action.get("target") or col}
            out.append(m)
            by_source[col] = m
        if kind == "change_target_type" and action.get("to_type"):
            widened = action["to_type"]
            m["destination_type"] = widened
            m["target_type"] = widened
            m["destType"] = widened
            m["dest_type"] = widened
        elif kind == "add_transform" and action.get("transform"):
            xf = action["transform"]
            transforms = list(m.get("transforms") or [])
            if not any((t.get("type") if isinstance(t, dict) else t) == xf for t in transforms):
                transforms.append({"type": xf})
            m["transforms"] = transforms
            m["transform"] = xf
        elif kind == "map_column" and action.get("target"):
            m["destination"] = action["target"]
    return out


def apply_actions_with_report(
    mappings: list[dict[str, Any]],
    actions: list[dict[str, Any]],
) -> dict[str, Any]:
    """Apply mutative actions only; report honesty when nothing can change mappings."""
    mutative = [a for a in actions if action_is_mapping_mutative(a)]
    skipped = [
        {
            "kind": a.get("kind"),
            "label": a.get("label"),
            "reason": "not_mapping_mutative",
        }
        for a in actions
        if not action_is_mapping_mutative(a)
    ]
    if not mutative:
        return {
            "applied": False,
            "mappings": list(mappings),
            "mutated_count": 0,
            "skipped_actions": skipped,
            "reason": "no_mutative_actions",
            "message": (
                "No mapping mutations in this proposal (e.g. duplicate identity keys). "
                "Approve does not change Studio mappings — fix source keys / identity on Map."
            ),
        }
    updated = apply_actions_to_mappings(mappings, mutative)
    return {
        "applied": True,
        "mappings": updated,
        "mutated_count": len(mutative),
        "skipped_actions": skipped,
    }
