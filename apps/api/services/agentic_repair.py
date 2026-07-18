"""Agentic pipeline repair — detect → diagnose → propose → human approve → apply.

Builds on :mod:`services.validation_assistant` with a closed loop and audit trail.
Auto-apply is limited to additive/safe fixes; breaking changes require approval.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from services.validation_assistant import explain_validation

_LOCK = threading.RLock()
_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_AUDIT_PATH = os.path.join(_DATA_DIR, "agentic_repair_audit.jsonl")
_PROPOSALS_PATH = os.path.join(_DATA_DIR, "agentic_repair_proposals.json")


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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _audit(event: str, payload: dict[str, Any]) -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)
    row = {"ts": time.time(), "event": event, **payload}
    with _LOCK:
        with open(_AUDIT_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, default=str) + "\n")


def _load_proposals() -> list[dict[str, Any]]:
    if not os.path.isfile(_PROPOSALS_PATH):
        return []
    try:
        with open(_PROPOSALS_PATH, encoding="utf-8") as f:
            return list(json.load(f).get("proposals") or [])
    except Exception:
        return []


def _save_proposals(rows: list[dict[str, Any]]) -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)
    tmp = _PROPOSALS_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"proposals": rows[-200:]}, f, indent=2, default=str)
    os.replace(tmp, _PROPOSALS_PATH)


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
    safe_actions = [a for a in actions if _action_is_safe(a)]
    auto = bool(safe_actions) and len(safe_actions) == len(actions) and all(
        a.get("kind") in ("add_transform", "change_target_type") for a in safe_actions
    )
    conf = "high" if auto else ("medium" if safe_actions else "low")
    proposal = RepairProposal(
        id=f"repair_{uuid.uuid4().hex[:12]}",
        job_id=job_id,
        source="preflight",
        confidence=conf,
        auto_applicable=auto,
        summary=str(explained.get("narrative") or explained.get("summary") or "Repair proposed"),
        actions=actions,
        diagnosis={
            "issues": explained.get("issues") or [],
            "column_fixes": explained.get("column_fixes") or explained.get("fixes") or [],
            "narrative": explained.get("narrative") or "",
        },
    )
    with _LOCK:
        rows = _load_proposals()
        rows.append(proposal.to_dict())
        _save_proposals(rows)
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
        confidence="medium" if actions else "low",
        auto_applicable=False,  # quarantine always needs human approve
        summary=f"{len(actions)} repair action(s) from {len(rejected_details or [])} quarantined detail(s)",
        actions=actions,
        diagnosis={"rejected_count": len(rejected_details or [])},
    )
    with _LOCK:
        rows = _load_proposals()
        rows.append(proposal.to_dict())
        _save_proposals(rows)
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
    )


def get_proposal(proposal_id: str) -> RepairProposal | None:
    with _LOCK:
        for r in _load_proposals():
            if r.get("id") == proposal_id:
                return _proposal_from_row(r)
    return None


def list_proposals(*, job_id: str = "", status: str = "") -> list[RepairProposal]:
    with _LOCK:
        rows = _load_proposals()
    out = [_proposal_from_row(r) for r in rows]
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
    """Approve/reject a proposal; on approve optionally run ``apply_fn(actions)``."""
    with _LOCK:
        rows = _load_proposals()
        idx = next((i for i, r in enumerate(rows) if r.get("id") == proposal_id), -1)
        if idx < 0:
            raise KeyError(f"Unknown proposal: {proposal_id}")
        row = dict(rows[idx])
        if not approve:
            row["status"] = "rejected"
            row["decided_at"] = time.time()
            row["decided_by"] = actor
            rows[idx] = row
            _save_proposals(rows)
            _audit("reject", {"proposal_id": proposal_id, "actor": actor})
            return _proposal_from_row(row)

        row["status"] = "approved"
        row["decided_at"] = time.time()
        row["decided_by"] = actor
        apply_result: dict[str, Any] = {"applied": False}
        if apply_fn is not None:
            try:
                apply_result = dict(apply_fn(list(row.get("actions") or [])) or {})
                apply_result.setdefault("applied", True)
                row["status"] = "applied"
            except Exception as exc:
                row["status"] = "failed"
                apply_result = {"applied": False, "error": str(exc)}
        row["apply_result"] = apply_result
        rows[idx] = row
        _save_proposals(rows)
        _audit("approve", {"proposal_id": proposal_id, "actor": actor, "status": row["status"]})
        return _proposal_from_row(row)


def apply_actions_to_mappings(
    mappings: list[dict[str, Any]],
    actions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Deterministic apply: mutate mapping list from approved actions."""
    out = [dict(m) for m in mappings]
    by_source = {(m.get("source") or m.get("source_column") or ""): m for m in out}
    for action in actions:
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
            m["destination_type"] = action["to_type"]
            m["target_type"] = action["to_type"]
        elif kind == "add_transform" and action.get("transform"):
            transforms = list(m.get("transforms") or [])
            transforms.append({"type": action["transform"]})
            m["transforms"] = transforms
            m["transform"] = action["transform"]
        elif kind == "map_column" and action.get("target"):
            m["destination"] = action["target"]
    return out
