"""Server-side record of what Validate approved (SEC-APPROVAL-1).

A Decision Artifact hash is a content hash anyone can compute for any Map,
so a hash a client sends is not, by itself, an approval:

* P1: Execute of a plan that Validate ran, sent without the Validate stamps,
  stamped its own preflight and wrote columns nobody approved.
* P2: the stamps did not name the destination, so Validate of table ``s1``
  ran into table ``s2``.
* P3: a schedule save re-stamped whatever rows were saved.

Every Validate that produces stamps (``POST /transfer/plans/{id}/preflight``
and ``POST /preflight/run``) records here the hashes it returned, the
destination table, connector and selected stream names it checked, the plan
and the workspace. Execute of a validated plan must then carry one of that
plan's approvals, for the same destination; a schedule keeps a stamp only
when it is a recorded approval of exactly the rows being saved.

Stored in the shared control-plane collection when there is one (every
replica sees the same approvals), else in ``<data_dir>/validate_approvals.json``.
Bounded: the newest ``_MAX_ROWS`` approvals, none older than ``_TTL_DAYS``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import HTTPException

logger = logging.getLogger(__name__)

COLLECTION = "validate_approvals"
_FILE = "validate_approvals.json"
_MAX_ROWS = 5000
_TTL_DAYS = 90


def _now() -> datetime:
    return datetime.now(UTC)


def _path() -> Path:
    from services.platform_config import data_dir

    return data_dir() / _FILE


def _collection() -> Any | None:
    from services.control_plane_store import shared_collection

    return shared_collection(COLLECTION)


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _get(obj: Any, name: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def binding_of(destination: Any, stream_contracts: Any = None, *, table: str = "", connector_id: str = "") -> dict:
    """The destination a Validate checked: connector, table and selected streams."""
    dest_table = table or _get(destination, "table") or _get(destination, "collection") or ""
    connector = connector_id or _get(destination, "connector_id") or ""
    streams = sorted({
        _norm(c.get("name"))
        for c in (stream_contracts or [])
        if isinstance(c, dict) and c.get("name") and c.get("selected", True) is not False
    })
    return {"connector_id": _norm(connector), "table": _norm(dest_table), "streams": streams}


def _hashes(pf: dict[str, Any]) -> tuple[list[str], str]:
    proof = pf.get("proof_bundle") if isinstance(pf.get("proof_bundle"), dict) else {}
    artifact = proof.get("decision_artifact") if isinstance(proof.get("decision_artifact"), dict) else {}
    decision = sorted({h for h in (_norm(proof.get("decision_artifact_hash")), _norm(artifact.get("content_hash"))) if h})
    ddl = _norm((proof.get("ddl_identity") or {}).get("ddl_identity_hash")) if isinstance(
        proof.get("ddl_identity"), dict) else ""
    return decision, ddl


def record_validate(
    pf: dict[str, Any] | None,
    *,
    workspace_id: str,
    binding: dict[str, Any],
    plan_id: str = "",
) -> None:
    """Record the stamps one Validate returned. Never fails the Validate answer."""
    if not isinstance(pf, dict):
        return
    decision, ddl = _hashes(pf)
    if not decision:
        return
    row = {
        "_id": uuid.uuid4().hex,
        "workspace_id": str(workspace_id or ""),
        "plan_id": str(plan_id or ""),
        "decision_hashes": decision,
        "ddl_identity_hash": ddl,
        "binding": binding,
        "passed": bool(pf.get("passed")),
        "run_id": str(pf.get("run_id") or ""),
        "created_at": _now().isoformat(),
        "expires_at": _now() + timedelta(days=_TTL_DAYS),
    }
    try:
        coll = _collection()
        if coll is not None:
            coll.insert_one(row)
            return
        from services.metadata_backend import json_doc_transaction

        with json_doc_transaction(_path(), {"rows": []}) as doc:
            cutoff = (_now() - timedelta(days=_TTL_DAYS)).isoformat()
            rows = [r for r in doc.get("rows") or [] if str(r.get("created_at") or "") >= cutoff]
            rows.append({**row, "expires_at": row["expires_at"].isoformat()})
            doc["rows"] = rows[-_MAX_ROWS:]
    except Exception:  # noqa: BLE001 - Execute then refuses the unrecorded stamp
        logger.error("Could not record a Validate approval", exc_info=True)


def approvals(*, workspace_id: str, plan_id: str = "", decision_hash: str = "") -> list[dict[str, Any]]:
    """Recorded approvals of one workspace, narrowed to a plan and/or a hash."""
    query: dict[str, Any] = {"workspace_id": str(workspace_id or "")}
    if plan_id:
        query["plan_id"] = str(plan_id)
    if decision_hash:
        query["decision_hashes"] = _norm(decision_hash)
    coll = _collection()
    if coll is not None:
        return list(coll.find(query).sort("created_at", -1).limit(200))
    from services.metadata_backend import load_json_doc

    rows = load_json_doc(_path(), {"rows": []}).get("rows") or []
    out = []
    for row in reversed(rows):
        if str(row.get("workspace_id") or "") != query["workspace_id"]:
            continue
        if plan_id and str(row.get("plan_id") or "") != str(plan_id):
            continue
        if decision_hash and _norm(decision_hash) not in (row.get("decision_hashes") or []):
            continue
        out.append(row)
    return out


def _binding_matches(recorded: dict[str, Any], current: dict[str, Any]) -> bool:
    rec = recorded or {}
    if rec.get("table") and current.get("table") and rec["table"] != current["table"]:
        return False
    if rec.get("table") and not current.get("table"):
        return False
    if rec.get("connector_id") and current.get("connector_id") and rec["connector_id"] != current["connector_id"]:
        return False
    recorded_streams = list(rec.get("streams") or [])
    current_streams = list(current.get("streams") or [])
    if recorded_streams:
        return recorded_streams == current_streams
    # Validated as one table: a run may name that table as its one stream, no other.
    return not current_streams or current_streams == [rec.get("table") or current.get("table")]


def _refuse(code: str, message: str) -> HTTPException:
    return HTTPException(status_code=409, detail={"code": code, "error": message, "message": message})


def assert_execute_approved(request: Any, *, plan_id: str = "") -> None:
    """Refuse an Execute whose approval is not a recorded Validate of this run (P1, P2).

    * A plan Validate ran must be executed with one of that plan's approvals;
      without stamps the run is refused, never self-stamped.
    * A recorded approval holds only for the destination table, connector and
      selected streams Validate checked.
    A run that names no plan and carries no recorded stamp is an ordinary
    programmatic run, decided by the engine's own gates as before.
    """
    decision = _norm(_get(request, "approved_decision_artifact_hash"))
    ddl = _norm(_get(request, "approved_ddl_identity_hash"))
    current = binding_of(_get(request, "destination"), _get(request, "stream_contracts"))
    plan_id = str(plan_id or "").strip()
    plan_rows: list[dict[str, Any]] = []
    validated = False
    workspace = ""
    if plan_id:
        from services.transfer_plan_store import get_plan

        plan = get_plan(plan_id)
        if plan is not None:
            workspace = str(plan.workspace_id or "")
            plan_rows = approvals(workspace_id=workspace, plan_id=plan_id)
            validated = bool(plan_rows) or bool(plan.preflight_runs)
    if not workspace:
        from services.workspace_filter import current_workspace_stamp

        workspace = current_workspace_stamp(for_write=False) or ""
    if not decision:
        if validated:
            raise _refuse(
                "validate_approval_required",
                "This plan was validated, so Run needs that Validate's approval. Run Validate again, then Run.",
            )
        return
    candidates = [r for r in plan_rows if decision in (r.get("decision_hashes") or [])]
    if plan_rows and not candidates:
        raise _refuse(
            "validate_approval_mismatch",
            "The approval sent with this run is not from a Validate of this plan. Run Validate again, then Run.",
        )
    if not candidates:
        candidates = approvals(workspace_id=workspace, decision_hash=decision)
    if not candidates:
        return
    if ddl:
        candidates = [r for r in candidates if not r.get("ddl_identity_hash") or r["ddl_identity_hash"] == ddl]
        if not candidates:
            raise _refuse(
                "validate_approval_mismatch",
                "The Map approval sent with this run does not match what Validate approved. Run Validate again.",
            )
    if not any(_binding_matches(r.get("binding") or {}, current) for r in candidates):
        raise _refuse(
            "validate_approval_destination",
            "Validate approved a different destination table or set of streams. Run Validate for this "
            "destination, then Run.",
        )


def schedule_stamp_verified(merged: dict[str, Any], *, dest_db: str) -> bool:
    """Whether a schedule's stamp is a recorded Validate of exactly these rows (P3).

    The Map→DDL identity of the rows being saved (each selected stream's own
    rows included) must equal the identity Validate approved with that hash,
    for the schedule's destination table.
    """
    decision = _norm(merged.get("approved_decision_artifact_hash"))
    if len(decision) != 64 or not dest_db:
        return False
    try:
        rows = approvals(workspace_id=str(merged.get("workspace_id") or ""), decision_hash=decision)
    except Exception:  # noqa: BLE001 - an unreadable ledger verifies nothing
        logger.warning("Validate approvals unreadable while saving a schedule", exc_info=True)
        return False
    if not rows:
        return False
    from services.conversion_contract import approved_mapping_ddl_fingerprint, bind_stream_maps
    from services.schedule_mapping_contract import persisted_mapping_rows

    contracts = list(merged.get("stream_contracts") or [])
    identity = approved_mapping_ddl_fingerprint(
        bind_stream_maps(persisted_mapping_rows(merged.get("mappings")), contracts, dest_db=dest_db),
        dest_db=dest_db,
    )
    current = binding_of({}, contracts, table=str(merged.get("dest_table") or ""),
                         connector_id=str(merged.get("dest_connector_id") or ""))
    return any(
        r.get("ddl_identity_hash") == identity and _binding_matches(r.get("binding") or {}, current)
        for r in rows
    )


__all__ = [
    "approvals",
    "assert_execute_approved",
    "binding_of",
    "record_validate",
    "schedule_stamp_verified",
]
