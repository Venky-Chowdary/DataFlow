"""Sync now and full re-sync.

One signed contract, a fast preflight (drift and connectivity only), a new
job id linked to its parent, and one pre-filled confirm. A second click
returns the same unconsumed approval so it cannot start a second run.
"""

from __future__ import annotations

import copy
import threading
from typing import Any

FAST_SCOPE = "drift_connectivity"

_open: dict[str, str] = {}
_lock = threading.Lock()


class SyncNowRefused(Exception):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.details = details or {}


def clear_sync_now_for_tests() -> None:
    with _lock:
        _open.clear()


def full_resync_plan(request: dict[str, Any] | None) -> dict[str, Any]:
    """How a full re-sync re-reads without a silent duplicate append.

    A signed key upserts from the start (at-least-once). Overwrite, mirror,
    and SCD2 keep their own full read. With no key, replace is the only
    re-sync that does not append a second copy — the confirm names that.
    """
    request = request or {}
    mode = str(request.get("sync_mode") or "").strip().lower()
    key = str(request.get("primary_key") or "").strip()
    if not key:
        for contract in request.get("stream_contracts") or []:
            if isinstance(contract, dict) and str(contract.get("primary_key") or "").strip():
                key = str(contract.get("primary_key")).strip()
                break
    if mode in {"full_refresh_overwrite", "mirror", "scd2"}:
        return {
            "sync_mode": mode,
            "destructive": mode == "full_refresh_overwrite",
            "clears_cursor": True,
            "note": "Full re-sync uses the signed sync mode and re-reads from the start.",
        }
    if key:
        # D208: a full-source upsert. incremental_deduped read the route's
        # saved watermark, so "re-read from the start" read only new rows.
        return {
            "sync_mode": "upsert",
            "destructive": False,
            "clears_cursor": True,
            "note": (
                "Re-read from the start and upsert on the signed key. "
                "Delivery stays at-least-once."
            ),
        }
    return {
        "sync_mode": "full_refresh_overwrite",
        "destructive": True,
        "clears_cursor": True,
        "note": "No key on the signed contract — full re-sync replaces the destination.",
    }


def _clear_cursors(request: dict[str, Any]) -> None:
    for key in ("cursor_value", "watermark"):
        request.pop(key, None)
    source = request.get("source")
    if isinstance(source, dict):
        extra = dict(source.get("extra") or {})
        extra.pop("cursor_value", None)
        extra.pop("watermark", None)
        source["extra"] = extra


def child_request_dict(
    parent: dict[str, Any],
    *,
    mode: str,
    parent_job_id: str = "",
    schedule_id: str = "",
) -> dict[str, Any]:
    """Copy the signed request. Full re-sync adjusts only the read window."""
    request = copy.deepcopy(parent or {})
    request["parent_job_id"] = parent_job_id
    request["preflight_scope"] = FAST_SCOPE
    request["skip_preflight"] = False
    if schedule_id:
        request["schedule_id"] = schedule_id
    if mode == "full":
        plan = full_resync_plan(request)
        request["sync_mode"] = plan["sync_mode"]
        request["historical_resync"] = True
        request["full_resync"] = plan
        _clear_cursors(request)
    else:
        request["historical_resync"] = False
    return request


def apply_resync_to_request(request: Any, mode: str) -> None:
    """Mutate a TransferRequest the schedule builder already signed."""
    if mode == "full":
        plan = full_resync_plan({
            "sync_mode": getattr(request, "sync_mode", ""),
            "primary_key": getattr(request, "primary_key", ""),
            "stream_contracts": list(getattr(request, "stream_contracts", None) or []),
        })
        request.sync_mode = plan["sync_mode"]
        request.historical_resync = True
        source = getattr(request, "source", None)
        extra = dict(getattr(source, "extra", None) or {})
        extra.pop("cursor_value", None)
        extra.pop("watermark", None)
        if source is not None:
            source.extra = extra
    if mode in {"sync", "full"}:
        request.preflight_scope = FAST_SCOPE


def evaluate_fast_preflight(
    *,
    source_connected: bool,
    destination_connected: bool,
    drift: dict[str, Any] | None = None,
    source_error: str = "",
    destination_error: str = "",
    connectivity_checked: bool = True,
) -> dict[str, Any]:
    """Drift and connectivity only. Non-breaking drift is an audit pass."""
    drift = drift or {}
    evolution = drift.get("schema_evolution") or {}
    gates: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []

    def _gate(gate_id: str, status: str, message: str, **details: Any) -> None:
        gates.append({
            "id": gate_id,
            "cause_id": gate_id,
            "status": status,
            "message": message,
            "details": details,
        })

    if not connectivity_checked:
        _gate(
            "g1_source",
            "skip",
            "Source connectivity is checked when the run starts.",
        )
        _gate(
            "g2_destination",
            "skip",
            "Destination connectivity is checked when the run starts.",
        )
    else:
        if source_connected:
            _gate("g1_source", "pass", "Source connectivity checked.")
        else:
            message = source_error or "Source is not reachable."
            _gate("g1_source", "block", message)
            blockers.append({"id": "g1_source", "message": message})
        if destination_connected:
            _gate("g2_destination", "pass", "Destination connectivity checked.")
        else:
            message = destination_error or "Destination is not reachable."
            _gate("g2_destination", "block", message)
            blockers.append({"id": "g2_destination", "message": message})

    hard = list(evolution.get("hard_breaking") or [])
    action = str(evolution.get("action") or "continue")
    audit = list(drift.get("non_breaking_audit") or evolution.get("audit") or [])
    acknowledge = False
    if hard or (evolution.get("should_pause") and hard):
        message = (
            f"Breaking schema change: {hard[0].get('kind')} — sync paused"
            if hard and isinstance(hard[0], dict)
            else "Breaking schema change — sync paused"
        )
        _gate(
            "schema_drift",
            "block",
            message,
            ack_required=False,
            schema_evolution=evolution,
        )
        blockers.append({"id": "schema_drift", "message": message})
    elif action not in {"continue", "propagate", ""} or (
        evolution.get("should_pause") and not hard
    ):
        acknowledge = True
        message = (
            "Schema change detected. Confirm accepts it for this run and "
            "keeps the signed mappings."
        )
        _gate(
            "schema_drift",
            "pass",
            message,
            acknowledged_on_confirm=True,
            schema_evolution=evolution,
        )
    elif audit or drift.get("destination_only_columns"):
        columns = list(drift.get("destination_only_columns") or [])
        message = (
            "Non-breaking drift auto-passed"
            + (f" ({len(columns)} destination-only column(s))" if columns else "")
            + "."
        )
        _gate("schema_drift", "pass", message, audit=audit)
    else:
        _gate("schema_drift", "pass", "No schema drift.")

    passed = not blockers
    return {
        "passed": passed,
        "gates": gates,
        "blockers": blockers,
        "audit": audit,
        "acknowledge_on_confirm": acknowledge and passed,
        "preflight_scope": FAST_SCOPE,
    }


def idempotency_key(*, parent_id: str, mode: str, contract_fingerprint: str) -> str:
    return f"{parent_id}:{mode}:{contract_fingerprint}"


def stage_sync_ack(
    *,
    key: str,
    payload: dict[str, Any],
    preview: dict[str, Any],
    ledger: Any | None = None,
) -> dict[str, Any]:
    """Return one open approval for this parent and mode."""
    if ledger is None:
        from src.ai.copilot.ack_ledger import get_ack_ledger

        ledger = get_ack_ledger()
    with _lock:
        existing = _open.get(key)
        if existing:
            peek = ledger.peek(existing)
            if peek and not peek.get("consumed"):
                return {
                    "ack_id": existing,
                    "idempotent": True,
                    "preview": peek.get("preview") or preview,
                    "kind": "sync_now",
                }
            _open.pop(key, None)
        ack_id = ledger.put(kind="sync_now", payload=payload, preview=preview)
        _open[key] = ack_id
    return {
        "ack_id": ack_id,
        "idempotent": False,
        "preview": preview,
        "kind": "sync_now",
    }


def prepare_sync(
    *,
    mode: str,
    parent_request: dict[str, Any],
    parent_id: str,
    source_connected: bool,
    destination_connected: bool,
    drift: dict[str, Any] | None = None,
    source_error: str = "",
    destination_error: str = "",
    connectivity_checked: bool = True,
    schedule_id: str = "",
    ledger: Any | None = None,
) -> dict[str, Any]:
    """Stage the one confirm, or refuse when connectivity or a hard break fails."""
    chosen = (mode or "sync").strip().lower()
    if chosen not in {"sync", "full"}:
        raise SyncNowRefused("mode must be sync or full")
    report = evaluate_fast_preflight(
        source_connected=source_connected,
        destination_connected=destination_connected,
        drift=drift,
        source_error=source_error,
        destination_error=destination_error,
        connectivity_checked=connectivity_checked,
    )
    if not report["passed"]:
        raise SyncNowRefused(
            report["blockers"][0]["message"] if report["blockers"] else "Fast preflight failed",
            details={"fast_preflight": report},
        )
    child = child_request_dict(
        parent_request,
        mode=chosen,
        parent_job_id="" if schedule_id else parent_id,
        schedule_id=schedule_id,
    )
    if report.get("acknowledge_on_confirm"):
        child["schema_drift_acknowledged"] = True
        child["acknowledgment_actor"] = "sync-now"
        child["acknowledgment_reason"] = "Accept drift for this run"
    plan = child.get("full_resync") if chosen == "full" else {
        "sync_mode": child.get("sync_mode") or parent_request.get("sync_mode"),
        "destructive": False,
        "note": "Sync now reuses the signed contract.",
    }
    from services.schema_fingerprint import fingerprint_mappings

    fingerprint = fingerprint_mappings(list(child.get("mappings") or []))
    preview = {
        "mode": chosen,
        "parent_id": parent_id,
        "schedule_id": schedule_id,
        "sync_mode": plan.get("sync_mode") if isinstance(plan, dict) else child.get("sync_mode"),
        "destructive": bool(plan.get("destructive")) if isinstance(plan, dict) else False,
        "note": plan.get("note") if isinstance(plan, dict) else "",
        "confirm_reason": (
            "Full re-sync using the signed contract"
            if chosen == "full"
            else "Sync now using the signed contract"
        ),
        "fast_preflight": report,
    }
    payload = {
        "mode": chosen,
        "parent_job_id": "" if schedule_id else parent_id,
        "schedule_id": schedule_id,
        "transfer_request": None if schedule_id else child,
    }
    staged = stage_sync_ack(
        key=idempotency_key(parent_id=parent_id, mode=chosen, contract_fingerprint=fingerprint),
        payload=payload,
        preview=preview,
        ledger=ledger,
    )
    return staged


def begin_sync_now(payload: dict[str, Any]) -> tuple[dict[str, Any], Any | None]:
    """Create the child job. The caller starts a transfer request in the background.

    A schedule payload starts through the schedule runner, which already
    submits the job. The second return value is None in that case.
    """
    schedule_id = str(payload.get("schedule_id") or "").strip()
    mode = str(payload.get("mode") or "sync").strip().lower()
    if schedule_id and not payload.get("transfer_request"):
        from services.schedule_runner import _run_schedule

        job_id = _run_schedule(schedule_id, manual=True, resync_mode=mode)
        if not job_id:
            raise SyncNowRefused("Could not start this schedule.")
        return {
            "job_id": job_id,
            "schedule_id": schedule_id,
            "parent_job_id": "",
            "mode": mode,
            "status": "queued",
        }, None

    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import get_transfer_engine
    from src.transfer.models import transfer_request_from_dict

    request = transfer_request_from_dict(dict(payload.get("transfer_request") or {}))
    job_id = get_transfer_engine()._create_pending_job(request)
    parent = str(payload.get("parent_job_id") or getattr(request, "parent_job_id", "") or "")
    mongo = get_mongodb_service()
    mongo.update_job_status(
        job_id,
        "pending",
        parent_job_id=parent,
        resync_mode=mode,
        message=(
            f"Full re-sync of {parent}" if mode == "full" else f"Sync now of {parent}"
        ),
    )
    if parent:
        mongo.update_job_fields(parent, {"last_sync_job_id": job_id})
    return {
        "job_id": job_id,
        "parent_job_id": parent,
        "mode": mode,
        "status": "pending",
    }, request
