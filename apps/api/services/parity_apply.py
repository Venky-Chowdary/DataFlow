"""Parity Studio: deciding a fix — approve, apply, re-check, settle — or dismissing it.

A decision is taken against the report of one state version and written by
compare-and-swap on it (:mod:`services.parity_state`), so a concurrent decision
(another replica, another person) is a 409, never a lost update. Every decision
is also recorded in the one repair approval workflow,
:mod:`services.agentic_repair` (a ``source="parity"`` proposal decided with
``decide_proposal``), and the routes need the same permission as that
workflow's decide route, ``job.manage``.

A write fix is first *claimed* (status ``applying``; the round limit is checked
inside the compare-and-swap), then applied through an existing write path, then
*settled*: its keys are re-read on both sides and a round is recorded. A key
re-sync is a transfer job of its own, so it runs on the existing transfer
scheduler (``run_transfer_async``: the local pool or the worker fleet) and is
settled when that job finishes — from its completion callback, or by the next
read of the report on any replica — so no request waits on a source scan.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable

from services.parity_evidence import request_of, target_to_source
from services.parity_fixes import (
    APPLYING,
    _find,
    public_fix,
    write_fixes_used,
)
from services.parity_state import (
    MAX_APPLIED_ROUNDS,
    ParityConflict,
    ParityError,
    ParityInvalid,
    audit,
    check_version,
    job_store,
    load_job,
    now_iso,
    state_of,
    update_state,
)

logger = logging.getLogger(__name__)

SIGNED_IS_FINAL = "This report is signed, and a signed report is final; re-run the job for a new report."
#: A claim with no running job behind it after this long is settled as failed.
STALE_CLAIM_SECONDS = 3600
_CHILD_OK = frozenset({"completed", "completed_with_quarantine", "completed_with_warnings", "success"})
_CHILD_FAILED = frozenset({"failed", "cancelled", "canceled", "error"})


def not_signed(state: dict[str, Any]) -> None:
    """A signed report is final: no decision may change what was signed."""
    if state.get("signed"):
        raise ParityConflict(SIGNED_IS_FINAL)


def _still_proposed(state: dict[str, Any], fix_id: str) -> None:
    """Inside the compare-and-swap: nobody decided this fix since it was read, nor signed the report."""
    not_signed(state)
    stored = (state.get("fixes") or {}).get(fix_id)
    if stored:
        raise ParityConflict(f"Fix {fix_id} is already {stored.get('status')}.")


def _decision_report(job_id: str, fix_id: str, expected_version: int | None) -> tuple[dict, dict, dict]:
    from services.parity_report import build_parity_report

    job = load_job(job_id)
    check_version(job, expected_version)
    not_signed(state_of(job))
    report = build_parity_report(job, reread=True, job_id=job_id)
    fix = _find(report, fix_id)
    if fix["status"] != "proposed":
        raise ParityConflict(f"Fix {fix_id} is already {fix['status']}.")
    return job, report, fix


def summarized(job: dict[str, Any], state: dict[str, Any], job_id: str) -> dict[str, Any]:
    """The report of ``state`` (re-read), with its compact summary set on ``state``.

    Called inside a compare-and-swap change, so the dashboard summary is
    written in the same write as the decision that changed it.
    """
    from services.parity_report import build_parity_report, compact_summary

    report = build_parity_report({**job, "parity": state}, reread=True, job_id=job_id)
    state["summary"] = compact_summary(report)
    return report


def _with_state(report: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    report["version"] = int(state.get("version") or 0)
    report["rounds"] = list(state.get("rounds") or [])
    return report


def _repair_decision(job_id: str, fix: dict[str, Any], *, approve: bool, actor: str,
                     apply_fn: Callable[[list[dict[str, Any]]], dict[str, Any]] | None = None) -> Any:
    """Record the decision in the repair approval workflow (one workflow for every fix)."""
    from services.agentic_repair import decide_proposal, propose_from_parity

    proposal = propose_from_parity(job_id=job_id, fix=fix)
    return decide_proposal(proposal.id, approve=approve, actor=actor or "anonymous", apply_fn=apply_fn)


# ── approve ──────────────────────────────────────────────────────────────────


def approve_fix(
    job_id: str, fix_id: str, *, actor: str, note: str = "", expected_version: int | None = None,
    start_rerun: Callable[[], str] | None = None,
) -> dict[str, Any]:
    """Approve one fix and apply it; returns the new report.

    ``accept`` is recorded at once. ``widen_column`` and ``rerun`` are applied
    and settled before returning; ``resync_keys`` returns while its transfer
    job runs (the fix reads ``applying`` until it is settled).
    """
    from services.agentic_repair import parity_proposal_id

    job, report, fix = _decision_report(job_id, fix_id, expected_version)
    if not fix["applicable"]:
        raise ParityConflict(str(fix.get("not_applicable_reason") or "This fix cannot be applied."))
    if fix["action"] == "rerun" and start_rerun is None:
        raise ParityConflict("A re-run can only be started from the API.")
    version = int(report["version"])
    note = note[:500]
    details = {"fix_id": fix_id, "action": fix["action"], "difference_ids": fix["difference_ids"], "note": note}
    record = {**public_fix(fix), "approved_by": actor, "decided_at": now_iso(), "note": note,
              "repair_proposal_id": parity_proposal_id(job_id, fix_id)}
    if fix["action"] == "accept":
        return _accept(job, job_id, report, fix, record, actor=actor, note=note, details=details)

    def claim(_job: dict[str, Any], state: dict[str, Any]) -> None:
        _still_proposed(state, fix_id)
        if write_fixes_used(state) >= MAX_APPLIED_ROUNDS:
            raise ParityConflict(
                f"This job has used its {MAX_APPLIED_ROUNDS} applied fix rounds; re-run the job to start again."
            )
        # What the re-check needs once the write is done (internal: never exported).
        target = {k: fix[k] for k in ("_table", "_keys", "_key_column") if fix.get(k)}
        state["fixes"][fix_id] = {
            **record, **target, "_unexplained_before": report["parity"]["unexplained"],
            "status": APPLYING, "applied_at": None, "result": None,
        }

    update_state(job_id, claim, expected_version=version)
    audit("job.parity.fix_approved", job=job, job_id=job_id, actor=actor, details=details)
    if fix["action"] == "resync_keys":
        return _start_resync(job, job_id, fix, actor)

    def apply(_actions: list[dict[str, Any]]) -> dict[str, Any]:
        if fix["action"] == "widen_column":
            return {"applied": True, "result": _widen_apply(job, fix)}
        started = start_rerun() if start_rerun is not None else ""
        return {"applied": True, "result": f"Started re-run {started}; it gets its own parity report when it completes."}

    decided = _repair_decision(job_id, fix, approve=True, actor=actor, apply_fn=apply)
    outcome = dict(decided.apply_result or {})
    ok = decided.status == "applied"
    result = str(outcome.get("result") if ok else outcome.get("error") or outcome.get("reason") or "the fix failed")
    return settle_fix(job_id, fix_id, ok=ok, result=result[:500])


def _accept(job: dict[str, Any], job_id: str, report: dict[str, Any], fix: dict[str, Any],
            record: dict[str, Any], *, actor: str, note: str, details: dict[str, Any]) -> dict[str, Any]:
    if not note.strip():
        raise ParityInvalid("Accepting a difference needs a note: say why it is acceptable.")
    fix_id = fix["id"]
    scope = {
        d["id"]: {"keys": list(d["_keys"]), "keyless_rows": int(d["_keyless"]), "rows": int(d["rows"])}
        for d in report["differences"] if d["id"] in fix["difference_ids"]
    }
    result = f"Accepted {len(fix['difference_ids'])} difference(s)."

    def accept(job_now: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
        _still_proposed(state, fix_id)
        for did in fix["difference_ids"]:
            # Exactly the rows the person saw: later rows with the same cause are not covered.
            state["acceptances"][did] = {"by": actor, "at": now_iso(), "note": note, "fix_id": fix_id,
                                         **scope.get(did, {"keys": [], "keyless_rows": 0, "rows": 0})}
        state["fixes"][fix_id] = {**record, "status": "applied", "applied_at": now_iso(), "result": result}
        return summarized(job_now, state, job_id)

    _updated, state, after = update_state(job_id, accept, expected_version=int(report["version"]))
    _repair_decision(job_id, fix, approve=True, actor=actor,
                     apply_fn=lambda _a: {"applied": True, "result": result})
    audit("job.parity.fix_approved", job=job, job_id=job_id, actor=actor, details=details)
    audit("job.parity.fix_applied", job=job, job_id=job_id, actor=actor,
          details={**details, "result": result, "unexplained_after": after["parity"]["unexplained"]})
    return _with_state(after, state)


# ── key re-sync: a transfer job on the existing scheduler ────────────────────


def _resync_request(job: dict[str, Any], job_id: str, fix: dict[str, Any], actor: str) -> Any:
    """The job's own route, as an upsert of exactly the fix's keys."""
    from src.transfer.models import transfer_request_from_dict

    keys = list(fix.get("_keys") or [])
    key_tgt = str(fix.get("_key_column") or "")
    key_src = target_to_source(job).get(key_tgt, key_tgt)
    req = transfer_request_from_dict(request_of(job))
    req.sync_mode = "upsert"
    req.source_filter = {"column": key_src, "op": "in", "value": keys}
    contracts = [dict(c) for c in req.stream_contracts or [] if isinstance(c, dict)] or [
        {"name": str(req.source.table or req.source.collection or ""), "selected": True}
    ]
    for c in contracts:
        c.pop("cursor_field", None)
        c.update({"sync_mode": "upsert", "primary_key": [key_src]})
    req.stream_contracts = contracts
    req.skip_preflight = True
    req.limit = 0
    req.parent_job_id = job_id
    req.workspace_id = str(job.get("workspace_id") or req.workspace_id or "")
    req.triggered_by = actor or "parity studio"
    req.acknowledgment_actor = actor
    req.acknowledgment_reason = (
        f"Parity Studio fix {fix['id']}: re-sync {len(keys)} key(s) approved by {actor or 'operator'}."
    )
    req.idempotency_key = f"parity-{job_id}-{fix['id']}"
    return req


def _launch(child_id: str, req: Any) -> Any:
    """Run the re-sync job on the existing transfer scheduler; returns its future (or None)."""
    from src.transfer.background import run_transfer_async

    return run_transfer_async(child_id, req, resume=False)


def _start_resync(job: dict[str, Any], job_id: str, fix: dict[str, Any], actor: str) -> dict[str, Any]:
    from src.transfer.engine import get_transfer_engine

    fix_id = fix["id"]
    try:
        req = _resync_request(job, job_id, fix, actor)
        child_id = get_transfer_engine()._create_pending_job(req)
    except Exception as exc:  # noqa: BLE001 - a failed start is recorded on the fix, never hidden
        error = str(exc)[:400]
        _repair_decision(job_id, fix, approve=True, actor=actor,
                         apply_fn=lambda _a: {"applied": False, "error": error})
        return settle_fix(job_id, fix_id, ok=False, result=f"The re-sync could not start: {error}")

    def link(_job: dict[str, Any], state: dict[str, Any]) -> None:
        record = state["fixes"].get(fix_id) or {}
        if record.get("status") == APPLYING:
            record["child_job_id"] = child_id
            record["result"] = f"Re-sync job {child_id} is running."

    update_state(job_id, link)
    _repair_decision(job_id, fix, approve=True, actor=actor,
                     apply_fn=lambda _a: {"applied": False, "reason": "running", "child_job_id": child_id})
    try:
        future = _launch(child_id, req)
    except Exception as exc:  # noqa: BLE001 - the scheduler refused: the fix failed
        return settle_fix(job_id, fix_id, ok=False, result=f"The re-sync job {child_id} could not be scheduled: "
                          f"{str(exc)[:300]}")
    if future is not None and hasattr(future, "add_done_callback"):
        future.add_done_callback(lambda _f: _settle_quietly(job_id))
    settled = settle_pending(job_id)
    if settled is not None:
        return settled
    from services.parity_report import build_parity_report

    return build_parity_report(load_job(job_id), reread=True, job_id=job_id)


def _settle_quietly(job_id: str) -> None:
    try:
        settle_pending(job_id)
    except Exception as exc:  # noqa: BLE001 - the next read settles it
        logger.info("parity re-sync not settled yet for %s (%s)", job_id, type(exc).__name__)


def _rows_written(child: dict[str, Any]) -> int | None:
    dest = child.get("destination_summary") if isinstance(child.get("destination_summary"), dict) else {}
    for value in (dest.get("rows_written"), child.get("records_transferred"), child.get("rows_written")):
        try:
            if value is not None and not isinstance(value, bool):
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _resync_outcome(record: dict[str, Any], child: dict[str, Any] | None) -> tuple[bool, str] | None:
    """(ok, result) once the re-sync job is finished, else None."""
    child_id = str(record.get("child_job_id") or "")
    keys = list(record.get("_keys") or [])
    key_col = str(record.get("_key_column") or "the key")
    if child is None:
        return False, f"The re-sync job {child_id} is gone, so the fix cannot be confirmed."
    status = str(child.get("status") or "").strip().lower()
    if status in _CHILD_FAILED:
        error = str(child.get("error") or child.get("message") or status)[:300]
        return False, f"re-sync job {child_id} {status}: {error}"
    if status not in _CHILD_OK:
        return None
    written = _rows_written(child)
    if keys and written == 0:
        # A key rendering the source column does not match selects nothing: never "re-synced".
        return False, (f"Re-sync job {child_id} wrote no rows: the source held none of the {len(keys)} key(s) "
                       f"as rendered on {key_col}.")
    tail = f"; {written:,} row(s) written" if written is not None else ""
    return True, f"Re-synced {len(keys)} key(s) through job {child_id} (upsert on {key_col}{tail})."


def _age_seconds(iso: str | None) -> float:
    try:
        then = datetime.fromisoformat(str(iso))
    except (TypeError, ValueError):
        return 0.0
    if then.tzinfo is None:
        then = then.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - then).total_seconds()


def settle_pending(job_id: str) -> dict[str, Any] | None:
    """Settle every claimed fix whose work is over; returns the last settled report, or None.

    Cheap when nothing is applying (one job read). A re-sync is settled when
    its transfer job is finished; a claim with nothing running behind it for
    :data:`STALE_CLAIM_SECONDS` (the API restarted mid-apply) settles as failed.
    """
    from services.agentic_repair import record_outcome

    state = state_of(load_job(job_id))
    settled = None
    for fix_id, record in list((state.get("fixes") or {}).items()):
        if record.get("status") != APPLYING:
            continue
        child_id = str(record.get("child_job_id") or "")
        if child_id:
            outcome = _resync_outcome(record, job_store().get_job(child_id))
        elif _age_seconds(record.get("decided_at")) > STALE_CLAIM_SECONDS:
            outcome = (False, "The fix was approved but never finished (the API restarted while applying it).")
        else:
            outcome = None
        if outcome is None:
            continue
        ok, result = outcome
        try:
            settled = settle_fix(job_id, fix_id, ok=ok, result=result)
        except ParityConflict:
            continue  # another replica settled it first
        if record.get("repair_proposal_id"):
            record_outcome(str(record["repair_proposal_id"]), applied=ok, result=result,
                           actor=str(record.get("approved_by") or ""))
    return settled


# ── widen: an existing in-place DDL helper ───────────────────────────────────


def _widen_apply(job: dict[str, Any], fix: dict[str, Any]) -> str:
    from connectors.schema_drift import widen_existing_columns_native
    from services.dest_precount import warehouse_sql_engine
    from services.dialect_profiles import schema_from_cfg
    from services.parity_evidence import _endpoints
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    _req, _source, destination = _endpoints(job)
    cfg = resolve_connector_config(destination)
    dialect = resolve_driver_type(str(cfg.get("type") or destination.format or "")).lower()
    with warehouse_sql_engine(dialect, cfg) as eng:
        raw = eng.raw_connection()
        try:
            ran = widen_existing_columns_native(
                raw.cursor(), dialect, schema_from_cfg(dialect, cfg) or None, str(fix["_table"]),
                [str(fix["_column"])], [str(fix["_new_type"])], backfill=True,
            )
            raw.commit()
        finally:
            raw.close()
    if not ran:
        raise RuntimeError("the destination column was not changed: it is already at least as wide")
    return "Ran: " + "; ".join(ran)


# ── settle and dismiss ───────────────────────────────────────────────────────


def settle_fix(job_id: str, fix_id: str, *, ok: bool, result: str) -> dict[str, Any]:
    """Record how a claimed write fix ended: re-check its keys, append the round.

    Written by compare-and-swap on the freshest state (retried, never pinned:
    the write already happened and its outcome must not be lost to a
    concurrent unrelated change).
    """
    from services.parity_report import build_parity_report

    def settle(job: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
        record = state["fixes"].get(fix_id)
        if not record or record.get("status") != APPLYING:
            raise ParityConflict(f"Fix {fix_id} is not being applied.")
        record.update(status="applied" if ok else "failed", applied_at=now_iso(), result=result)
        if ok and record["action"] in {"resync_keys", "widen_column"}:
            after = build_parity_report({**job, "parity": state}, reread=True, job_id=job_id)
            still = {k for d in after["differences"] if not d["cause"]["explained"] for k in d["_keys"]}
            table = str(record.get("_table") or "")
            keys = list(record.get("_keys") or [])
            if table and keys:
                resolved = set(state["resolved"].get(table) or []) | (set(keys) - still)
                state["resolved"][table] = sorted(resolved)
            state["rounds"].append({
                "n": len(state["rounds"]) + 1, "at": now_iso(),
                "unexplained_before": int(record.get("_unexplained_before") or 0),
                "unexplained_after": after["parity"]["unexplained"], "fix_id": fix_id,
            })
        return summarized(job, state, job_id)

    job, state, after = update_state(job_id, settle)
    record = state["fixes"][fix_id]
    actor = str(record.get("approved_by") or "")
    details = {"fix_id": fix_id, "action": record["action"], "difference_ids": record.get("difference_ids"),
               "result": result[:500]}
    if ok:
        audit("job.parity.fix_applied", job=job, job_id=job_id, actor=actor,
              details={**details, "unexplained_after": after["parity"]["unexplained"]})
    else:
        audit("job.parity.fix_failed", job=job, job_id=job_id, actor=actor,
              details={**details, "error": result[:500]}, outcome="failed")
    return _with_state(after, state)


def dismiss_fix(
    job_id: str, fix_id: str, *, actor: str, note: str = "", expected_version: int | None = None,
) -> dict[str, Any]:
    from services.agentic_repair import parity_proposal_id

    job, report, fix = _decision_report(job_id, fix_id, expected_version)
    note = note[:500]

    def dismiss(job_now: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
        _still_proposed(state, fix_id)
        state["fixes"][fix_id] = {**public_fix(fix), "status": "dismissed", "approved_by": actor,
                                  "decided_at": now_iso(), "note": note, "result": note or None,
                                  "repair_proposal_id": parity_proposal_id(job_id, fix_id)}
        return summarized(job_now, state, job_id)

    _updated, state, after = update_state(job_id, dismiss, expected_version=int(report["version"]))
    _repair_decision(job_id, fix, approve=False, actor=actor)
    audit("job.parity.fix_dismissed", job=job, job_id=job_id, actor=actor,
          details={"fix_id": fix_id, "action": fix["action"], "note": note})
    return _with_state(after, state)


__all__ = [
    "ParityError", "SIGNED_IS_FINAL", "approve_fix", "dismiss_fix", "not_signed", "settle_fix",
    "settle_pending", "summarized",
]
