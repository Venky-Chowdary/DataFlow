"""D208 — what an operator re-run of a keyed upsert reads, and what it reports.

Contract (rerun, retry, MCP and REST alike — the Pilot's Confirm calls the
REST handlers):

* An operator re-run or retry of an upsert is a **full second pass**: it
  re-reads the whole source, ignoring the saved watermark, and upserts every
  row onto the key. Before this module, an incremental upsert
  (``incremental_deduped``) re-run said "a second full pass merges onto the
  declared key" while the new job read only rows past the saved watermark — a
  destination row edited or deleted by hand stayed wrong.
* The saved watermark is left where it is (neither used nor advanced): the
  route's next scheduled run continues from it, and re-upserting rows a full
  pass already wrote is idempotent.
* A pure upsert never deletes. Rows that exist only in the destination are
  kept, and the job report says so (``read_scope.destination_only_rows``).
* Resume continues from its checkpoint and a schedule beat continues from the
  watermark — neither is a full pass, and their reports say what they read.
* An incremental *append* or SCD2 re-run is not converted: a full pass would
  write committed rows again (append) or reopen closed history (SCD2).
"""

from __future__ import annotations

from typing import Any

READ_SCOPE_KEY = "read_scope"

DESTINATION_ONLY_KEPT = (
    "kept — an upsert inserts and updates by key and never deletes; rows that "
    "exist only in the destination stay (mirror is the mode that deletes them)"
)


def _canonical(mode: Any) -> str:
    from services.sync_cursor import normalize_sync_mode

    return normalize_sync_mode(str(mode or ""), default="")


def _effective_mode(xfer_req: Any) -> str:
    from services.sync_cursor import resolve_effective_sync_mode, resolve_sync_contract

    contract = resolve_sync_contract(list(getattr(xfer_req, "stream_contracts", None) or []))
    return resolve_effective_sync_mode(
        getattr(xfer_req, "sync_mode", "") or "",
        contract.sync_mode if contract else None,
    )


def _stored_watermark(parent_job: dict | None) -> tuple[Any, str]:
    job = parent_job or {}
    report = job.get("incremental_watermark") or {}
    if isinstance(report, dict) and report.get("after") not in (None, ""):
        return report.get("after"), str(report.get("cursor_key") or "")
    value = job.get("cursor_value")
    return (None if value in (None, "") else value), ""


def apply_full_pass(xfer_req: Any, *, parent_job: dict | None, action: str) -> dict | None:
    """Make an operator re-run/retry of an incremental upsert a full pass.

    Returns the ``read_scope`` record for the new job, or ``None`` when the
    request is not an incremental upsert (nothing changes).
    """
    requested = _effective_mode(xfer_req)
    if requested != "incremental_deduped":
        return None
    xfer_req.sync_mode = "upsert"
    contracts = []
    for contract in list(getattr(xfer_req, "stream_contracts", None) or []):
        if isinstance(contract, dict) and _canonical(contract.get("sync_mode")) in (
            "incremental_deduped", ""
        ):
            contract = {**contract, "sync_mode": "upsert"}
        contracts.append(contract)
    xfer_req.stream_contracts = contracts
    watermark, cursor_key = _stored_watermark(parent_job)
    return {
        "action": action,
        "mode": "upsert",
        "requested_mode": requested,
        "scope": "full_source",
        "watermark": "ignored",
        "stored_watermark": watermark,
        "cursor_key": cursor_key or None,
        "watermark_after_run": "unchanged — the next incremental run continues from it",
        "destination_only_rows": DESTINATION_ONLY_KEPT,
        "reason": (
            f"{action.capitalize()} of an incremental upsert is a full second pass: "
            "the whole source is re-read and every row is upserted on the key; the "
            f"saved watermark ({watermark if watermark not in (None, '') else 'none'}) "
            "is ignored."
        ),
    }


def full_pass_decision(decision: dict[str, Any], scope: dict | None) -> dict[str, Any]:
    """The rerun/retry decision, worded as what the new job will do."""
    if not scope:
        return decision
    return {**decision, "read_scope": "full_source", "reason": scope["reason"]}


def run_read_scope(
    destination_summary: dict | None, *, rows_read: Any, staged: dict | None = None
) -> dict | None:
    """The ``read_scope`` a finished non-incremental upsert job reports.

    ``staged`` is the record a re-run/retry put on the job at creation; a plain
    upsert run (MCP, REST, schedule) reports the same shape.
    """
    summary = destination_summary or {}
    mode = _canonical(summary.get("sync_mode") or summary.get("write_mode") or "")
    if not staged and mode != "upsert":
        return None
    out = dict(staged or {})
    out.setdefault("mode", "upsert")
    out.setdefault("scope", "full_source")
    out.setdefault("watermark", "not used")
    out.setdefault("destination_only_rows", DESTINATION_ONLY_KEPT)
    try:
        out["rows_read"] = int(rows_read or 0)
    except (TypeError, ValueError):
        out["rows_read"] = None
    source_rows = summary.get("source_row_count", summary.get("source_rows"))
    dest_rows = summary.get("dest_count")
    try:
        if source_rows is not None and dest_rows is not None:
            out["destination_only_count"] = max(0, int(dest_rows) - int(source_rows))
    except (TypeError, ValueError):
        pass
    return out


#: ``last_status`` values that are bookkeeping, not how a run ended: a finding
#: approved, a park re-armed or rejected, a run still open. Reading them as a
#: failed run made the next run after a *completed* one a full pass (D433).
_NOT_A_RUN_OUTCOME = frozenset({"", "approved", "rearmed", "rejected", "pending", "running", "skipped"})


def _last_written_run_failed(sched: Any) -> bool:
    """D296 / D433: did the schedule's last run that reached a job fail?

    A pre-run refusal (source drift, a parked decision, a residency refusal) is
    recorded as a failed run with no job: nothing was written, so there is no
    stale row to repair and the watermark is where the last accepted run left
    it (R1). Only a run whose job failed can have left the destination behind
    the source. The newest finished run record that had a job decides; a
    refusal, a skip and the record still open for this very run are skipped.
    ``last_status`` decides only for a document with no such record, and only
    when it is a run outcome: it is also overwritten by bookkeeping
    ("approved" after a released finding), and that read a completed run as
    failed, so the next run re-read the whole source instead of the delta.
    """
    from services.job_status import is_completed
    from services.schedule_run_lifecycle import SKIPPED, is_open

    for entry in reversed(list(getattr(sched, "run_history", None) or [])):
        if not isinstance(entry, dict) or not str(entry.get("job_id") or ""):
            continue
        status = str(entry.get("status") or "")
        if status == SKIPPED or is_open(status):
            continue
        return not is_completed(status)
    last_status = str(getattr(sched, "last_status", "") or "").strip().lower()
    return (
        bool(str(getattr(sched, "last_job_id", "") or ""))
        and last_status not in _NOT_A_RUN_OUTCOME
        and not is_completed(last_status)
    )


def _route_watermark_stored(xfer_req: Any) -> bool:
    """True when the route has a committed watermark, or when it cannot be read."""
    try:
        from services.preflight_cursor_gate import read_scope_for_transfer_request

        scope = read_scope_for_transfer_request(xfer_req)
    except Exception:  # noqa: BLE001 - unknown state keeps the full pass (repair first)
        return True
    if not str(getattr(scope, "cursor_key", "") or ""):
        return True  # not an incremental route this resolver addresses: keep D210
    return getattr(scope, "watermark", None) not in (None, "")


def apply_schedule_full_pass(xfer_req: Any, sched: Any, *, attempt: int) -> dict | None:
    """D210/D208: a schedule's retry, and its first run after a failed run, is a
    full pass of an incremental upsert.

    A failed attempt can leave a destination row that no longer matches the
    source (QA D210: an Oracle row kept its old value where the source is now
    ``''``). Continuing from the watermark never reads that row again, so the
    stale value would be kept forever. Re-upserting every row is idempotent and
    the saved watermark is neither used nor moved.
    """
    after_failure = attempt > 0 or _last_written_run_failed(sched)
    if not after_failure:
        return None
    if not _route_watermark_stored(xfer_req):
        # D433: no watermark has been committed on this route (the failed run
        # was the first), so the incremental run already reads every row and
        # upserts it on the key — the same repair a full pass makes. Converted
        # to a full pass it left the watermark unset, and the next run re-read
        # the whole source again instead of the delta since this one.
        return None
    parent: dict | None = None
    last_job_id = str(getattr(sched, "last_job_id", "") or "")
    if last_job_id:
        try:
            from services.mongodb_service import get_mongodb_service

            parent = get_mongodb_service().get_job(last_job_id)
        except Exception:  # noqa: BLE001 - the report's watermark is informational
            parent = None
    return apply_full_pass(xfer_req, parent_job=parent, action="retry")
