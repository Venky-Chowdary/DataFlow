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
