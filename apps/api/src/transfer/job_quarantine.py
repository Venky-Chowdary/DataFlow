"""Job-level quarantine persistence and rollback-plan attachment.

Split out of ``engine.py`` (a god module over its size budget). These helpers
own the durability contract for rejected rows: every row the writers refuse is
persisted to the DLQ and counted on the job document, so a transfer can never
silently drop rows, and an operator always has a replay handle plus a rollback
plan for what already landed.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def rows_with_findings(details: list[dict[str, Any]]) -> int:
    """Distinct source rows that carry a finding of their own."""
    return len(
        {
            d.get("row")
            for d in details
            if isinstance(d, dict) and d.get("row") is not None
        }
    )


def split_refused_unit(
    details: list[dict[str, Any]], rejected_rows: int, summary: dict[str, Any]
) -> int:
    """Name the rows a refused write unit rolled back, and return the true rejects.

    A refused unit reports every uncommitted row as rejected, because the writers
    count ``source - kept`` and an abort keeps nothing. A 5,000-row batch holding
    2,500 bad cells therefore claimed "5,000 quarantined" while only 2,500 rows
    had a finding to review — a total Inspect could never explain, and no
    remediation could act on. Quarantine is what the writer found; the rest of
    the unit was rolled back with it.
    """
    found = rows_with_findings(details)
    if not found or rejected_rows <= found:
        return rejected_rows
    summary["rows_rolled_back"] = rejected_rows - found
    summary["rows_refused_unit"] = rejected_rows
    summary["rejected_rows"] = found
    return found


def _persist_checkpoint_quarantine_delta(
    job_id: str,
    checkpoint: dict[str, Any] | None,
    *,
    request: Any = None,
    last_persisted: list[int],
) -> None:
    """Persist new rejected rows from a buffered checkpoint before continuing.

    Stream path already fail-closes per batch. Buffered writers must not wait
    until terminal status — crash mid-write must not lose quarantine durability.
    ``last_persisted`` is a single-element list holding the count already written.
    """
    details = list((checkpoint or {}).get("rejected_details") or [])
    if not details:
        return
    prev = int(last_persisted[0] or 0)
    if len(details) <= prev:
        return
    new_details = details[prev:]
    try:
        from services.quarantine_dlq import persist_rejected_rows

        persist_rejected_rows(
            job_id=job_id,
            rejected_details=new_details,
            workspace_id=str(getattr(request, "workspace_id", "") or "")
            if request
            else "",
            source="buffered_checkpoint",
            connector=str(
                getattr(getattr(request, "destination", None), "format", "")
                or getattr(getattr(request, "destination", None), "kind", "")
                or ""
            )
            if request
            else "",
        )
        last_persisted[0] = len(details)
        if isinstance(checkpoint, dict):
            checkpoint["quarantine_dlq_persisted_count"] = len(details)
    except Exception as exc:
        raise RuntimeError(
            "Quarantine DLQ persist failed at buffered checkpoint — refuse to "
            f"continue (rows cannot disappear): {exc}"
        ) from exc


def checkpoint_quarantine_summary(
    checkpoint: dict[str, Any],
    details: list[dict[str, Any]],
    preview: list[dict[str, Any]],
    total: int,
    truncated: bool,
) -> dict[str, Any]:
    """Destination summary for a running checkpoint, findings named separately.

    A writer counts ``source - kept`` per unit, so a refused batch reports every
    uncommitted row as rejected. Written straight onto the job that becomes
    "5,000 quarantined / 0 findings" on Inspect: the rows without a finding of
    their own were rolled back with the batch, and no finding exists to show.
    """
    summary: dict[str, Any] = {
        "checksum": checkpoint.get("checksum", ""),
        "rejected_details": preview,
        "rejected_details_total": total,
        "rejected_details_truncated": truncated,
        "quarantine_checkpoint_durable": True,
    }
    rejected = int(checkpoint.get("rejected_rows") or total or 0)
    summary["rejected_rows"] = split_refused_unit(details, rejected, summary)
    return summary


def _persist_job_quarantine(
    job_id: str,
    dest_summary: dict[str, Any],
    request: Any = None,
    *,
    already_persisted: list[int] | None = None,
) -> None:
    """Durable DLQ write for rejected rows — fail closed if control-plane persist fails.

    Writes control-plane JSONL/Mongo **and** (when supported) a destination
    ``{table}_df_quarantine`` table so operators can query/promote with SQL.

    Module 5: never complete as success/quarantine-ok when rejected rows exist
    but control-plane DLQ is not durable (replay would find nothing).

    When checkpoint/stream already persisted a prefix of ``rejected_details``,
    only the delta is appended to the control-plane DLQ (no duplicate rows).
    Destination quarantine table is still written once here (not mid-stream).
    """
    details = list(dest_summary.get("rejected_details") or [])
    if not details:
        dest_summary["quarantine_durable"] = True
        return
    from services.quarantine_dlq import (
        is_contract_skip_detail,
        persist_rejected_rows,
        replay_quarantine_details,
    )

    skip_n = sum(1 for d in details if is_contract_skip_detail(d))
    replay_all = replay_quarantine_details(details)
    dest_summary["rows_skipped_contract"] = skip_n
    dest_summary["rows_quarantined_replay"] = len(replay_all)
    dest_summary["rejected_details_total"] = len(details)
    dest_summary["rejected_details_truncated"] = len(details) > 2000
    already = int(
        (already_persisted[0] if already_persisted else None)
        or dest_summary.get("quarantine_dlq_persisted_count")
        or 0
    )
    already = max(0, min(already, len(details)))
    delta = details[already:]
    if not replay_all:
        # SKIP_ROW / audit-skip only — persist skip_audit (not replay), then ok.
        try:
            if delta:
                persist_rejected_rows(
                    job_id=job_id,
                    rejected_details=delta,
                    workspace_id=str(getattr(request, "workspace_id", "") or "")
                    if request
                    else "",
                    source="universal_engine",
                    connector=str(
                        getattr(getattr(request, "destination", None), "format", "")
                        or getattr(getattr(request, "destination", None), "kind", "")
                        or ""
                    )
                    if request
                    else "",
                )
            dest_summary["quarantine_durable"] = True
            dest_summary["quarantine_dlq_persisted_count"] = len(details)
            if already_persisted is not None:
                already_persisted[0] = len(details)
        except Exception as exc:
            dest_summary["quarantine_skip_audit_error"] = str(exc)[:300]
            # Replay not required for skip-only; still surface audit loss.
            dest_summary["quarantine_durable"] = True
            dest_summary["quarantine_dlq_persisted_count"] = already
        return
    try:
        if delta:
            persist_rejected_rows(
                job_id=job_id,
                rejected_details=delta,
                workspace_id=str(getattr(request, "workspace_id", "") or "")
                if request
                else "",
                source="universal_engine",
                connector=str(
                    getattr(getattr(request, "destination", None), "format", "")
                    or getattr(getattr(request, "destination", None), "kind", "")
                    or ""
                )
                if request
                else "",
            )
        dest_summary["quarantine_dlq_persisted_count"] = len(details)
        if already_persisted is not None:
            already_persisted[0] = len(details)
    except Exception as exc:
        dest_summary["quarantine_dlq_error"] = str(exc)[:300]
        dest_summary["quarantine_durable"] = False
    else:
        dest_summary["quarantine_durable"] = True

    # Destination-side DLQ table (SQL sinks). Failures are surfaced — never silent.
    # Control-plane durability remains the fail-closed authority for Module 5.
    # Skip when a prior finalize already wrote the dest quarantine for this job.
    # SKIP_ROW audit skips are excluded — not replay holdouts.
    if (
        request is not None
        and getattr(request, "destination", None) is not None
        and not dest_summary.get("dest_quarantine_rows")
        and replay_all
    ):
        try:
            from services.dest_quarantine import write_dest_quarantine

            dest_result = write_dest_quarantine(
                request.destination,
                replay_all,
                job_id=job_id,
            )
            dest_summary["dest_quarantine"] = dest_result
            if dest_result.get("ok") and not dest_result.get("skipped"):
                dest_summary["dest_quarantine_table"] = dest_result.get("table")
                dest_summary["dest_quarantine_rows"] = dest_result.get("rows_written")
        except Exception as exc:
            dest_summary["dest_quarantine_error"] = str(exc)[:300]
            dest_summary.setdefault(
                "dest_quarantine", {"ok": False, "error": str(exc)[:300]}
            )

    from services.quarantine_dlq import assert_quarantine_durable_or_raise

    assert_quarantine_durable_or_raise(dest_summary)


def fail_closed_on_silent_loss(
    *,
    job_id: str,
    request: Any,
    dest_summary: dict[str, Any],
    recon: dict[str, Any],
    rows_written: int,
    operation: str,
) -> Any | None:
    """Refuse terminal success when a measured dest population does not close.

    Returns a failed ``TransferResult`` or ``None`` when the ledger is honest
    (balanced, or dest unmeasured). Owner: ``services.row_conservation``.
    """
    from services.row_conservation import (
        PopulationConservationError,
        assert_population_conservation_closed,
    )
    from src.transfer.models import TransferResult

    try:
        ledger = assert_population_conservation_closed(
            {
                "records_processed": rows_written,
                "sync_mode": str(getattr(request, "sync_mode", "") or ""),
                "reconciliation": recon,
                "destination_summary": dest_summary,
                "rejected_rows": dest_summary.get("rejected_rows"),
                "coerced_null_rows": dest_summary.get("coerced_null_rows"),
            },
            validation_mode=str(getattr(request, "validation_mode", "") or ""),
        )
    except PopulationConservationError as exc:
        from services.mongodb_service import get_mongodb_service

        note = str(exc)
        dest_summary = dict(dest_summary)
        dest_summary["silent_loss"] = True
        dest_summary["conservation_error"] = note[:500]
        get_mongodb_service().update_job_status(
            job_id,
            "failed",
            error=note,
            phase="failed",
            progress_pct=99,
            message=note,
            reconciliation=recon,
            destination_summary=dest_summary,
            rejected_rows=int(dest_summary.get("rejected_rows", 0) or 0),
            coerced_null_rows=int(dest_summary.get("coerced_null_rows", 0) or 0),
        )
        return TransferResult(
            success=False,
            error=note,
            operation=operation,
            job_id=job_id,
            records_transferred=rows_written,
            destination_summary=dest_summary,
            reconciliation=recon,
        )
    dest_summary["row_accounting"] = ledger.to_dict()
    recon["row_accounting"] = ledger.to_dict()
    return None


def _attach_job_rollback_plan(
    job_id: str, dest_summary: dict[str, Any], request: Any = None
) -> None:
    """Module 6: stamp signed rollback plan onto destination_summary."""
    try:
        from services.migration_rollback import attach_rollback_plan

        dest = getattr(request, "destination", None) if request is not None else None
        attach_rollback_plan(
            dest_summary,
            job_id=job_id,
            sync_mode=str(getattr(request, "sync_mode", "") or "") if request else "",
            destination_table=str(
                dest_summary.get("table")
                or dest_summary.get("collection")
                or getattr(dest, "table", "")
                or getattr(dest, "collection", "")
                or ""
            ),
            dest_type=str(
                getattr(dest, "format", "") or getattr(dest, "kind", "") or ""
            ),
        )
    except Exception as rb_exc:
        logger.warning(
            "rollback plan attach failed (plan stamp only): %s",
            rb_exc,
            exc_info=rb_exc,
        )
