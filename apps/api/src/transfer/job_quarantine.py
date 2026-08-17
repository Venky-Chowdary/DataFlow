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
