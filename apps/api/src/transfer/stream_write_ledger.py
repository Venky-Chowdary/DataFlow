"""Per-run write bookkeeping for the streaming engine.

Split out of ``stream.py`` (a god module at its size budget). Everything here
is about what a run *records* around its writes rather than how a row is read
or written:

* how the per-route write summaries of one chunk fold into a single ledger
  (``_merge_chunk_write_summaries``) and how embedding usage accumulates across
  chunks (``_merge_embedding_usage``);
* what the job document's checkpoint carries (``_checkpoint_for_job``) and how a
  run resumed at reconcile inherits the committed pass (``_seed_write_complete_summary``);
* what happens to the destination when a write fails: the undo of a failed
  batch (``_undo_failed_write``) and the row-copy backup armed before a strict
  keyed write (``_arm_strict_keyed_backup``);
* the referential-integrity verdict a page gets before it is committed
  (``_fk_orphans_fail_closed`` / ``_fk_orphan_violation_message``).

``stream.py`` re-exports every name, so ``src.transfer.stream._undo_failed_write``
and friends keep resolving for callers and tests.
"""

from __future__ import annotations

import logging
from typing import Any

from services.dialect_profiles import schema_from_cfg

from .models import EndpointConfig

logger = logging.getLogger(__name__)


def _merge_embedding_usage(previous: Any, incoming: Any) -> dict[str, Any] | None:
    if not isinstance(incoming, dict):
        return None
    if not isinstance(previous, dict) or not previous:
        return dict(incoming)
    return {
        "provider": incoming.get("provider") or previous.get("provider"),
        "model": incoming.get("model") or previous.get("model"),
        "calls": int(previous.get("calls") or 0) + int(incoming.get("calls") or 0),
        "retries": int(previous.get("retries") or 0)
        + int(incoming.get("retries") or 0),
        "input_tokens": int(previous.get("input_tokens") or 0)
        + int(incoming.get("input_tokens") or 0),
        "token_count_estimated": bool(
            previous.get("token_count_estimated")
            or incoming.get("token_count_estimated")
        ),
        "cache_hits": int(previous.get("cache_hits") or 0)
        + int(incoming.get("cache_hits") or 0),
        "cache_misses": int(previous.get("cache_misses") or 0)
        + int(incoming.get("cache_misses") or 0),
        "estimated_cost_usd": (
            None
            if previous.get("estimated_cost_usd") is None
            or incoming.get("estimated_cost_usd") is None
            else float(previous.get("estimated_cost_usd") or 0)
            + float(incoming.get("estimated_cost_usd") or 0)
        ),
    }


def _merge_chunk_write_summaries(
    merged: dict[str, Any], incoming: dict[str, Any]
) -> dict[str, Any]:
    """Merge per-route write summaries into one batch ledger.

    Row counts sum; rejected-details and warnings concatenate (each detail
    already names its route's table); anything else takes the latest write's
    value.
    """
    incoming = dict(incoming or {})
    if not merged:
        return incoming
    for key in ("rejected_rows", "coerced_null_rows", "orphan_fk_rows", "rows_expanded"):
        merged[key] = int(merged.get(key) or 0) + int(incoming.get(key) or 0)
    for key in ("rejected_details", "warnings"):
        merged[key] = list(merged.get(key) or []) + list(incoming.get(key) or [])
    for key, value in incoming.items():
        if key in ("rejected_rows", "coerced_null_rows", "orphan_fk_rows", "rows_expanded", "rejected_details", "warnings"):
            continue
        merged[key] = value
    return merged


def _arm_strict_keyed_backup(
    destination: EndpointConfig,
    *,
    dest_type: str,
    dest_cfg: dict[str, Any],
    dest_table: str,
    job_id: str,
) -> dict[str, Any]:
    """D111 — row-copy a populated PG destination before a strict keyed write.

    A failed verification then restores the exact prior state (DELETE + INSERT
    FROM ``__dfbak_<job>``) instead of leaving the rejected batch in place.
    Armed once per run (a COPY attempt and the row path share it); returns the
    ``route_backups`` stamp on ``destination.extra``. A backup that cannot be
    taken never aborts the write, but a table whose triggers this role cannot
    suspend is refused up front (D122) — that restore could never be exact.
    """
    from connectors.table_manager import PgBackupRestoreRefused, create_pg_table_backup

    extra = dict(getattr(destination, "extra", None) or {})
    backups = dict(extra.get("route_backups") or {})
    if backups:
        return backups
    try:
        pair = create_pg_table_backup(
            dest_cfg,
            schema=schema_from_cfg(dest_type, dest_cfg),
            table=dest_table,
            job_id=job_id,
        )
    except PgBackupRestoreRefused:
        raise
    except Exception as exc:  # noqa: BLE001 - never abort a write for the safety net
        logger.warning("Strict-run destination backup of %s failed: %s", dest_table, exc)
        return backups
    if pair:
        backups[str(pair.get("table") or dest_table)] = pair
        extra["route_backups"] = backups
        destination.extra = extra
    return backups


def _seed_write_complete_summary(summary: dict[str, Any], checkpoint: Any) -> None:
    """Carry the committed write pass into a run resumed at reconcile (D263).

    Every batch was already committed, and its findings are on the DLQ, so this
    pass writes nothing. Reconcile still needs that pass's key-aligned sample and
    its quarantine findings, or it reads the committed load as unproven and the
    held-out rows as missing.
    """
    sample = list(getattr(checkpoint, "reconcile_sample", None) or [])
    if sample:
        summary["reconcile_sample"] = sample
    details = list(getattr(checkpoint, "rejected_details", None) or [])
    if details:
        summary["rejected_details"] = details
        summary["quarantine_dlq_persisted_count"] = len(details)


def _checkpoint_for_job(run: Any, checkpoint: Any) -> dict[str, Any]:
    """The checkpoint the job document sees, with the findings this run holds.

    The stream keeps each batch's findings on the run (already on the DLQ), not
    on the saved checkpoint. Without them the job summary reads "N rejected, no
    finding" as a unit rolled back before any row was judged, which is false
    for rows held out with a reason (D258). The persisted count stops the job
    side from writing those findings to the DLQ a second time.
    """
    out = checkpoint.to_dict()
    profile = getattr(run.cfg, "phase_profile", None)
    if profile is not None:
        out["phase_profile"] = profile.snapshot()
    summary = run.tally.dest_summary if isinstance(run.tally.dest_summary, dict) else {}
    details = summary.get("rejected_details") or []
    if details and not out.get("rejected_details"):
        out["rejected_details"] = list(details)
        out["quarantine_dlq_persisted_count"] = int(
            summary.get("quarantine_dlq_persisted_count") or 0
        )
    return out


def _undo_failed_write(run: Any, exc: BaseException) -> BaseException:
    """Undo a failed run's writes (or drop the table it created); return ``exc``.

    Shared by the batch-error handler and the post-loop "nothing written"
    failures: a run that held out every row has still created its table, and
    leaving it makes the next run bind to a table nobody approved (D258).
    """
    try:
        from services.batch_undo import undo_failed_batch_if_dest_was_empty

        note = undo_failed_batch_if_dest_was_empty(
            destination=run.cfg.destination,
            dest_summary=run.tally.dest_summary,
            sync_mode=run.cfg.effective_sync,
            job_id=str(getattr(run.cfg, "job_id", "") or ""),
        )
        if note:
            setattr(exc, "failed_batch_undo_note", note)
        measured = (run.tally.dest_summary or {}).get("rows_committed_before_failure")
        if isinstance(measured, int):
            # What the destination holds of this run, counted — not a writer tally.
            setattr(exc, "rows_committed_measured", measured)
    except Exception:  # noqa: BLE001 - undo must not hide the write failure
        logging.getLogger(__name__).warning("Failed-write undo did not run", exc_info=True)
    return exc


def _stamp_all_held_out_ledger(
    run: Any, exc: BaseException, *, removed_on_read: int = 0
) -> BaseException:
    """Stamp the counted read on a run that held out every row; return ``exc``.

    The failure path rebuilt the ledger from a result with no read count, so
    the Run page read "source not measured · held out 0" while the DLQ held
    every row (QA1). The rows were read and judged: the read is measured, no
    row was committed, and destination COUNT(*) was not taken.
    """
    from dataclasses import replace

    from services.row_conservation import write_refused_ledger

    held = max(int(run.tally.rejected_total or 0), 0)
    removed = max(int(removed_on_read or 0), 0)
    read = held + removed
    note = (
        f"{read:,} source row(s) were read and {held:,} held out to quarantine "
        "with a named finding, so no row was committed and destination "
        "COUNT(*) was not taken. The read is measured."
    )
    if removed:
        note += f" {removed:,} row(s) were removed on read by the declared recipe or filter."
    ledger = write_refused_ledger(
        rows_read=read,
        quarantined_rows=held,
        sync_mode=str(getattr(run.cfg, "effective_sync", "") or ""),
    )
    setattr(exc, "row_accounting", replace(ledger, note=note).to_dict())
    return exc


def _fk_orphans_fail_closed(validation_mode: str) -> bool:
    return str(validation_mode or "").strip().lower() in {"strict", "maximum"}


def _fk_orphan_violation_message(details: list[dict[str, Any]]) -> str:
    samples = "; ".join(str(d.get("reason") or "") for d in details[:3])
    return (
        f"Referential integrity failed before write: {len(details)} orphan "
        f"child row(s) in this page reference a parent absent at the destination "
        f"({samples}). Strict validation refuses to commit an orphan row — load "
        "the parent rows first or run in balanced mode to quarantine them."
    )
