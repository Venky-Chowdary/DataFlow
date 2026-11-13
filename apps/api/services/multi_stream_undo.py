"""Undo a failed multi-table run, each table against its own pre-run state (backlog C4).

A sequential multi-table run writes table after table, each committing as it
goes. When a later table (or the job-level verification of the last table)
fails, the batch undo saw one object: the request's table and whatever single
``dest_rows_before`` / ``dest_table_existed_before`` the *last* stream left on
``destination.extra``. So:

* tables the run created or found empty kept every row it wrote (a 3-table
  upsert that failed on ``items`` left ``people`` — created by the run — full);
* the strict-run backup was armed once per run, for the first occupied table
  only, and its restore was reported as "the destination table was restored to
  its pre-run content" while other tables still held this run's rows.

Each stream now pins its own pre-run facts under its own object
(:data:`UNDO_PINS_KEY`, reset each pass: rows before, whether the
object existed, sync mode), every occupied table of a strict keyed run gets its
own backup, and on failure every table the run reached is undone with *its*
facts: restored from its backup, dropped (created by the run), emptied (empty
before), or kept with its own count named (occupied with no backup: deleting by
a partial key list is not a rollback; the append-continuity ledger blocks a
duplicating re-run).
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Any

logger = logging.getLogger(__name__)

#: Summary key: one entry per table the undo judged.
UNDO_KEY = "multi_stream_undo"
#: ``destination.extra`` key: ``{object: {rows_before, existed_before, sync_mode}}``
#: for the tables *this pass* reached. Reset at the start of each pass: a resumed
#: pass must never undo what an earlier pass of the job committed (D134).
UNDO_PINS_KEY = "stream_undo_pins"


def begin_undo_pass(destination: Any) -> None:
    """Start this pass with no undo facts (the request may carry an earlier pass's)."""
    extra = getattr(destination, "extra", None)
    if isinstance(extra, dict):
        extra.pop(UNDO_PINS_KEY, None)


def pin_stream_existence(cfg: Any, source_name: str, rows_before: int | None) -> None:
    """Record whether this stream's object existed before the run (first pin wins).

    Called before the first write of a fresh (not resumed) stream. A table the
    precount could not read is probed for existence: absent means the run
    creates it, which the undo may then drop. ``None`` (unknown) is never pinned
    as absent.
    """
    from src.transfer.adapters import resolve_dest_table

    destination = getattr(cfg, "destination", None)
    extra = getattr(destination, "extra", None)
    if not isinstance(extra, dict):
        return
    table = str(resolve_dest_table(cfg.dest_type, destination, source_name) or "")
    if not table:
        return
    existed: bool | None = True if isinstance(rows_before, int) and rows_before > 0 else None
    if existed is None:
        try:
            from services.dest_precount import destination_table_exists
            from services.dialect_profiles import schema_from_cfg

            existed = destination_table_exists(
                cfg.dest_type, cfg.dest_cfg,
                schema=schema_from_cfg(cfg.dest_type, cfg.dest_cfg), table_name=table,
            )
        except Exception:  # noqa: BLE001 — unknown existence is never "absent"
            existed = None
    per = extra.get(UNDO_PINS_KEY)
    if not isinstance(per, dict):
        per = extra[UNDO_PINS_KEY] = {}
    pin = per.setdefault(table, {"sync_mode": str(getattr(cfg, "effective_sync", "") or "")})
    if "rows_before" not in pin:
        if isinstance(rows_before, int):
            pin["rows_before"] = int(rows_before)
        elif existed is False:
            pin["rows_before"] = 0
    if "existed_before" not in pin and existed is not None:
        pin["existed_before"] = bool(existed)


def remember_stream_restore_point(destination: Any, stream_name: str, summary: Any) -> None:
    """Keep a finished stream's pre-run route watermark on its pin.

    The stream advanced its route watermark as each batch committed; if a later
    table fails and this table's rows are undone, the watermark must go back
    too, or the next run skips the rows it just lost.
    """
    point = (summary or {}).get("watermark_restore_point") if isinstance(summary, dict) else None
    per = (getattr(destination, "extra", None) or {}).get(UNDO_PINS_KEY)
    if not isinstance(point, dict) or not isinstance(per, dict):
        return
    table = str((summary or {}).get("table") or "")
    pin = per.get(table) if table in per else next(
        (p for name, p in per.items() if str(name).lower() == str(stream_name).lower()), None
    )
    if isinstance(pin, dict):
        pin.setdefault("watermark_restore_point", dict(point))


def _table_view(destination: Any, table: str, pin: dict[str, Any]) -> Any:
    """``destination`` addressing ``table``, carrying only that table's pre-run facts."""
    from services.multi_stream_cancel import ROWS_BEFORE_KEY

    extra = {
        k: v
        for k, v in dict(getattr(destination, "extra", None) or {}).items()
        if k not in {
            "dest_rows_before", "dest_table_existed_before", "overwrite_rows_before",
            "cancel_rows_before", "route_backups", "overwrite_backup",
            "overwrite_backup_engine", ROWS_BEFORE_KEY, UNDO_PINS_KEY,
            "stream_overwrite_backups",
        }
    }
    if isinstance(pin.get("rows_before"), int):
        extra["dest_rows_before"] = int(pin["rows_before"])
    if "existed_before" in pin:
        extra["dest_table_existed_before"] = bool(pin["existed_before"])
    fields = {"extra": extra}
    if getattr(destination, "collection", None):
        fields["collection"] = table
    else:
        fields["table"] = table
    return dataclasses.replace(destination, **fields)


def _restore_own_backup(destination: Any, table: str) -> bool | None:
    """Restore ``table`` from the backup armed for it; ``None`` when it has none."""
    extra = getattr(destination, "extra", None)
    backups = dict((extra or {}).get("route_backups") or {})
    key = next((k for k, pair in backups.items() if str((pair or {}).get("table") or k) == table), None)
    if key is None:
        # An overwrite stream that landed parked its backup under its table.
        from src.transfer.engine_finish import settle_parked_overwrite_backups

        return settle_parked_overwrite_backups(destination, restore=True, only_table=table)
    from connectors.table_manager import restore_pg_route_backups
    from src.transfer.adapters import resolve_connector_config

    try:
        restored = restore_pg_route_backups(resolve_connector_config(destination), {key: backups[key]})
        ok = bool(restored)
    except Exception as exc:  # noqa: BLE001 — reported per table, never hides the failure
        logger.error("Multi-table undo: restoring %s from its backup failed: %s", table, exc)
        ok = False
    backups.pop(key, None)
    if isinstance(extra, dict):
        if backups:
            extra["route_backups"] = backups
        else:
            extra.pop("route_backups", None)
    return ok


def undo_multi_stream_run(
    destination: Any,
    *,
    sync_mode: str,
    job_id: str = "",
    skip_tables: tuple[str, ...] = (),
) -> tuple[str, list[dict[str, Any]]]:
    """Undo every table the failed run reached, each with its own pre-run facts.

    ``skip_tables``: tables whose undo already ran with their own facts (the
    failing stream undoes its own batch). Returns the operator note and one
    entry per table. Never raises.
    """
    from services.batch_undo import undo_failed_batch_if_dest_was_empty
    from services.dest_precount import PRECOUNT_KEY

    per = (getattr(destination, "extra", None) or {}).get(UNDO_PINS_KEY)
    if not isinstance(per, dict) or not per:
        return "", []
    entries: list[dict[str, Any]] = []
    for table in reversed(list(per)):
        pin = per[table] if isinstance(per[table], dict) else {}
        restored = _restore_own_backup(destination, table)
        if table in skip_tables and restored is None:
            continue
        entry: dict[str, Any] = {
            "table": table,
            "rows_before": pin.get("rows_before"),
            "existed_before": pin.get("existed_before"),
        }
        if restored is not None:
            entry["decision"] = "restored" if restored else "restore_failed"
            entry["note"] = (
                "restored from its pre-run backup" if restored
                else "its pre-run backup could not be restored; the backup was kept"
            )
            entries.append(entry)
            continue
        mode = str(pin.get("sync_mode") or sync_mode or "")
        summary: dict[str, Any] = {"table": table, "sync_mode": mode, "job_id": job_id}
        if isinstance(pin.get("rows_before"), int):
            summary[PRECOUNT_KEY] = int(pin["rows_before"])
        if "existed_before" in pin:
            summary["dest_table_existed_before"] = bool(pin["existed_before"])
        if isinstance(pin.get("watermark_restore_point"), dict):
            summary["watermark_restore_point"] = dict(pin["watermark_restore_point"])
        try:
            note = undo_failed_batch_if_dest_was_empty(
                destination=_table_view(destination, table, pin),
                dest_summary=summary,
                sync_mode=mode,
                job_id=job_id,
            )
        except Exception as exc:  # noqa: BLE001 — the undo itself never raises; belt and braces
            note = f"the undo could not run ({type(exc).__name__})"
        entry["decision"] = str(summary.get("partial_batch_undo") or "retained")
        entry["note"] = note
        entries.append(entry)
    return _note(entries), entries


def forget_undone_progress(job_id: str, entries: list[dict[str, Any]], *extra_tables: str) -> None:
    """A table whose rows were undone is no longer finished for a resume (C5)."""
    from services.multi_stream_resume import forget_streams

    tables = [str(e.get("table") or "") for e in entries if e.get("decision") != "retained"]
    forget_streams(job_id, [t for t in (*tables, *extra_tables) if t])


_DECISION_WORDS = {
    "restored": "restored from its pre-run backup",
    "restore_failed": "NOT restored (backup kept)",
    "cleared": "emptied (it was empty before)",
}


def _note(entries: list[dict[str, Any]]) -> str:
    if not entries:
        return ""
    parts: list[str] = []
    for e in entries:
        decision = str(e.get("decision") or "")
        if decision == "cleared" and e.get("existed_before") is False:
            what = "dropped (this run created it)"
        elif decision in _DECISION_WORDS:
            what = _DECISION_WORDS[decision]
        else:
            before = e.get("rows_before")
            held = f"held {before:,} row(s) before" if isinstance(before, int) else "pre-run count unknown"
            what = f"kept as is ({held}; see below)"
        parts.append(f"{e['table']}: {what}")
    kept = [str(e.get("note") or "") for e in entries if e.get("decision") not in _DECISION_WORDS]
    out = (
        "Every table this failed run reached was undone against its own pre-run "
        "state — " + "; ".join(parts) + "."
    )
    if kept:
        out += " " + " ".join(k for k in kept if k)
    return out


def note_failed_multi_stream(
    exc: BaseException, destination: Any, *, sync_mode: str, job_id: str, failed_table: str
) -> None:
    """Undo the tables before the failing one and put the note on ``exc`` (never raises)."""
    from services.error_handling import TransferCancelled

    if isinstance(exc, TransferCancelled):
        # A cancel is not a failed batch (D102/D134): the committed tables stay,
        # and a resume skips them (C5).
        return
    try:
        note, entries = undo_multi_stream_run(
            destination, sync_mode=sync_mode, job_id=job_id, skip_tables=(failed_table,)
        )
    except Exception:  # noqa: BLE001 — the run's own failure must surface
        logger.warning("multi-table undo did not run", exc_info=True)
        return
    forget_undone_progress(job_id, entries, failed_table)
    if not note:
        return
    own = str(getattr(exc, "failed_batch_undo_note", "") or "")
    setattr(exc, "failed_batch_undo_note", f"{own} {note}".strip())
    setattr(exc, "multi_stream_undo", entries)
