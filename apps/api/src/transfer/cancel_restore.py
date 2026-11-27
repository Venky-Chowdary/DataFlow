"""A cancelled full-refresh overwrite puts the pre-run rows back.

The set-aside overwrite (MySQL ``<table>__df_bak`` row copy, MongoDB rename,
the generic-SQL row copy and the warehouse set-aside of
:mod:`connectors.overwrite_set_aside`) empties the live object before the
route writes. A failed run restores that backup; a cancel used to drop it
(D134), and the job said only "Transfer cancelled by user" while the live
table was empty.

D134 excluded cancels for a reason that holds for resumable writes only: a
cancelled incremental / keyed run keeps its committed prefix, its checkpoint
and route watermark already point past it, and restoring the pre-run backup
removed rows the resume then skipped as landed. An overwrite never resumes
from a checkpoint (``resume_state.restarts_from_start``: clear-and-reload
restarts from the first row), so putting its pre-run rows back is safe, and
is what the owner decided: a cancel must not leave the destination empty
while Datawrap still holds the rows it had.

The decision is per table, from that table's own stream:

* the stream's backup is restored only when that stream is an overwrite
  (``overwrite_backup`` is made by the overwrite clear alone; a route backup
  carries the ``sync_mode`` of the stream that armed it, and one without it
  is never restored), and
* only while that table's write is incomplete: once every source row of the
  table is committed (:func:`mark_write_complete`) a cancel is "after the
  write" and keeps the rows, as the COPY route does;
* a multi-table run's finished streams (parked backups) keep what they wrote.

The record says per table what happened: the tables put back, with their
counts, and the tables that keep the rows this run committed.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

#: Shown on the job while the destination is being put back. The source read
#: transaction ended within the D302 budget already; this can take longer.
RESTORING_MESSAGE = (
    "Cancelling: putting back the rows the destination had before this run…"
)

#: ``destination.extra``: tables whose every source row is committed.
WRITE_COMPLETE_KEY = "overwrite_write_complete_tables"


#: Shown at once when the operator cancels after the load already landed:
#: the run still finishes verifying what it wrote (can take minutes).
LATE_CANCEL_MESSAGE = (
    "Cancel received after the load had finished: the rows are committed; "
    "finishing verification of what was written…"
)

#: Job ids whose late-cancel note is being watched (one watcher per run).
_LATE_WATCHED: set[str] = set()
_LATE_LOCK = threading.Lock()


def _watch_for_late_cancel(interval: float = 2.0, budget: float = 3600.0) -> None:
    """Say on the job, as soon as it happens, that a cancel came after the write.

    Post-write verification does not stop for a cancel (the rows are
    committed; the run reports them exactly when it ends), so the job showed
    nothing for a minute or more after the operator pressed Cancel.
    """
    from services.cancel_check import active_job_store

    bound = active_job_store()
    if bound is None:
        return
    mongo, job_id = bound
    with _LATE_LOCK:
        if job_id in _LATE_WATCHED:
            return
        _LATE_WATCHED.add(job_id)

    def run() -> None:
        try:
            _poll()
        finally:
            with _LATE_LOCK:
                _LATE_WATCHED.discard(job_id)

    def _poll() -> None:
        from services.job_status import TERMINAL_STATUSES

        deadline = time.monotonic() + budget
        while time.monotonic() < deadline:
            try:
                job = mongo.get_job(job_id)
            except Exception:  # noqa: BLE001 - a missed read is retried
                job = {}
            if job is None:
                return  # the job record is gone
            status = str(job.get("status") or "")
            if job.get("cancel_outcome"):
                return  # the run already wrote its final cancelled record
            cancelled = bool(job.get("cancel_requested")) or status == "cancelled"
            if status in TERMINAL_STATUSES and status != "cancelled":
                return  # completed (with quarantine / warnings) or failed: done
            if cancelled:
                try:
                    # Compare-and-swap: never over the run's final record (it
                    # sets cancel_outcome), never over a status that moved on.
                    mongo.update_job_fields(
                        job_id,
                        {"message": LATE_CANCEL_MESSAGE, "cancel_note": LATE_CANCEL_MESSAGE},
                        expected={"cancel_outcome": None, "status": status or None},
                    )
                except Exception:  # noqa: BLE001 - a note never fails the run
                    logger.warning("late-cancel note not written for %s", job_id, exc_info=True)
                return
            time.sleep(interval)

    threading.Thread(target=run, name="df-late-cancel-note", daemon=True).start()


def mark_write_complete(destination: Any, table: str = "") -> None:
    """Record that ``table``'s load is fully committed (a cancel keeps it)."""
    if destination is None:
        return
    _watch_for_late_cancel()
    name = str(table or "") or _table_of(destination)
    extra = dict(getattr(destination, "extra", None) or {})
    done = list(extra.get(WRITE_COMPLETE_KEY) or [])
    if name not in done:
        done.append(name)
    extra[WRITE_COMPLETE_KEY] = done
    try:
        destination.extra = extra
    except Exception:  # noqa: BLE001 - a frozen endpoint simply is not marked
        logger.debug("write-complete mark skipped", exc_info=True)


def _stream_modes(destination: Any, request: Any) -> dict[str, str]:
    """``{table: sync_mode}`` for every stream of the run that pinned one."""
    from services.multi_stream_cancel import ROWS_BEFORE_KEY

    extra = dict(getattr(destination, "extra", None) or {})
    modes: dict[str, str] = {}
    for table, pinned in dict(extra.get(ROWS_BEFORE_KEY) or {}).items():
        if isinstance(pinned, dict) and pinned.get("sync_mode"):
            modes[str(table)] = str(pinned["sync_mode"])
    return modes


def _single_stream_mode(request: Any) -> str:
    """The mode of a one-stream run ('' for none, unknown or several streams)."""
    if request is None:
        return ""
    try:
        from services.sync_cursor import resolve_effective_sync_mode, resolve_selected_sync_contracts

        contracts = resolve_selected_sync_contracts(getattr(request, "stream_contracts", None))
        if len(contracts) > 1:
            return ""
        return str(
            resolve_effective_sync_mode(
                getattr(request, "sync_mode", ""), contracts[0].sync_mode if contracts else None
            )
            or ""
        )
    except Exception:  # noqa: BLE001 - unknown: only overwrite-clear backups count
        logger.debug("sync mode unavailable", exc_info=True)
        return ""


def restorable_on_cancel(destination: Any, request: Any = None) -> list[dict[str, str]]:
    """The pre-run backups a cancel of this run puts back (``[]``: none).

    Each entry is ``{"table", "backup", "engine", "kind"}`` with ``kind`` one
    of ``own`` (the overwrite clear's backup of the stream the cancel stopped)
    or ``route`` (a route backup armed by an overwrite stream).
    """
    from services.sync_cursor import is_overwrite_sync

    from .engine_finish import RESTORABLE_BACKUP_ENGINES

    extra = dict(getattr(destination, "extra", None) or {})
    complete = {str(t) for t in (extra.get(WRITE_COMPLETE_KEY) or [])}
    modes = _stream_modes(destination, request)
    single = _single_stream_mode(request)

    def overwrite_table(table: str, stamped: str = "") -> bool:
        mode = stamped or modes.get(table) or single
        return bool(mode) and is_overwrite_sync(mode) and table not in complete

    held: list[dict[str, str]] = []
    own, engine = extra.get("overwrite_backup"), str(extra.get("overwrite_backup_engine") or "")
    if own and engine in RESTORABLE_BACKUP_ENGINES:
        table = _table_of(destination)
        # Only the overwrite clear makes this backup: the stream is an
        # overwrite whatever the other streams declare.
        if table not in complete:
            held.append({"table": table, "backup": str(own), "engine": engine, "kind": "own"})
    for key, pair in dict(extra.get("route_backups") or {}).items():
        pair = dict(pair or {})
        table = str(pair.get("table") or key)
        if pair.get("backup") and overwrite_table(table, str(pair.get("sync_mode") or "")) and pair.get("sync_mode"):
            held.append({
                "table": table, "backup": str(pair["backup"]), "engine": "route", "kind": "route",
                "route_key": str(key),
            })
    return held


def restore_on_cancel(
    destination: Any, exc: BaseException, held: list[dict[str, str]], request: Any = None
) -> bool:
    """Put ``held`` back after a cancel, discard every other backup, stamp ``exc``."""
    from services.cancel_check import note_on_job

    from .adapters import resolve_connector_config
    from .engine_finish import _settle_overwrite_backup, settle_single_overwrite_backup

    note_on_job({"message": RESTORING_MESSAGE})
    results: dict[str, bool] = {}
    if any(e["kind"] == "own" for e in held):
        results["own"] = settle_single_overwrite_backup(destination, restore=True) is not False
    routes = [e for e in held if e["kind"] == "route"]
    if routes:
        from connectors.table_manager import restore_route_backups

        extra = dict(getattr(destination, "extra", None) or {})
        all_routes = dict(extra.get("route_backups") or {})
        chosen = {e["route_key"]: all_routes[e["route_key"]] for e in routes if e["route_key"] in all_routes}
        try:
            restored = restore_route_backups(resolve_connector_config(destination), chosen)
            results["route"] = len(restored) == len(chosen)
        except Exception:  # noqa: BLE001 - reported on the job with the backups kept
            logger.error("route backup restore after cancel failed", exc_info=True)
            results["route"] = False
        for key in chosen:
            all_routes.pop(key, None)
        extra["route_backups"] = all_routes
        destination.extra = extra
    # What is left (finished streams, resumable writes, complete tables)
    # keeps this run's rows: D134's discard.
    _settle_overwrite_backup(destination, restore=False)
    ok = all(results.values())
    tables = []
    for entry in held:
        tables.append({
            **entry,
            "restored": bool(results.get(entry["kind"])),
            "rows_now": _count(destination, entry["table"]),
            "steps": "" if results.get(entry["kind"]) else _steps(destination, entry),
        })
    exc.cancel_restore = {"restored": ok, "tables": tables}  # type: ignore[attr-defined]
    stopped = _table_of(destination)
    mine = next((t for t in tables if t["table"] == stopped and t["restored"]), None)
    if mine is not None and mine["rows_now"] is not None:
        # The stream the cancel stopped is back to its pre-run rows: nothing
        # of this run is in it. Other streams are measured on their own.
        exc.destination_rows_now = int(mine["rows_now"])  # type: ignore[attr-defined]
        exc.destination_rows_by_run = 0  # type: ignore[attr-defined]
        exc.write_rolled_back = True  # type: ignore[attr-defined]
        if getattr(exc, "destination_rows_before", None) is None:
            exc.destination_rows_before = int(mine["rows_now"])  # type: ignore[attr-defined]
    return ok


def stamp_cancel_outcome(exc: BaseException, outcome: dict[str, Any]) -> dict[str, Any]:
    """Replace the cancel message with what the restore did (no-op without one).

    A backup restore or drop that failed while the cancel settled (carried on
    ``exc.overwrite_restore``) is added to the message: the job names it.
    """
    out = _stamp_cancel_restore(exc, outcome)
    failed = getattr(exc, "overwrite_restore", None)
    message = str((failed or {}).get("message") or "").strip() if isinstance(failed, dict) else ""
    if message and message not in str(out.get("message") or ""):
        out = {**out, "message": f"{out.get('message') or ''} {message}".strip()}
    return out


def _stamp_cancel_restore(exc: BaseException, outcome: dict[str, Any]) -> dict[str, Any]:
    info = getattr(exc, "cancel_restore", None)
    if not isinstance(info, dict):
        return outcome
    tables = list(info.get("tables") or [])
    out = dict(outcome)
    restored = [t for t in tables if t.get("restored")]
    failed = [t for t in tables if not t.get("restored")]
    back = {t["table"] for t in tables}
    kept = [
        s for s in (outcome.get("streams") or [])
        if s.get("table") not in back and s.get("rows_by_run")
    ]
    parts: list[str] = []
    if restored:
        parts.append(_restored_sentence(restored, several=bool(kept or failed)))
    if kept:
        parts.append(
            "This run's committed rows were kept in "
            + ", ".join(f"{s['table']} ({int(s['rows_by_run']):,} rows)" for s in kept)
            + " (those streams keep what they wrote; Resume continues after it)."
        )
    if failed:
        parts.append(restore_failed_message(failed))
    if not parts:
        return outcome
    out["message"] = "Cancelled. " + " ".join(parts)
    out["pre_run_rows_restored"] = bool(restored) and not failed
    out["restored_tables"] = [
        {"table": t["table"], "rows_now": t.get("rows_now")} for t in restored
    ]
    if failed:
        out["pre_run_backups"] = [
            {"table": t["table"], "backup": t["backup"]} for t in failed if t.get("backup")
        ]
    return out


def _restored_sentence(tables: list[dict[str, Any]], *, several: bool) -> str:
    if len(tables) == 1 and not several:
        n = tables[0].get("rows_now")
        held = f" ({int(n):,} rows)" if n is not None else ""
        return f"The destination holds the rows it had before this run{held}."
    return "Put back to the rows they had before this run: " + ", ".join(
        f"{t['table']} ({int(t['rows_now']):,} rows)" if t.get("rows_now") is not None else str(t["table"])
        for t in tables
    ) + "."


def restored_message(tables: list[dict[str, Any]]) -> str:
    return "Cancelled. " + _restored_sentence(tables, several=False)


def restore_failed_message(tables: list[dict[str, Any]]) -> str:
    """Where the pre-run rows are and the exact statements that put them back."""
    lines = []
    for t in tables:
        backup = t.get("backup") or ""
        if not backup:
            lines.append(f"{t['table']}: its backup could not be located.")
            continue
        steps = t.get("steps") or f"copy the rows of {backup} back into {t['table']}"
        lines.append(f"{t['table']}: the pre-run rows are kept in {backup}. To put them back: {steps}.")
    return (
        "The destination could not be put back automatically to the rows it had "
        "before this run, so the pre-run rows were kept. " + " ".join(lines)
    )


def _steps(destination: Any, entry: dict[str, str]) -> str:
    try:
        from connectors.overwrite_restore_steps import manual_restore_steps

        from .adapters import resolve_connector_config
        from .connector_capabilities import resolve_driver_type

        cfg = resolve_connector_config(destination)
        return manual_restore_steps(
            engine=entry.get("engine") or "",
            dest_type=resolve_driver_type(destination.format),
            cfg=cfg, table=entry["table"], backup=entry["backup"],
            schema=cfg.get("schema"),
        )
    except Exception:  # noqa: BLE001 - the message still names the backup
        logger.warning("restore steps unavailable for %s", entry.get("table"), exc_info=True)
        return ""


def _table_of(destination: Any) -> str:
    try:
        from .adapters import resolve_dest_table
        from .connector_capabilities import resolve_driver_type

        return str(resolve_dest_table(resolve_driver_type(destination.format), destination) or "")
    except Exception:  # noqa: BLE001 - the name only labels the message
        return str(getattr(destination, "table", "") or getattr(destination, "collection", "") or "")


def _count(destination: Any, table: str) -> int | None:
    try:
        from services.dest_precount import precount_table

        from .adapters import resolve_connector_config
        from .connector_capabilities import resolve_driver_type

        n = precount_table(
            resolve_driver_type(destination.format), resolve_connector_config(destination), table
        )
        return None if n is None else int(n)
    except Exception:  # noqa: BLE001 - an uncounted table is said to be uncounted
        logger.warning("could not count %s after the cancel restore", table, exc_info=True)
        return None
