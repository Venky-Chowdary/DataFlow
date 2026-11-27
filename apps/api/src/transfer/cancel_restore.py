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

Only backups an overwrite clear creates are restored on a cancel:
``overwrite_backup`` (set by the overwrite clear alone), the parked backups
of a multi-table overwrite, and ``route_backups`` only when the run's sync
mode is known to be an overwrite (strict keyed incremental writes also keep
``route_backups``, D111, and D134 still applies to them).
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

#: Shown on the job while the destination is being put back. The source read
#: transaction ended within the D302 budget already; this can take longer.
RESTORING_MESSAGE = (
    "Cancelling: putting back the rows the destination had before this run…"
)


def effective_sync_mode(request: Any) -> str:
    """The run's sync mode as the stream applied it (stream contract first)."""
    if request is None:
        return ""
    try:
        from services.sync_cursor import resolve_effective_sync_mode, resolve_sync_contract

        contract = resolve_sync_contract(getattr(request, "stream_contracts", None))
        return str(
            resolve_effective_sync_mode(
                getattr(request, "sync_mode", ""), contract.sync_mode if contract else None
            )
            or ""
        )
    except Exception:  # noqa: BLE001 - fall back to the declared mode
        logger.debug("effective sync mode unavailable", exc_info=True)
        return str(getattr(request, "sync_mode", "") or "")


def restorable_on_cancel(destination: Any, sync_mode: str) -> list[dict[str, str]]:
    """The pre-run backups a cancel of this run puts back (``[]``: none).

    Each entry is ``{"table", "backup", "engine"}``. ``sync_mode`` empty means
    unknown: then only the backups an overwrite clear alone creates count.
    """
    from services.sync_cursor import is_overwrite_sync

    from .engine_finish import PARKED_BACKUPS_KEY, RESTORABLE_BACKUP_ENGINES

    overwrite = is_overwrite_sync(sync_mode) if sync_mode else None
    if overwrite is False:
        return []
    extra = dict(getattr(destination, "extra", None) or {})
    held: list[dict[str, str]] = []
    own, engine = extra.get("overwrite_backup"), str(extra.get("overwrite_backup_engine") or "")
    if own and engine in RESTORABLE_BACKUP_ENGINES:
        held.append({"table": _table_of(destination), "backup": str(own), "engine": engine})
    for table, pair in dict(extra.get(PARKED_BACKUPS_KEY) or {}).items():
        pair = dict(pair or {})
        if pair.get("overwrite_backup"):
            held.append({
                "table": str(table),
                "backup": str(pair["overwrite_backup"]),
                "engine": str(pair.get("overwrite_backup_engine") or ""),
            })
    if overwrite is True:
        for table, pair in dict(extra.get("route_backups") or {}).items():
            pair = dict(pair or {})
            held.append({
                "table": str(pair.get("table") or table),
                "backup": str(pair.get("backup") or pair.get("backup_table") or ""),
                "engine": "route",
            })
    return held


def restore_on_cancel(destination: Any, exc: BaseException, held: list[dict[str, str]]) -> bool:
    """Put ``held`` back after a cancel and stamp the outcome on ``exc``.

    The job record then reads, from :func:`stamp_cancel_outcome`: on success
    "Cancelled. The destination holds the rows it had before this run (N
    rows)." with exact counts; on failure where the pre-run rows are and how
    to put them back (the backup is kept, as after a failed run).
    """
    from services.cancel_check import note_on_job

    from .engine_finish import (
        _settle_route_backups,
        settle_parked_overwrite_backups,
        settle_single_overwrite_backup,
    )

    note_on_job({"message": RESTORING_MESSAGE})
    # Each kind on its own: route backups of a strict keyed (resumable) write
    # keep D134's discard; only an overwrite's are put back.
    routes_back = any(entry["engine"] == "route" for entry in held)
    parked = settle_parked_overwrite_backups(destination, restore=True)
    routed = _settle_route_backups(destination, restore=routes_back)
    own = settle_single_overwrite_backup(destination, restore=True)
    restored = parked is not False and own is not False and (
        routed is not False or not routes_back
    )
    counts = {entry["table"]: _count(destination, entry["table"]) for entry in held}
    exc.cancel_restore = {  # type: ignore[attr-defined]
        "restored": restored,
        "tables": [
            {**entry, "rows_now": counts.get(entry["table"])} for entry in held
        ],
    }
    if restored and all(n is not None for n in counts.values()):
        total = sum(int(n) for n in counts.values())
        exc.destination_rows_now = total  # type: ignore[attr-defined]
        exc.destination_rows_by_run = 0  # type: ignore[attr-defined]
        exc.write_rolled_back = True  # type: ignore[attr-defined]
        if getattr(exc, "destination_rows_before", None) is None:
            # The backup is the pre-run object: what it put back is the
            # pre-run count.
            exc.destination_rows_before = total  # type: ignore[attr-defined]
    return restored


def stamp_cancel_outcome(exc: BaseException, outcome: dict[str, Any]) -> dict[str, Any]:
    """Replace the cancel message with what the restore did (no-op without one)."""
    info = getattr(exc, "cancel_restore", None)
    if not isinstance(info, dict):
        return outcome
    tables = list(info.get("tables") or [])
    out = dict(outcome)
    if info.get("restored"):
        out["message"] = restored_message(tables)
        out["pre_run_rows_restored"] = True
    else:
        out["message"] = restore_failed_message(tables)
        out["pre_run_rows_restored"] = False
        out["pre_run_backups"] = [
            {"table": t["table"], "backup": t["backup"]} for t in tables if t.get("backup")
        ]
    return out


def restored_message(tables: list[dict[str, Any]]) -> str:
    if len(tables) == 1:
        n = tables[0].get("rows_now")
        held = f" ({int(n):,} rows)" if n is not None else ""
        return f"Cancelled. The destination holds the rows it had before this run{held}."
    parts = ", ".join(
        f"{t['table']}: {int(t['rows_now']):,} rows" if t.get("rows_now") is not None else str(t["table"])
        for t in tables
    )
    return (
        "Cancelled. Every destination table holds the rows it had before this run "
        f"({parts})."
    )


def restore_failed_message(tables: list[dict[str, Any]]) -> str:
    """Where the pre-run rows are and how to put them back, per table."""
    steps = []
    for t in tables:
        table, backup, engine = t["table"], t.get("backup") or "", t.get("engine") or ""
        if not backup:
            steps.append(f"{table}: its backup could not be located")
        elif engine == "mongodb":
            steps.append(
                f"{table}: the rows are in the collection {backup}; copy its documents "
                f"back into {table} (or rename {backup} to {table}), then drop {backup}"
            )
        else:
            steps.append(
                f"{table}: the rows are in the table {backup} in the same database; "
                f"replace {table}'s rows with them (DELETE FROM {table}; "
                f"INSERT INTO {table} SELECT * FROM {backup}), then drop {backup}"
            )
    return (
        "Cancelled. The destination could not be put back to the rows it had "
        "before this run; the pre-run rows were kept. " + "; ".join(steps) + "."
    )


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
