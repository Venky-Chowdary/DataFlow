"""Release a schedule's log-capture resources when the schedule goes away.

A PostgreSQL CDC schedule owns a replication slot on the *source*. Deleting
the schedule without dropping the slot retains WAL forever and eventually
exhausts ``max_replication_slots`` — an outage on the client's database, not
ours. Fivetran/Estuary drop the slot on connector deletion; Airbyte leaves it
to the operator. We drop it, with two guards:

* another CDC schedule on the same route (same source connector + table set →
  same destination connector + table) still needs the slot → keep it;
* a slot with an attached consumer (``active``) is never dropped.

The slot/publication identity is read from the last run's job document
(``cursor_key`` + ``cursor_value``) — the same values the reader derived them
from — so this module never re-implements slot naming.
"""

from __future__ import annotations

import logging
from typing import Any

from services.schedule_store import PipelineSchedule, list_schedules
from services.sync_cursor import clear_watermark, normalize_sync_mode

_logger = logging.getLogger(__name__)

_PG_TYPES = {"postgresql", "postgres"}


def _route_of(s: PipelineSchedule) -> tuple[str, str, str, str]:
    return (
        s.source_connector_id,
        (s.source_table or "").strip().lower(),
        s.dest_connector_id,
        (s.dest_table or "").strip().lower(),
    )


def route_still_in_use(sched: PipelineSchedule) -> bool:
    """True when another CDC schedule shares this schedule's capture route."""
    route = _route_of(sched)
    for other in list_schedules():
        if other.id == sched.id:
            continue
        if normalize_sync_mode(other.sync_mode) != "cdc":
            continue
        if (other.workspace_id or "") != (sched.workspace_id or ""):
            continue
        if _route_of(other) == route:
            return True
    return False


def _capture_tables(sched: PipelineSchedule) -> str | list[str]:
    names = [
        str(c.get("name") or "").strip()
        for c in sched.stream_contracts
        if isinstance(c, dict) and c.get("selected", True) and c.get("name")
    ]
    if len(names) > 1:
        return names
    return sched.source_table


def release_schedule_cdc_capture(sched: PipelineSchedule) -> dict[str, Any]:
    """Drop the slot/publication + watermark behind ``sched`` if nothing else uses them.

    Returns ``{"released": bool, "reason": str, ...detail}``. Never raises:
    a failed release must not block the schedule delete; it is logged and
    reported so the operator can drop the slot by hand.
    """
    if normalize_sync_mode(sched.sync_mode) != "cdc":
        return {"released": False, "reason": "not_cdc"}
    if not sched.last_job_id:
        return {"released": False, "reason": "never_ran"}
    if route_still_in_use(sched):
        return {"released": False, "reason": "route_shared"}

    from services.connector_store import get_connector
    from services.mongodb_service import get_mongodb_service

    job = get_mongodb_service().get_job(sched.last_job_id) or {}
    cursor_key = str(job.get("cursor_key") or "")
    token = str(job.get("cursor_value") or "")
    if not cursor_key:
        return {"released": False, "reason": "no_cursor_key"}

    connector = get_connector(sched.source_connector_id, sched.workspace_id or None)
    if connector is None:
        clear_watermark(cursor_key)
        return {"released": False, "reason": "source_connector_missing", "cursor_key": cursor_key}
    if (connector.type or "").lower() not in _PG_TYPES:
        # MySQL/SQL Server/Oracle capture holds no server-side slot; only the
        # watermark is ours to clear.
        cleared = clear_watermark(cursor_key)
        return {
            "released": True,
            "reason": "watermark_only",
            "cursor_key": cursor_key,
            "watermark_cleared": bool(cleared.get("cleared")),
        }

    from connectors.postgresql_change_stream import (
        _publication_name,
        decode_pg_resume_token,
        release_pg_capture,
    )

    cfg = connector.to_dict()
    database = str(cfg.get("database") or "postgres")
    tables = _capture_tables(sched)
    slot_name, _lsn, _phase = decode_pg_resume_token(
        token, database=database, table=tables, cursor_key=cursor_key
    )
    publication = _publication_name(database, tables, cursor_key)
    try:
        detail = release_pg_capture(cfg, slot_name=slot_name, publication_name=publication)
    except Exception as exc:
        _logger.warning(
            "Could not release PostgreSQL capture for schedule %s (slot %s): %s",
            sched.id,
            slot_name,
            exc,
        )
        return {
            "released": False,
            "reason": "source_unreachable",
            "cursor_key": cursor_key,
            "slot_name": slot_name,
            "publication_name": publication,
            "error": str(exc),
            "next_action": f"SELECT pg_drop_replication_slot('{slot_name}') on {database}",
        }
    if detail.get("slot") == "active":
        return {"released": False, "reason": "slot_active", "cursor_key": cursor_key, **detail}
    cleared = clear_watermark(cursor_key)
    return {
        "released": True,
        "reason": "ok",
        "cursor_key": cursor_key,
        "watermark_cleared": bool(cleared.get("cleared")),
        **detail,
    }
