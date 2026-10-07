"""Catch-up honesty for a CDC job that is about to stop.

An empty poll is not proof the destination has the latest source row.
PostgreSQL keeps that change in the replication slot until it is decoded.
A later COUNT(*) still matches after an UPDATE, so a count gate stays green
while the slot is behind.

A paused or live CDC schedule still resumes from its slot, so that slot is
kept. A failed job is kept too. A one-shot job that completed or was
cancelled drops the slot, because nothing else will read it and an idle
slot retains WAL until the source disk fills.
"""

from __future__ import annotations

import logging
from typing import Any

_logger = logging.getLogger(__name__)

_PG_TYPES = {"postgresql", "postgres"}


class CdcStreamBehind(RuntimeError):
    """Catch-up stopped while the source log still held an unread change."""


def behind_message(cdc: Any) -> str:
    slot = str(getattr(cdc, "slot_name", "") or "").strip()
    lag: int | None = None
    try:
        raw = cdc.replication_lag_bytes()
        if raw is not None:
            lag = int(raw)
    except Exception as exc:
        _logger.debug("CDC lag unread while reporting a behind stream: %s", exc)
        lag = None
    where = f"slot {slot}" if slot else "the source log"
    extra = f" ({lag} bytes behind the source head)" if lag and lag > 0 else ""
    return (
        f"CDC catch-up stopped while {where} still has an unread change{extra}. "
        "A row-count check cannot see an UPDATE that has not been applied. "
        "The replication slot was kept so the change can be resumed. "
        "At-least-once upsert. Not a completed catch-up."
    )


def _summary_field(summary: dict[str, Any], key: str) -> str:
    direct = summary.get(key)
    if direct:
        return str(direct)
    nested = summary.get("cdc")
    if isinstance(nested, dict) and nested.get(key):
        return str(nested.get(key))
    return ""


def unread_postgres_change(source: Any, summary: dict[str, Any] | None) -> str | None:
    """Message when the job's Postgres slot still has a change, else None.

    Called after the reader has closed, immediately before a success status.
    A peek error does not fail the job: the drain already required a proof
    when the reader could see the slot.
    """
    if not isinstance(summary, dict):
        return None
    slot = _summary_field(summary, "cdc_slot_name")
    if not slot:
        return None
    fmt = str(getattr(source, "format", "") or getattr(source, "type", "") or "").lower()
    if fmt not in _PG_TYPES:
        return None
    from src.transfer.adapters import resolve_connector_config

    try:
        cfg = resolve_connector_config(source)
    except Exception as exc:
        _logger.debug("CDC completion peek skipped; source config unread: %s", exc)
        return None
    from connectors.postgresql_change_stream import pg_slot_has_pending_changes

    plugin = _summary_field(summary, "cdc_plugin") or "pgoutput"
    publication = _summary_field(summary, "cdc_publication_name")
    pending = pg_slot_has_pending_changes(
        cfg,
        slot_name=slot,
        publication_name=publication,
        output_plugin=plugin,
    )
    if pending is not True:
        return None
    return (
        f"CDC catch-up stopped while slot {slot} still has an unread change. "
        "A row-count check cannot see an UPDATE that has not been applied. "
        "The replication slot was kept so the change can be resumed. "
        "At-least-once upsert. Not a completed catch-up."
    )


def _schedule_owns_slot(job: dict[str, Any], *, schedule_id: str, job_id: str) -> bool:
    """True when a CDC schedule, including a paused one, still needs this slot."""
    from services.schedule_store import get_schedule, list_schedules
    from services.sync_cursor import normalize_sync_mode

    if schedule_id:
        sched = get_schedule(schedule_id)
        if sched is not None and normalize_sync_mode(sched.sync_mode) == "cdc":
            return True
    src = str(job.get("source_connector_id") or "")
    table = str(job.get("source_name") or "").strip().lower()
    dest = str(job.get("dest_connector_id") or "")
    dest_table = str(
        job.get("destination_collection") or job.get("dest_table") or ""
    ).strip().lower()
    for other in list_schedules():
        if normalize_sync_mode(other.sync_mode) != "cdc":
            continue
        if job_id and str(other.last_job_id or "") == job_id:
            return True
        if not (src and table and dest and dest_table):
            continue
        if (
            other.source_connector_id == src
            and (other.source_table or "").strip().lower() == table
            and other.dest_connector_id == dest
            and (other.dest_table or "").strip().lower() == dest_table
        ):
            return True
    return False


def _slot_name_from_job(job: dict[str, Any]) -> str:
    named = str(job.get("cdc_slot_name") or "").strip()
    if named:
        return named
    token = str(job.get("cursor_value") or "")
    for part in token.split("|"):
        if part.lower().startswith("slot="):
            return part.split("=", 1)[1].strip()[:63]
    return ""


def release_finished_cdc_slot(
    job: dict[str, Any] | None,
    *,
    reason: str,
    schedule_id: str = "",
    source_cfg: dict[str, Any] | None = None,
    job_id: str = "",
) -> dict[str, Any]:
    """Drop the Postgres slot behind a completed or cancelled one-shot CDC job.

    Never raises. A failed release is logged so the operator can drop the
    slot by hand. Does not drop when a CDC schedule still owns the route,
    and does not clear the watermark unless the slot was actually dropped.
    """
    if reason not in {"completed", "cancelled"}:
        return {"released": False, "reason": "not_terminal"}
    job = dict(job or {})
    jid = job_id or str(job.get("id") or job.get("_id") or "")
    try:
        if _schedule_owns_slot(job, schedule_id=schedule_id, job_id=jid):
            return {"released": False, "reason": "schedule_owns_slot", "job_id": jid}
    except Exception as exc:
        _logger.warning("CDC slot release skipped; schedule lookup failed: %s", exc)
        return {"released": False, "reason": "schedule_lookup_failed", "job_id": jid}

    cfg = dict(source_cfg or {})
    if not cfg:
        connector_id = str(job.get("source_connector_id") or "")
        if connector_id:
            try:
                from services.connector_store import get_connector

                connector = get_connector(
                    connector_id, str(job.get("workspace_id") or "") or None
                )
                if connector is not None:
                    cfg = connector.to_dict()
            except Exception as exc:
                _logger.warning(
                    "CDC slot release could not load source connector %s: %s",
                    connector_id,
                    exc,
                )
                return {
                    "released": False,
                    "reason": "source_connector_unreadable",
                    "job_id": jid,
                }
    kind = str(cfg.get("type") or cfg.get("format") or "").strip().lower()
    if kind not in _PG_TYPES:
        return {"released": False, "reason": "not_postgres", "job_id": jid}

    slot_name = _slot_name_from_job(job)
    if not slot_name:
        return {"released": False, "reason": "no_slot_name", "job_id": jid}
    publication = str(job.get("cdc_publication_name") or "")
    from connectors.postgresql_change_stream import release_pg_capture

    try:
        detail = release_pg_capture(
            cfg, slot_name=slot_name, publication_name=publication
        )
    except Exception as exc:
        _logger.warning(
            "Could not drop Postgres slot %s after CDC job %s (%s): %s",
            slot_name,
            jid,
            reason,
            exc,
        )
        return {
            "released": False,
            "reason": "source_unreachable",
            "job_id": jid,
            "slot_name": slot_name,
            "error": str(exc),
        }
    if detail.get("slot") != "dropped":
        return {"released": False, "reason": str(detail.get("slot") or "kept"), "job_id": jid, **detail}

    cursor_key = str(job.get("cursor_key") or "")
    cleared = False
    if cursor_key:
        try:
            from services.sync_cursor import clear_watermark

            cleared = bool(clear_watermark(cursor_key).get("cleared"))
        except Exception as exc:
            _logger.warning(
                "Dropped slot %s but could not clear watermark %s: %s",
                slot_name,
                cursor_key,
                exc,
            )
    return {
        "released": True,
        "reason": reason,
        "job_id": jid,
        "watermark_cleared": cleared,
        **detail,
    }
