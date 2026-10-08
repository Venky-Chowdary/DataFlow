"""Catch-up honesty for a CDC job that is about to stop.

An empty poll is not proof the destination has the latest source row.
PostgreSQL keeps that change in the replication slot until it is decoded.
A later COUNT(*) still matches after an UPDATE, so a count gate stays green
while the slot is behind.

A paused or live CDC schedule still resumes from its slot, so that slot is
kept. A retriable failure keeps it too. A one-shot job that completed, was
cancelled, or failed for a reason a resume on the same slot cannot clear
drops the slot, because nothing else will read it and an idle slot retains
WAL until the source disk fills.
"""

from __future__ import annotations

import logging
from typing import Any

_logger = logging.getLogger(__name__)

_PG_TYPES = {"postgresql", "postgres"}


class CdcStreamBehind(RuntimeError):
    """Catch-up stopped while the source log still held an unread change.

    The slot is kept. Resume reads the change. Delivery stays at-least-once.
    """

    code = "cdc_stream_behind"

    def __init__(self, message: str, *, slot_name: str = "") -> None:
        super().__init__(message)
        self.slot_name = slot_name


_CAPTURE_IDENTITY_ATTR = "cdc_capture_identity"


def capture_identity(cdc: Any) -> dict[str, str]:
    """Slot and publication a Postgres log reader names at construction.

    The names are deterministic before the first poll. A writer that fails
    on its first batch never checkpoints, so the job document has no slot
    name and a terminal release would otherwise skip it and leak the slot.
    Query-CDC and non-Postgres readers return an empty identity.
    """
    from connectors.postgresql_change_stream import PostgreSqlChangeStreamCdc

    if not isinstance(cdc, PostgreSqlChangeStreamCdc):
        return {}
    slot = str(getattr(cdc, "slot_name", "") or "").strip()
    if not slot:
        return {}
    out = {"cdc_slot_name": slot}
    publication = str(getattr(cdc, "publication_name", "") or "").strip()
    if publication:
        out["cdc_publication_name"] = publication
    return out


def stamp_capture_identity(exc: BaseException | None, cdc: Any) -> None:
    """Attach the reader's slot identity to an exception leaving the CDC run."""
    if exc is None or cdc is None:
        return
    try:
        identity = capture_identity(cdc)
    except Exception as err:  # noqa: BLE001
        _logger.debug("CDC capture identity unread: %s", err)
        return
    if not identity:
        return
    prior = getattr(exc, _CAPTURE_IDENTITY_ATTR, None)
    if isinstance(prior, dict) and prior.get("cdc_slot_name"):
        return
    try:
        setattr(exc, _CAPTURE_IDENTITY_ATTR, identity)
    except Exception as err:  # noqa: BLE001
        _logger.debug("CDC capture identity not attachable: %s", err)


def capture_identity_from(exc: BaseException | None) -> dict[str, str]:
    """Slot identity stamped by :func:`stamp_capture_identity`, else empty."""
    seen: set[int] = set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        identity = getattr(current, _CAPTURE_IDENTITY_ATTR, None)
        if isinstance(identity, dict) and identity.get("cdc_slot_name"):
            return dict(identity)
        current = current.__cause__ or current.__context__
    return {}


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


def _worker_still_holds_cdc_lease(job_id: str) -> bool:
    """True when this job's worker still has a live CDC lease.

    Peek mode leaves the Postgres slot inactive between polls. Dropping it
    then cuts the worker off and clears the watermark under an open reader.
    A stale lease is a worker that has already exited. A lease-store error
    keeps the slot: the worker path drops it after ``close()``.
    """
    if not job_id:
        return False
    try:
        from services.cdc_lease import list_lease_views

        return any(
            not view.get("stale") for view in list_lease_views(job_id=job_id)
        )
    except Exception as exc:  # noqa: BLE001
        _logger.warning(
            "CDC slot release skipped; lease lookup failed for %s: %s",
            job_id,
            exc,
        )
        return True


def _cursor_keys_to_clear(job: dict[str, Any], job_id: str) -> list[str]:
    """Route keys whose watermark must go when this job's slot is dropped.

    The job document is the first source. A run that advanced the cursor
    store without stamping ``cursor_key`` is found by ``metadata.job_id``.
    Leaving that watermark in place makes the next one-shot run see a
    missing slot and refuse to snapshot.
    """
    keys: list[str] = []

    def _add(value: Any) -> None:
        text = str(value or "").strip()
        if text and text not in keys:
            keys.append(text)

    _add(job.get("cursor_key"))
    _add(job.get("cdc_lease_cursor_key"))
    nested = job.get("cdc")
    if isinstance(nested, dict):
        _add(nested.get("cursor_key"))
    if job_id:
        try:
            from services.sync_cursor import cursor_keys_for_job

            for key in cursor_keys_for_job(job_id):
                _add(key)
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "CDC watermark lookup by job %s failed: %s", job_id, exc
            )
    return keys


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
    worker_closed: bool = False,
    retriable: bool = False,
    dest_type: str = "",
    dest_cfg: dict[str, Any] | None = None,
    extra_lsns: list[Any] | None = None,
) -> dict[str, Any]:
    """Drop the Postgres slot behind a finished one-shot CDC job.

    Completed and cancelled one-shots drop the slot. A failed one-shot
    drops it only when the failure is not retriable and the worker has
    closed the replication connection — a 1292 or a fence refusal will
    not resume on the same slot, and leaving it holds WAL. A retriable
    failure (the slot still has the change, or the lease store blipped)
    keeps the slot. A CDC schedule, including a paused one, keeps it.

    Never raises. A failed release is logged so the operator can drop the
    slot by hand. Does not clear the watermark unless the slot was
    actually dropped. A dropped slot also retires the destination
    exactly-once LSN for those cursor keys. The writer fence is not
    changed. The next run snapshots current keys instead of streaming
    the dead LSN. Historical slots are not enumerated here.

    ``worker_closed`` is set by the worker after ``close()`` has released
    the replication connection. A cancel request that arrives while the
    worker still holds the lease leaves the slot; peek mode makes that
    slot look idle between polls.
    """
    if reason not in {"completed", "cancelled", "failed"}:
        return {"released": False, "reason": "not_terminal"}
    job = dict(job or {})
    jid = job_id or str(job.get("id") or job.get("_id") or "")
    if reason == "failed":
        if retriable:
            return {
                "released": False,
                "reason": "retriable_failure",
                "job_id": jid,
            }
        if not worker_closed:
            return {
                "released": False,
                "reason": "worker_not_closed",
                "job_id": jid,
            }
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
    if (
        reason == "cancelled"
        and not worker_closed
        and _worker_still_holds_cdc_lease(jid)
    ):
        return {
            "released": False,
            "reason": "worker_still_holds_lease",
            "job_id": jid,
            "slot_name": slot_name,
        }
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

    keys = _cursor_keys_to_clear(job, jid)
    cleared_keys: list[str] = []
    prior_by_key: dict[str, Any] = {}
    if not keys:
        _logger.warning(
            "Dropped slot %s for job %s but no cursor key was found; "
            "the next run can hit a slot gap until the watermark is cleared",
            slot_name,
            jid,
        )
    else:
        from services.sync_cursor import clear_watermark

        for cursor_key in keys:
            try:
                cleared = clear_watermark(cursor_key)
            except Exception as exc:  # noqa: BLE001
                _logger.warning(
                    "Dropped slot %s but could not clear watermark %s: %s",
                    slot_name,
                    cursor_key,
                    exc,
                )
                continue
            if cleared.get("cleared"):
                cleared_keys.append(cursor_key)
            if cleared.get("prior_watermark") is not None:
                prior_by_key[cursor_key] = cleared.get("prior_watermark")
    extra = list(extra_lsns or [])
    for field in ("cursor_value", "eos_committed_lsn", "cdc_confirmed_flush_lsn"):
        if job.get(field):
            extra.append(job.get(field))
    try:
        from services.cdc_slot_resume import retire_after_slot_drop

        retired = retire_after_slot_drop(
            slot_name=slot_name,
            cursor_keys=keys,
            prior_by_key=prior_by_key,
            extra_tokens=extra,
            dest_type=dest_type,
            dest_cfg=dest_cfg,
        )
    except Exception as exc:  # noqa: BLE001
        _logger.warning(
            "Dropped slot %s but could not retire its resume LSN: %s",
            slot_name,
            exc,
        )
        retired = {"retired_lsns": [], "dest_resume_cleared": []}
    return {
        "released": True,
        "reason": reason,
        "job_id": jid,
        "watermark_cleared": bool(cleared_keys),
        "cursor_keys": cleared_keys,
        **retired,
        **detail,
    }
