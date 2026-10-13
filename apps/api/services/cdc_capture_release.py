"""Release a schedule's log-capture resources when the schedule goes away.

A PostgreSQL CDC schedule owns a replication slot on the *source*. Deleting
the schedule without dropping the slot retains WAL forever and eventually
exhausts ``max_replication_slots`` — an outage on the client's database, not
ours. Fivetran/Estuary drop the slot on connector deletion; Airbyte leaves it
to the operator. We drop it, with two guards:

* another CDC schedule on the same route (same source connector + table set →
  same destination connector + table) still needs the slot → keep it;
* a slot with an attached consumer (``active``) is never dropped.

Slot identity comes from every place a run left it, never from one document
(QA S14: the last job had no ``cursor_key`` → ``no_cursor_key`` → slot leaked):

1. ``cursor_key`` / ``cursor_value`` on the last, running, and historical jobs;
2. watermark-store entries whose metadata names one of those jobs;
3. the route identity re-derived by the CDC transfer's own owner
   (:func:`src.transfer.cdc_transfer.cdc_route_cursor_keys`).

Slot naming itself stays in ``postgresql_change_stream``. A release that cannot
finish (source down, consumer attached, identity unknown) is written to a
durable ledger and retried by the scheduler beat, so a delete never forgets the
capture state it left on a client database.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
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


def _schedule_job_ids(sched: PipelineSchedule) -> list[str]:
    ids: list[str] = []
    for raw in (
        [sched.last_job_id, sched.running_job_id]
        + [e.get("job_id") for e in reversed(sched.run_history or []) if isinstance(e, dict)]
    ):
        jid = str(raw or "").strip()
        if jid and jid not in ids:
            ids.append(jid)
    return ids


def _derived_route_identities(sched: PipelineSchedule) -> list[dict[str, Any]]:
    """Route cursor keys re-derived by the CDC transfer's own owner."""
    try:
        from services.schedule_runner import _resolve_connector, build_schedule_request
        from src.transfer.cdc_transfer import cdc_route_cursor_keys

        src = _resolve_connector(sched.source_connector_id)
        dst = _resolve_connector(sched.dest_connector_id)
        if not src or not dst:
            return []
        request = build_schedule_request(sched, src, dst)
        return cdc_route_cursor_keys(
            request.source, request.destination, list(request.stream_contracts or [])
        )
    except Exception as exc:  # noqa: BLE001 — derivation is one of three sources
        _logger.info("CDC route identity not derivable for %s: %s", sched.id, exc)
        return []


def capture_identities(sched: PipelineSchedule) -> list[dict[str, Any]]:
    """Every ``{cursor_key, token, tables, found_via}`` this schedule may own."""
    from services.mongodb_service import get_mongodb_service
    from services.sync_cursor import cursor_keys_for_job, get_watermark

    found: dict[str, dict[str, Any]] = {}
    default_tables = _capture_tables(sched)

    def _add(key: str, *, token: str = "", tables: Any = None, via: str) -> None:
        key = str(key or "").strip()
        if not key:
            return
        entry = found.setdefault(
            key, {"cursor_key": key, "token": "", "tables": tables or default_tables, "found_via": via}
        )
        if token and not entry["token"]:
            entry["token"] = token
        if tables and entry["tables"] == default_tables:
            entry["tables"] = tables

    job_ids = _schedule_job_ids(sched)
    try:
        svc = get_mongodb_service()
        for jid in job_ids:
            job = svc.get_job(jid) or {}
            _add(job.get("cursor_key"), token=str(job.get("cursor_value") or ""), via=f"job:{jid}")
    except Exception as exc:  # noqa: BLE001
        _logger.info("job lookup for capture release of %s failed: %s", sched.id, exc)
    for jid in job_ids:
        try:
            for key in cursor_keys_for_job(jid):
                _add(key, via=f"watermark_meta:{jid}")
        except Exception as exc:  # noqa: BLE001
            _logger.debug("cursor_keys_for_job(%s) failed: %s", jid, exc)
    for ident in _derived_route_identities(sched):
        _add(ident.get("cursor_key"), tables=ident.get("tables"), via="route_derived")
    for entry in found.values():
        if not entry["token"]:
            try:
                entry["token"] = str(get_watermark(entry["cursor_key"]) or "")
            except Exception:  # noqa: BLE001
                entry["token"] = ""
    return list(found.values())


def release_schedule_cdc_capture(sched: PipelineSchedule) -> dict[str, Any]:
    """Drop the slot/publication + watermark behind ``sched`` if nothing else uses them.

    Returns ``{"released": bool, "reason": str, ...detail}``. Never raises:
    a failed release must not block the schedule delete. Anything left on the
    source is recorded in the durable pending-release ledger (``pending_id``)
    and retried by the beat; ``next_action`` names the manual SQL.
    """
    if normalize_sync_mode(sched.sync_mode) != "cdc":
        return {"released": False, "reason": "not_cdc"}
    if not _schedule_job_ids(sched):
        return {"released": False, "reason": "never_ran"}
    if route_still_in_use(sched):
        return {"released": False, "reason": "route_shared"}

    from services.connector_store import get_connector

    identities = capture_identities(sched)
    connector = get_connector(sched.source_connector_id, sched.workspace_id or None)
    if connector is None:
        for ident in identities:
            clear_watermark(ident["cursor_key"])
        return {
            "released": False,
            "reason": "source_connector_missing",
            "cursor_key": identities[0]["cursor_key"] if identities else "",
            "cursor_keys": [i["cursor_key"] for i in identities],
            "next_action": (
                "The source connector was deleted, so its capture could not be "
                "inspected. Check the source for replication slots named df_* "
                "and drop inactive ones."
            ),
        }
    if not identities:
        out = {
            "released": False,
            "reason": "identity_unknown",
            "next_action": (
                "No run recorded a capture identity and the route could not be "
                "re-derived. Check the source for replication slots named df_*."
            ),
        }
        return _record_pending(sched, out, slots=[])
    if (connector.type or "").lower() not in _PG_TYPES:
        # MySQL/SQL Server/Oracle capture holds no server-side slot; only the
        # watermark is ours to clear.
        cleared = [bool(clear_watermark(i["cursor_key"]).get("cleared")) for i in identities]
        return {
            "released": True,
            "reason": "watermark_only",
            "cursor_key": identities[0]["cursor_key"],
            "cursor_keys": [i["cursor_key"] for i in identities],
            "watermark_cleared": any(cleared),
        }
    return _release_pg(sched, connector.to_dict(), identities)


def _pg_slots(cfg: dict[str, Any], identities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from connectors.postgresql_change_stream import _publication_name, decode_pg_resume_token

    database = str(cfg.get("database") or "postgres")
    slots: dict[str, dict[str, Any]] = {}
    for ident in identities:
        tables = ident.get("tables")
        slot_name, _lsn, _phase = decode_pg_resume_token(
            ident.get("token") or "",
            database=database,
            table=tables,
            cursor_key=ident["cursor_key"],
        )
        entry = slots.setdefault(
            slot_name,
            {
                "slot_name": slot_name,
                "publication_name": _publication_name(database, tables, ident["cursor_key"]),
                "cursor_keys": [],
            },
        )
        entry["cursor_keys"].append(ident["cursor_key"])
    return list(slots.values())


def _release_pg(
    sched: PipelineSchedule, cfg: dict[str, Any], identities: list[dict[str, Any]]
) -> dict[str, Any]:
    import connectors.postgresql_change_stream as pcs

    database = str(cfg.get("database") or "postgres")
    slots = _pg_slots(cfg, identities)
    results: list[dict[str, Any]] = []
    leftover: list[dict[str, Any]] = []
    for slot in slots:
        try:
            detail = pcs.release_pg_capture(
                cfg, slot_name=slot["slot_name"], publication_name=slot["publication_name"]
            )
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "Could not release PostgreSQL capture for schedule %s (slot %s): %s",
                sched.id,
                slot["slot_name"],
                exc,
            )
            results.append({**slot, "slot": "unreachable", "error": str(exc)})
            leftover.append(slot)
            continue
        results.append({**slot, **detail})
        if detail.get("slot") == "active":
            leftover.append(slot)
            continue
        for key in slot["cursor_keys"]:
            clear_watermark(key)
    primary = next(
        (r for r in results if r.get("slot") in {"active", "unreachable", "dropped"}),
        results[0],
    )
    out: dict[str, Any] = {
        "cursor_key": primary["cursor_keys"][0],
        "slot_name": primary["slot_name"],
        "publication_name": primary["publication_name"],
        "slot": primary.get("slot"),
        "publication": primary.get("publication"),
        "slots": results,
    }
    if not leftover:
        return {"released": True, "reason": "ok", "watermark_cleared": True, **out}
    reason = (
        "slot_active"
        if any(r.get("slot") == "active" for r in results)
        else "source_unreachable"
    )
    if reason == "source_unreachable":
        out["error"] = next((r.get("error") for r in results if r.get("error")), "")
    out["next_action"] = "; ".join(
        f"SELECT pg_drop_replication_slot('{s['slot_name']}') on {database}"
        for s in leftover
    )
    return _record_pending(sched, {"released": False, "reason": reason, **out}, slots=leftover)


# ── Durable pending-release ledger ──────────────────────────────────────────

_LEDGER = "cdc_pending_capture_releases"


def _ledger_path():
    from services.platform_config import data_dir

    return data_dir() / f"{_LEDGER}.json"


def _ledger_coll():
    try:
        from services.schedule_store import _mongo_backend

        svc = _mongo_backend()
        return svc.get_database()[_LEDGER] if svc else None
    except Exception:  # noqa: BLE001
        return None


def _file_entries() -> list[dict[str, Any]]:
    path = _ledger_path()
    if not path.exists():
        return []
    try:
        return list(json.loads(path.read_text(encoding="utf-8")).get("pending") or [])
    except (OSError, ValueError):
        return []


def _write_file_entries(entries: list[dict[str, Any]]) -> None:
    from services.atomic_file import write_json_atomic

    write_json_atomic(_ledger_path(), {"pending": entries})


def list_pending_capture_releases() -> list[dict[str, Any]]:
    coll = _ledger_coll()
    if coll is not None:
        return [{k: v for k, v in d.items() if k != "_id"} for d in coll.find({})]
    return _file_entries()


def _record_pending(
    sched: PipelineSchedule, out: dict[str, Any], *, slots: list[dict[str, Any]]
) -> dict[str, Any]:
    """Persist what the delete could not release. Connector id, not secrets."""
    entry = {
        "id": str(uuid.uuid4()),
        "schedule_id": sched.id,
        "schedule_name": sched.name,
        "workspace_id": sched.workspace_id or "",
        "source_connector_id": sched.source_connector_id,
        "reason": out.get("reason"),
        "slots": [
            {k: s.get(k) for k in ("slot_name", "publication_name", "cursor_keys")}
            for s in slots
        ],
        "next_action": out.get("next_action", ""),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "attempts": 0,
    }
    try:
        coll = _ledger_coll()
        if coll is not None:
            coll.update_one({"_id": entry["id"]}, {"$set": entry}, upsert=True)
        else:
            _write_file_entries(_file_entries() + [entry])
        out["pending_id"] = entry["id"]
    except Exception as exc:  # noqa: BLE001
        _logger.error("CDC pending-release ledger write failed for %s: %s", sched.id, exc)
        out["pending_ledger_error"] = str(exc)
    return out


def _drop_pending(entry_id: str) -> None:
    coll = _ledger_coll()
    if coll is not None:
        coll.delete_many({"_id": {"$in": [entry_id]}})
        return
    _write_file_entries([e for e in _file_entries() if e.get("id") != entry_id])


def _bump_pending(entry: dict[str, Any], error: str) -> None:
    patch = {"attempts": int(entry.get("attempts") or 0) + 1, "last_error": error[:300],
             "last_attempt_at": datetime.now(timezone.utc).isoformat()}
    coll = _ledger_coll()
    if coll is not None:
        coll.update_one({"_id": entry["id"]}, {"$set": patch})
        return
    _write_file_entries([{**e, **patch} if e.get("id") == entry["id"] else e for e in _file_entries()])


def _retry_due(entry: dict[str, Any], now: datetime) -> bool:
    """60s × 2^attempts, capped at one hour, from the last attempt."""
    stamp = entry.get("last_attempt_at") or entry.get("recorded_at")
    if not stamp:
        return True
    try:
        last = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    if not entry.get("last_attempt_at"):
        return True
    wait = min(3600, 60 * (2 ** min(int(entry.get("attempts") or 0), 6)))
    return (now - last).total_seconds() >= wait


def retry_pending_capture_releases(limit: int = 20) -> list[dict[str, Any]]:
    """Retry releases a delete could not finish. Called by the scheduler beat.

    Slots with an attached consumer are left alone (never cut off a live
    reader); a released or already-absent slot closes the entry.
    """
    from services.connector_store import get_connector

    import connectors.postgresql_change_stream as pcs

    outcomes: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    due = [e for e in list_pending_capture_releases() if _retry_due(e, now)]
    for entry in due[:limit]:
        slots = list(entry.get("slots") or [])
        if not slots:
            continue
        connector = get_connector(
            entry.get("source_connector_id") or "", entry.get("workspace_id") or None
        )
        if connector is None:
            _bump_pending(entry, "source connector missing")
            continue
        cfg = connector.to_dict()
        remaining: list[dict[str, Any]] = []
        error = ""
        for slot in slots:
            try:
                detail = pcs.release_pg_capture(
                    cfg,
                    slot_name=slot["slot_name"],
                    publication_name=slot.get("publication_name") or "",
                )
            except Exception as exc:  # noqa: BLE001
                remaining.append(slot)
                error = str(exc)
                continue
            if detail.get("slot") == "active":
                remaining.append(slot)
                error = "slot has an attached consumer"
                continue
            for key in slot.get("cursor_keys") or []:
                clear_watermark(key)
        if remaining:
            _bump_pending(entry, error)
            outcomes.append({"id": entry["id"], "released": False, "error": error})
        else:
            _drop_pending(entry["id"])
            _logger.info("Pending CDC capture release %s completed", entry["id"])
            outcomes.append({"id": entry["id"], "released": True})
    return outcomes
