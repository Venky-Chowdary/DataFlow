"""What Datawrap's PostgreSQL CDC left on a source, and the safe way to remove it.

Ownership (C11). A PostgreSQL CDC route makes exactly two objects on the
*source* database, both named by ``connectors.postgresql_change_stream``:

* a logical replication slot ``df_<database>_<table | mt_<digest>>_<8 hex>``
  (the hex is the route's cursor key hash), which holds WAL until it is read;
* for ``pgoutput``, a publication ``df_pub_<database>_<table>_<10 hex>``.

Every time Datawrap *creates* one of them it records an ownership entry in the
capture registry (``cdc_capture_registry``: control-plane Mongo, or a JSON file
under the data directory): the server, database, slot and publication names,
the route's cursor key, the captured tables and the job that created it. That
registry — not the name prefix — is what makes a slot Datawrap's to drop. A
slot that only *looks* like ours (``df_`` name, created before the registry
existed) is reported as ``datawrap_named`` and is removed only when an
operator names it and passes ``include_unregistered``. Anything else on the
server is ``external`` and is never touched.

A slot outlives the route that made it when the route is retired without a
schedule delete (a one-shot CDC run, an abandoned route) or when its source
table is dropped: the slot keeps retaining WAL for a table it can no longer
capture (QA C11). This module

* lists every logical slot and Datawrap publication on a source connector with
  its owner, the live CDC schedules that use it, its captured tables and
  whether they still exist, the WAL it retains, a ``state`` and warnings
  (``inventory_pg_captures``);
* drops what is safe on an explicit operator action (``cleanup_pg_captures``):
  owned, inactive (no reader attached) and either source-table-gone or not
  used by any live CDC schedule — audited, watermark cleared, registry entry
  closed;
* detects, from the scheduler beat, every owned slot that is ``orphaned`` or
  ``source_table_gone``, logs the warning and alerts the connector's workspace
  notification channels once per state, naming the WAL it holds
  (``sweep_source_gone_captures``). Nothing is dropped by the beat unless a
  platform admin turned on the deployment setting ``cdc_orphan_auto_drop``
  (off by default, audited as ``cdc.capture.auto_drop.changed``): then an
  owned, inactive slot whose captured tables all stayed gone for
  ``DATAFLOW_CDC_SOURCE_GONE_GRACE_SEC`` (default 900 s) is dropped, audited.
  Off by default because a table dropped and recreated inside a migration
  window, or restored from a backup, would otherwise lose its slot and force
  a full re-snapshot nobody asked for. A one-shot route's slot whose table is
  present is never auto-dropped: a later run of that route resumes from it.

Schedule delete keeps its own release path (``services.cdc_capture_release``);
it closes the registry entry for every slot it drops.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any

_logger = logging.getLogger(__name__)

_REGISTRY = "cdc_capture_registry"
_SLOT_RE = re.compile(r"^df_[a-z0-9_]+$")
_PUB_PREFIX = "df_pub_"
_LOOPBACK = {"localhost", "127.0.0.1", "::1"}
_sweep_lock = threading.Lock()
_last_sweep = 0.0

#: States an operator cleanup may drop.
CLEANABLE_STATES = frozenset({"orphaned", "source_table_gone"})


# ── Registry (ownership ledger) ─────────────────────────────────────────────


def server_identity(cfg: dict[str, Any]) -> str:
    """``host:port/database`` of a PostgreSQL source, loopback folded."""
    host = str(cfg.get("host") or "localhost").strip().lower()
    if host in _LOOPBACK:
        host = "loopback"
    port = str(cfg.get("port") or 5432).strip()
    database = str(cfg.get("database") or "postgres").strip()
    return f"{host}:{port}/{database}"


def _registry_path():
    from services.platform_config import data_dir

    return data_dir() / f"{_REGISTRY}.json"


def _registry_coll():
    try:
        from services.schedule_store import _mongo_backend

        svc = _mongo_backend()
        return svc.get_database()[_REGISTRY] if svc else None
    except Exception:  # noqa: BLE001 - the file store is the fallback
        return None


def _file_entries() -> list[dict[str, Any]]:
    path = _registry_path()
    if not path.exists():
        return []
    try:
        return list(json.loads(path.read_text(encoding="utf-8")).get("captures") or [])
    except (OSError, ValueError):
        return []


def _write_file_entries(entries: list[dict[str, Any]]) -> None:
    from services.atomic_file import write_json_atomic

    write_json_atomic(_registry_path(), {"captures": entries})


def _entry_id(server: str, slot_name: str) -> str:
    return f"{server}|{slot_name}"


def list_registered_captures(server: str | None = None) -> list[dict[str, Any]]:
    coll = _registry_coll()
    if coll is not None:
        query = {"server": server} if server else {}
        rows = [{k: v for k, v in d.items() if k != "_id"} for d in coll.find(query)]
    else:
        rows = _file_entries()
    if server:
        rows = [r for r in rows if r.get("server") == server]
    return rows


def _upsert_entry(entry: dict[str, Any]) -> None:
    coll = _registry_coll()
    if coll is not None:
        coll.update_one({"_id": entry["id"]}, {"$set": entry}, upsert=True)
        return
    rows = [e for e in _file_entries() if e.get("id") != entry["id"]]
    _write_file_entries(rows + [entry])


def _patch_entry(entry_id: str, patch: dict[str, Any]) -> None:
    coll = _registry_coll()
    if coll is not None:
        coll.update_one({"_id": entry_id}, {"$set": patch})
        return
    _write_file_entries(
        [{**e, **patch} if e.get("id") == entry_id else e for e in _file_entries()]
    )


def forget_capture(cfg: dict[str, Any], slot_name: str) -> None:
    """Close the ownership entry of a slot that is gone from the source."""
    entry_id = _entry_id(server_identity(cfg), slot_name)
    try:
        coll = _registry_coll()
        if coll is not None:
            coll.delete_many({"_id": {"$in": [entry_id]}})
            return
        _write_file_entries([e for e in _file_entries() if e.get("id") != entry_id])
    except Exception as exc:  # noqa: BLE001 - a stale entry is re-checked on read
        _logger.warning("CDC capture registry close failed for %s: %s", slot_name, exc)


def record_pg_capture(
    cfg: dict[str, Any],
    *,
    slot_name: str,
    publication_name: str,
    cursor_key: str,
    schema: str,
    tables: list[str],
    plugin: str = "",
) -> None:
    """Record that Datawrap created this slot/publication. Never raises.

    Called by the change stream right after ``pg_create_logical_replication_slot``
    / ``CREATE PUBLICATION``. Holds names and the connector id, never secrets.
    """
    server = server_identity(cfg)
    entry = {
        "id": _entry_id(server, slot_name),
        "server": server,
        "database": str(cfg.get("database") or "postgres"),
        "connector_id": str(cfg.get("connector_id") or ""),
        "slot_name": slot_name,
        "publication_name": publication_name or "",
        "cursor_key": cursor_key,
        "schema": schema or "public",
        "tables": [str(t) for t in tables],
        "plugin": plugin or "",
        "created_by_job": str(cfg.get("job_id") or ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        existing = next(
            (e for e in list_registered_captures(server) if e.get("id") == entry["id"]), None
        )
        if existing:
            # First creation stamp and job stay; a re-created slot refreshes the rest.
            entry["created_at"] = existing.get("created_at") or entry["created_at"]
            entry["created_by_job"] = existing.get("created_by_job") or entry["created_by_job"]
            entry["connector_id"] = entry["connector_id"] or existing.get("connector_id") or ""
        _upsert_entry(entry)
    except Exception as exc:  # noqa: BLE001 - capture must not fail on bookkeeping
        _logger.warning("CDC capture registry write failed for %s: %s", slot_name, exc)


# ── Inventory ───────────────────────────────────────────────────────────────


def _schedule_slot_users(connector_id: str, workspace_id: str, cfg: dict[str, Any]) -> dict[str, list[str]]:
    """slot name -> ids of live CDC schedules on this source connector that use it."""
    from services.schedule_store import list_schedules
    from services.sync_cursor import normalize_sync_mode

    users: dict[str, list[str]] = {}
    for sched in list_schedules():
        if normalize_sync_mode(sched.sync_mode) != "cdc":
            continue
        if sched.source_connector_id != connector_id:
            continue
        if workspace_id and (sched.workspace_id or "") != workspace_id:
            continue
        for slot in _schedule_slots(sched, cfg):
            users.setdefault(slot["slot_name"], []).append(sched.id)
    return users


def _schedule_slots(sched: Any, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """The slots a CDC schedule's capture identities name; [] when underivable."""
    from services.cdc_capture_release import _pg_slots, capture_identities

    try:
        return _pg_slots(cfg, capture_identities(sched))
    except Exception as exc:  # noqa: BLE001 - one bad schedule must not hide the rest
        _logger.info("capture identity of schedule %s unavailable: %s", sched.id, exc)
        return []


def _read_source(cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    from connectors.postgresql_conn import get_connection

    conn = get_connection(
        host=cfg.get("host") or "localhost",
        port=cfg.get("port") or 5432,
        database=str(cfg.get("database") or "postgres"),
        username=cfg.get("username") or "",
        password=cfg.get("password") or "",
        connection_string=cfg.get("connection_string") or "",
        ssl=bool(cfg.get("ssl")),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT slot_name, plugin, active, wal_status, restart_lsn::text, "
                "confirmed_flush_lsn::text, "
                "CASE WHEN pg_is_in_recovery() OR restart_lsn IS NULL THEN NULL "
                "ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint END "
                "FROM pg_replication_slots "
                "WHERE slot_type = 'logical' AND database = current_database() "
                "ORDER BY slot_name"
            )
            slots = [
                {
                    "slot_name": r[0],
                    "plugin": r[1],
                    "active": bool(r[2]),
                    "wal_status": r[3],
                    "restart_lsn": r[4],
                    "confirmed_flush_lsn": r[5],
                    "retained_wal_bytes": int(r[6]) if r[6] is not None else None,
                }
                for r in cur.fetchall()
            ]
            cur.execute(
                "SELECT p.pubname, pt.schemaname, pt.tablename FROM pg_publication p "
                "LEFT JOIN pg_publication_tables pt ON pt.pubname = p.pubname "
                "WHERE p.pubname LIKE 'df\\_pub\\_%' ORDER BY p.pubname"
            )
            pubs: dict[str, list[str]] = {}
            for name, sch, tbl in cur.fetchall():
                pubs.setdefault(name, [])
                if tbl:
                    pubs[name].append(f"{sch}.{tbl}")
        conn.commit()
    finally:
        conn.close()
    return slots, pubs


def _tables_present(cfg: dict[str, Any], schema: str, tables: list[str]) -> dict[str, bool]:
    from connectors.postgresql_conn import get_connection
    from services.cdc_table_name import split_cdc_tables

    if not tables:
        return {}
    sch, bare = split_cdc_tables(list(tables), schema or "public", dialect="postgresql")
    conn = get_connection(
        host=cfg.get("host") or "localhost",
        port=cfg.get("port") or 5432,
        database=str(cfg.get("database") or "postgres"),
        username=cfg.get("username") or "",
        password=cfg.get("password") or "",
        connection_string=cfg.get("connection_string") or "",
        ssl=bool(cfg.get("ssl")),
    )
    out: dict[str, bool] = {}
    try:
        with conn.cursor() as cur:
            for name, table in zip(tables, bare):
                cur.execute("SELECT to_regclass(%s)", (f'"{sch or "public"}"."{table}"',))
                row = cur.fetchone()
                out[str(name)] = bool(row and row[0] is not None)
        conn.commit()
    finally:
        conn.close()
    return out


def _warn(item: dict[str, Any]) -> str:
    wal = item.get("retained_wal_bytes")
    held = f" It retains {wal:,} bytes of WAL on the source." if isinstance(wal, int) else ""
    if item["state"] == "source_table_gone":
        return (
            f"Replication slot {item['slot_name']} captures "
            f"{', '.join(item['tables']) or 'a table'} which no longer exists on the "
            f"source, so nothing can read it.{held} "
            + (
                "cdc_orphan_auto_drop is on: it is dropped automatically once its "
                "tables stay gone past the grace period. "
                if auto_drop_enabled()
                else "Nothing drops it automatically (cdc_orphan_auto_drop is off). "
            )
            + "Clean it up with "
            "POST /api/v1/ops/cdc-captures/cleanup (MCP: cleanup_cdc_captures)."
        )
    if item["state"] == "orphaned":
        return (
            f"Replication slot {item['slot_name']} was created by Datawrap but no CDC "
            f"schedule uses it (a one-shot or retired route).{held} A later run of the "
            "same route resumes from it; if the route is retired, clean it up with "
            "POST /api/v1/ops/cdc-captures/cleanup (MCP: cleanup_cdc_captures)."
        )
    if item["state"] == "orphaned_publication":
        return (
            f"Publication {item['publication_name']} was created by Datawrap and has "
            "no replication slot left; it holds no WAL. Clean it up with "
            "POST /api/v1/ops/cdc-captures/cleanup."
        )
    return ""


def inventory_pg_captures(
    cfg: dict[str, Any], *, connector_id: str, workspace_id: str = ""
) -> dict[str, Any]:
    """Every logical slot (and Datawrap publication) on this source, classified.

    ``ownership``: ``datawrap`` (in the capture registry, or derived from a live
    CDC schedule of this connector), ``datawrap_named`` (Datawrap's naming but
    no ownership record) or ``external``. ``state``: ``active`` (a reader is
    attached), ``in_use`` (a live CDC schedule uses it), ``source_table_gone``,
    ``orphaned`` (Datawrap's, no live schedule), ``unregistered`` or
    ``external``; a publication whose slot is gone is ``orphaned_publication``.
    """
    cfg = {**cfg, "connector_id": cfg.get("connector_id") or connector_id}
    server = server_identity(cfg)
    slots, pubs = _read_source(cfg)
    registry = {e["slot_name"]: e for e in list_registered_captures(server)}
    users = _schedule_slot_users(connector_id, workspace_id, cfg)
    items: list[dict[str, Any]] = []
    slot_pubs: set[str] = set()
    for slot in slots:
        name = slot["slot_name"]
        entry = registry.get(name)
        if entry or name in users:
            ownership = "datawrap"
        elif _SLOT_RE.match(name):
            ownership = "datawrap_named"
        else:
            ownership = "external"
        pub = str((entry or {}).get("publication_name") or "")
        if pub:
            slot_pubs.add(pub)
        tables = list((entry or {}).get("tables") or [])
        schema = str((entry or {}).get("schema") or "public")
        present = (
            _tables_present(cfg, schema, tables) if tables and ownership != "external" else {}
        )
        if ownership == "external":
            state = "external"
        elif slot["active"]:
            state = "active"
        elif present and not any(present.values()):
            state = "source_table_gone"
        elif name in users:
            state = "in_use"
        elif ownership == "datawrap":
            state = "orphaned"
        else:
            state = "unregistered"
        item = {
            **slot,
            "ownership": ownership,
            "state": state,
            "publication_name": pub or None,
            "publication_exists": (pub in pubs) if pub else None,
            "cursor_key": (entry or {}).get("cursor_key"),
            "tables": tables,
            "tables_present": present,
            "used_by_schedules": users.get(name, []),
            "created_by_job": (entry or {}).get("created_by_job"),
            "created_at": (entry or {}).get("created_at"),
            "source_gone_since": (entry or {}).get("source_gone_since"),
            "alerted_state": (entry or {}).get("alerted_state"),
        }
        item["cleanup_allowed"] = bool(
            ownership == "datawrap" and state in CLEANABLE_STATES and not slot["active"]
        )
        item["warning"] = _warn(item)
        items.append(item)
    registered_pubs = {
        str(e.get("publication_name") or ""): e for e in registry.values() if e.get("publication_name")
    }
    for pub_name, pub_tables in pubs.items():
        if pub_name in slot_pubs:
            continue
        owned = pub_name in registered_pubs
        item = {
            "slot_name": None,
            "publication_name": pub_name,
            "publication_exists": True,
            "ownership": "datawrap" if owned else "datawrap_named",
            "state": "orphaned_publication",
            "tables": pub_tables,
            "used_by_schedules": [],
            "retained_wal_bytes": 0,
        }
        item["cleanup_allowed"] = owned
        item["warning"] = _warn(item)
        items.append(item)
    warnings = [i["warning"] for i in items if i.get("warning")]
    return {
        "connector_id": connector_id,
        "server": server,
        "captures": items,
        "count": len(items),
        "orphaned_count": sum(
            1 for i in items if i["state"] in CLEANABLE_STATES | {"orphaned_publication"}
        ),
        "retained_wal_bytes_orphaned": sum(
            int(i.get("retained_wal_bytes") or 0) for i in items if i["state"] in CLEANABLE_STATES
        ),
        "warnings": warnings,
        "auto_drop": {
            "enabled": auto_drop_enabled(),
            "setting": "cdc_orphan_auto_drop",
            "grace_seconds": _grace_seconds(),
            "applies_to": "source_table_gone",
        },
        "ownership": (
            "Datawrap owns a slot or publication only when it created it (capture "
            "registry) or a live CDC schedule of this connector derives its name. "
            "'datawrap_named' slots are removed only when named with "
            "include_unregistered; 'external' slots are never touched."
        ),
    }


# ── Cleanup ─────────────────────────────────────────────────────────────────


def _drop(cfg: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    import connectors.postgresql_change_stream as pcs
    from services.sync_cursor import clear_watermark

    detail = pcs.release_pg_capture(
        cfg,
        slot_name=str(item.get("slot_name") or ""),
        publication_name=str(item.get("publication_name") or ""),
    )
    if detail.get("slot") == "active":
        return {**detail, "action": "skipped", "reason": "a reader attached before the drop"}
    if item.get("cursor_key"):
        clear_watermark(str(item["cursor_key"]))
    if item.get("slot_name"):
        forget_capture(cfg, str(item["slot_name"]))
    return {**detail, "action": "dropped"}


def cleanup_pg_captures(
    cfg: dict[str, Any],
    *,
    connector_id: str,
    workspace_id: str = "",
    slot_names: list[str] | None = None,
    publication_names: list[str] | None = None,
    include_unregistered: bool = False,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Drop the orphaned captures an operator asked for (all cleanable by default).

    Re-inventories the source first: the decision is made on the source's
    current state, never on a listing the caller held. A slot is dropped only
    when Datawrap owns it (``datawrap``; ``datawrap_named`` with
    ``include_unregistered`` and an explicit name), no reader is attached, and
    it is ``orphaned`` or ``source_table_gone``. An ``in_use`` slot is never
    dropped here — delete or retire its schedule instead.
    """
    inv = inventory_pg_captures(cfg, connector_id=connector_id, workspace_id=workspace_id)
    wanted_slots = {s for s in (slot_names or []) if s}
    wanted_pubs = {p for p in (publication_names or []) if p}
    explicit = bool(wanted_slots or wanted_pubs)
    results: list[dict[str, Any]] = []
    cfg = {**cfg, "connector_id": cfg.get("connector_id") or connector_id}
    for item in inv["captures"]:
        name = item.get("slot_name")
        if explicit and not (
            (name and name in wanted_slots)
            or (not name and item.get("publication_name") in wanted_pubs)
        ):
            continue
        base = {
            "slot_name": name,
            "publication_name": item.get("publication_name"),
            "state": item["state"],
            "ownership": item["ownership"],
            "retained_wal_bytes": item.get("retained_wal_bytes"),
        }
        allowed = item["cleanup_allowed"] or (
            include_unregistered
            and explicit
            and item["ownership"] == "datawrap_named"
            and item["state"] in {"unregistered", "orphaned_publication", "source_table_gone"}
        )
        if not allowed:
            reason = {
                "external": "not created by Datawrap — never dropped",
                "active": "a reader is attached",
                "in_use": f"used by CDC schedule(s) {', '.join(item['used_by_schedules'])}",
                "unregistered": "named like a Datawrap slot but no ownership record; "
                "name it with include_unregistered=true to drop it",
            }.get(item["state"], "not cleanable in this state")
            results.append({**base, "action": "skipped", "reason": reason})
            continue
        if dry_run:
            results.append({**base, "action": "would_drop"})
            continue
        results.append({**base, **_drop_reporting(cfg, item)})
    if explicit:
        known = {i.get("slot_name") for i in inv["captures"]} | {
            i.get("publication_name") for i in inv["captures"]
        }
        for missing in sorted((wanted_slots | wanted_pubs) - known):
            results.append({"slot_name": missing, "action": "skipped", "reason": "not on the source"})
    return {
        "connector_id": connector_id,
        "server": inv["server"],
        "dry_run": bool(dry_run),
        "results": results,
        "dropped": [r["slot_name"] or r["publication_name"] for r in results if r.get("action") == "dropped"],
    }


# ── Beat: source table gone ────────────────────────────────────────────────


def _grace_seconds() -> float:
    from services.brand_env import getenv_brand

    try:
        return max(0.0, float(getenv_brand("CDC_SOURCE_GONE_GRACE_SEC", 900)))
    except (TypeError, ValueError):
        return 900.0


def _sweep_inventory(cfg: dict[str, Any], connector_id: str) -> list[dict[str, Any]]:
    """The source's captures, or [] when it cannot be read (next beat retries)."""
    try:
        return list(inventory_pg_captures(cfg, connector_id=connector_id)["captures"])
    except Exception as exc:  # noqa: BLE001 - an unreachable source waits for the next beat
        _logger.info("CDC capture sweep skipped connector %s: %s", connector_id, exc)
        return []


def _drop_reporting(cfg: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    """:func:`_drop`, with a failure reported as an outcome instead of raised."""
    try:
        return _drop(cfg, item)
    except Exception as exc:  # noqa: BLE001 - report per slot, keep going
        return {"action": "failed", "reason": str(exc)[:300]}


def auto_drop_enabled() -> bool:
    """The deployment setting ``cdc_orphan_auto_drop`` (off by default)."""
    try:
        from services.workspace_store import get_workspace_settings

        return bool(get_workspace_settings().get("cdc_orphan_auto_drop"))
    except Exception:  # noqa: BLE001 - an unreadable setting is "off"
        return False


def _alert_orphan(connector: Any, item: dict[str, Any]) -> bool:
    """One alert per orphan state through the workspace's notification channels.

    Also a WARNING log line, so a deployment without channels still shows it.
    Never raises: the beat must keep going.
    """
    _logger.warning("%s", item["warning"])
    wal = item.get("retained_wal_bytes")
    name = getattr(connector, "name", "") or getattr(connector, "id", "")
    payload = {
        "event": "cdc.capture.orphaned",
        "status": "warning",
        "title": f"CDC replication slot {item.get('slot_name')} on “{name}” is orphaned",
        "text": (
            f"{item['warning']}\n"
            f"Source connector: {name}\n"
            f"State: {item['state']}\n"
            + (f"WAL held: {wal:,} bytes\n" if isinstance(wal, int) else "")
            + (
                "Automatic drop is on: it is dropped once its tables stay gone past the grace period."
                if auto_drop_enabled() and item["state"] == "source_table_gone"
                else "Automatic drop is off: nothing is dropped until an admin cleans it up."
            )
        ),
        "connector_id": getattr(connector, "id", ""),
        "slot_name": item.get("slot_name"),
        "state": item["state"],
        "retained_wal_bytes": wal,
    }
    try:
        from services.notification_delivery_log import delivery_context
        from services.notification_service import notify_workspace

        with delivery_context(event="cdc.capture.orphaned"):
            notify_workspace(str(getattr(connector, "workspace_id", "") or ""), payload)
        return True
    except Exception as exc:  # noqa: BLE001 - the warning log line above stands
        _logger.info("CDC orphan alert not delivered for %s: %s", item.get("slot_name"), exc)
        return False


def _gone_for(since: Any, now: datetime) -> float:
    try:
        return (now - datetime.fromisoformat(str(since))).total_seconds()
    except ValueError:
        return 0.0


def _sweep_one(
    cfg: dict[str, Any], connector: Any, entry_id: str, item: dict[str, Any],
    *, now: datetime, grace: float, auto_drop: bool,
) -> dict[str, Any]:
    """Report (and alert once per state) one owned capture; drop it only when allowed."""
    name = item.get("slot_name")
    out = {
        "slot_name": name,
        "state": item["state"],
        "retained_wal_bytes": item.get("retained_wal_bytes"),
        "action": "none",
    }
    if item["state"] not in CLEANABLE_STATES:
        patch = {k: None for k in ("source_gone_since", "alerted_state") if item.get(k)}
        if patch:
            _patch_entry(entry_id, patch)
        return out
    out["action"] = "reported"
    if item.get("alerted_state") != item["state"]:
        out["alerted"] = _alert_orphan(connector, item)
        _patch_entry(entry_id, {"alerted_state": item["state"], "alerted_at": now.isoformat()})
    if item["state"] != "source_table_gone":
        return out
    since = item.get("source_gone_since")
    if not since:
        _patch_entry(entry_id, {"source_gone_since": now.isoformat()})
        return out
    if not auto_drop or _gone_for(since, now) < grace or not item["cleanup_allowed"]:
        return out
    detail = _drop_reporting(cfg, item)
    out.update(detail)
    if detail.get("action") == "dropped":
        from services.audit_coverage import record_security_event

        record_security_event(
            "cdc.capture.cleanup",
            outcome="succeeded",
            resource=f"pg_slot:{name}",
            actor="system:scheduler",
            details={
                "connector_id": cfg.get("connector_id"),
                "slot_name": name,
                "publication_name": item.get("publication_name"),
                "reason": "source_table_gone",
                "setting": "cdc_orphan_auto_drop",
                "tables": item.get("tables"),
                "source_gone_since": since,
                "retained_wal_bytes": item.get("retained_wal_bytes"),
            },
        )
    return out


def sweep_source_gone_captures(*, min_interval_sec: float = 300.0) -> list[dict[str, Any]]:
    """Detect orphaned Datawrap slots, warn and alert; drop only when the setting allows.

    For every owned slot that is ``orphaned`` or ``source_table_gone`` the beat
    logs the warning and alerts the connector's workspace channels once per
    state (WAL held included). The first sweep that sees every captured table
    missing stamps ``source_gone_since``; a table that comes back clears it.
    Only with the deployment setting ``cdc_orphan_auto_drop`` on (off by
    default) is a source-table-gone slot dropped, once its tables stayed gone
    past ``DATAFLOW_CDC_SOURCE_GONE_GRACE_SEC`` — audited. Called by the
    scheduler beat; throttled to one pass per ``min_interval_sec``.
    """
    global _last_sweep
    with _sweep_lock:
        now_mono = time.monotonic()
        if _last_sweep and now_mono - _last_sweep < min_interval_sec:
            return []
        _last_sweep = now_mono
    from services.connector_store import get_connector

    outcomes: list[dict[str, Any]] = []
    by_connector: dict[str, list[dict[str, Any]]] = {}
    for entry in list_registered_captures():
        cid = str(entry.get("connector_id") or "")
        if cid:
            by_connector.setdefault(cid, []).append(entry)
    grace = _grace_seconds()
    auto_drop = auto_drop_enabled()
    now = datetime.now(timezone.utc)
    for cid, entries in by_connector.items():
        connector = get_connector(cid)
        if connector is None:
            continue
        cfg = {**connector.to_dict(), "connector_id": cid}
        ids = {e["slot_name"]: e["id"] for e in entries}
        for item in _sweep_inventory(cfg, cid):
            if item.get("slot_name") in ids:
                outcomes.append(
                    _sweep_one(
                        cfg, connector, ids[item["slot_name"]], item,
                        now=now, grace=grace, auto_drop=auto_drop,
                    )
                )
    return outcomes


def start_source_gone_sweep(*, min_interval_sec: float = 300.0) -> bool:
    """Run :func:`sweep_source_gone_captures` off the beat thread (it dials sources).

    Returns True when a sweep was started. One sweep at a time; throttled.
    """
    with _sweep_lock:
        if _last_sweep and time.monotonic() - _last_sweep < min_interval_sec:
            return False
    if any(t.name == "df-cdc-capture-sweep" and t.is_alive() for t in threading.enumerate()):
        return False

    def _run() -> None:
        try:
            sweep_source_gone_captures(min_interval_sec=min_interval_sec)
        except Exception:  # noqa: BLE001 - the next beat retries
            _logger.exception("CDC source-gone capture sweep failed")

    threading.Thread(target=_run, name="df-cdc-capture-sweep", daemon=True).start()
    return True
