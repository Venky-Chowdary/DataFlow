"""What Datawrap's PostgreSQL CDC left on a source, and the safe way to remove it.

Ownership (C11). A PostgreSQL CDC route makes exactly two objects on the
*source* database, both named by ``connectors.postgresql_change_stream``:

* a logical replication slot ``df_<database>_<table | mt_<digest>>_<8 hex>``
  (the hex is the route's cursor key hash), which holds WAL until it is read;
* for ``pgoutput``, a publication ``df_pub_<database>_<table>_<10 hex>``.

Every time a Datawrap run creates — or resumes on — one of them it records an
ownership row in the capture registry (``cdc_capture_registry``: the shared
control-plane Mongo, or a locked JSON file under the data directory when the
deployment has no database). A row is keyed by the *server identity* — the
cluster's ``pg_control_system().system_identifier`` plus the database (the
resolved endpoint when the identifier cannot be read) — and the slot, and
carries the connector and the workspace that recorded it. The registry, not
the ``df_`` name prefix, makes a slot Datawrap's.

Scope. A caller sees and cleans only rows its own workspace recorded (a
platform admin with no workspace sees all). Every other slot or publication on
the server is counted as ``foreign`` — no name, table, cursor key or job — and
is never cleanable. A slot is *in use* when any CDC schedule, in any workspace,
whose source resolves to the same server derives its name; it is *running*
when a live CDC lease or a pending/running job holds it. Neither is ever
cleaned, and "source table gone" on an in-use slot is a warning only.

Operations:

* ``inventory_pg_captures`` — the listing with owner, state, WAL held and a
  warning per orphan;
* ``cleanup_pg_captures`` — the explicit operator action: drops only owned,
  inactive, not-in-use, not-running slots that are ``orphaned`` or
  ``source_table_gone`` (and publications no slot pairs with), re-checking
  in-use and running immediately before each drop; audited by the caller;
* ``sweep_source_gone_captures`` — the scheduler beat: warns and alerts the
  workspace channels once per orphan state (an atomic claim, released when the
  delivery fails), and drops a source-table-gone slot only when the
  deployment setting ``cdc_orphan_auto_drop`` is on (off by default, kept in
  the shared control-plane store), counting the grace
  (``DATAFLOW_CDC_SOURCE_GONE_GRACE_SEC``, default 900 s) from the later of
  "tables gone" and "setting turned on".

Schedule delete keeps its own release path (``services.cdc_capture_release``);
it closes the registry row for every slot it drops.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Iterator

_logger = logging.getLogger(__name__)

_REGISTRY = "cdc_capture_registry"
_FLAGS = "platform_flags"
_AUTO_DROP_ID = "cdc_orphan_auto_drop"
_LOOPBACK = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}
_UNREGISTERED_PLUGINS = frozenset({"pgoutput", "test_decoding"})
_SLOT_STEM = re.compile(r"^df_(.+)_[0-9a-f]{8}$")
_PUB_STEM = re.compile(r"^df_pub_(.+)_[0-9a-f]{10}$")
_NAME_MAX = 63
_SID_TTL = 300.0
_sid_cache: dict[str, tuple[float, str | None]] = {}
_lock = threading.RLock()
_sweep_lock = threading.Lock()
_last_sweep = 0.0

#: States an operator cleanup may drop.
CLEANABLE_STATES = frozenset({"orphaned", "source_table_gone"})
#: Job statuses that still own their capture.
_LIVE_JOB_STATUSES = frozenset({"pending", "queued", "running", "retrying"})


# ── Server identity ─────────────────────────────────────────────────────────


def resolved_endpoint(cfg: dict[str, Any]) -> dict[str, Any]:
    """Host, port and database the PostgreSQL driver will really dial."""
    from connectors.sql_dsn import resolve_sql_endpoint

    ep = resolve_sql_endpoint(
        family="postgresql",
        host=str(cfg.get("host") or ""),
        port=int(cfg.get("port") or 0),
        database=str(cfg.get("database") or ""),
        username=str(cfg.get("username") or ""),
        password=str(cfg.get("password") or ""),
        connection_string=str(cfg.get("connection_string") or ""),
        default_port=5432,
    )
    host = str(ep.get("host") or "localhost").strip().lower()
    return {
        "host": "loopback" if host in _LOOPBACK else host,
        "port": int(ep.get("port") or 5432),
        "database": str(ep.get("database") or cfg.get("database") or "postgres"),
    }


def endpoint_key(cfg: dict[str, Any]) -> str:
    ep = resolved_endpoint(cfg)
    return f"{ep['host']}:{ep['port']}/{ep['database']}"


@contextlib.contextmanager
def _pg(cfg: dict[str, Any]) -> Iterator[Any]:
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
        yield conn
    finally:
        conn.close()


def _read_system_identifier(cur: Any) -> str | None:
    try:
        cur.execute("SELECT system_identifier::text FROM pg_control_system()")
        row = cur.fetchone()
        return str(row[0]) if row and row[0] else None
    except Exception:  # noqa: BLE001 - no privilege: the endpoint is the identity
        try:
            cur.connection.rollback()
        except Exception:  # noqa: BLE001
            _logger.debug("rollback after system_identifier probe failed", exc_info=True)
        return None


def system_identifier(cfg: dict[str, Any]) -> str | None:
    """The cluster's ``system_identifier`` (cached per endpoint), or None."""
    key = endpoint_key(cfg)
    hit = _sid_cache.get(key)
    if hit and time.monotonic() - hit[0] < _SID_TTL:
        return hit[1]
    try:
        with _pg(cfg) as conn, conn.cursor() as cur:
            sid = _read_system_identifier(cur)
    except Exception as exc:  # noqa: BLE001 - unreachable: fall back to the endpoint
        _logger.debug("system_identifier unavailable for %s: %s", key, exc)
        return None
    _sid_cache[key] = (time.monotonic(), sid)
    return sid


def server_identity(cfg: dict[str, Any], *, sid: str | None = None) -> str:
    """``pg:<system_identifier>/<database>``, or ``ep:<host:port/db>``."""
    sid = sid if sid is not None else system_identifier(cfg)
    if sid:
        return f"pg:{sid}/{resolved_endpoint(cfg)['database']}"
    return f"ep:{endpoint_key(cfg)}"


# ── Registry (ownership ledger) ─────────────────────────────────────────────


def _coll(name: str) -> Any | None:
    """The shared collection; None without a control plane (file fallback).

    Raises ``ControlPlaneUnavailable`` when one is configured but unreachable:
    ownership and the auto-drop switch must never fall back to a node file.
    """
    from services.control_plane_store import shared_collection

    return shared_collection(name)


def _file(name: str):
    from services.platform_config import data_dir

    return data_dir() / f"{name}.json"


@contextlib.contextmanager
def _file_locked(name: str) -> Iterator[None]:
    """Serialise read-modify-write of a fallback file across threads and processes."""
    path = _file(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock, open(f"{path}.lock", "a+") as handle:
        try:
            import fcntl

            fcntl.flock(handle, fcntl.LOCK_EX)
        except (ImportError, OSError):  # Windows / odd fs: the thread lock still holds
            _logger.debug("file lock unavailable for %s", path)
        yield


def _file_rows(name: str, key: str) -> list[dict[str, Any]]:
    path = _file(name)
    if not path.exists():
        return []
    try:
        return list(json.loads(path.read_text(encoding="utf-8")).get(key) or [])
    except (OSError, ValueError):
        return []


def _write_file_rows(name: str, key: str, rows: list[dict[str, Any]]) -> None:
    from services.atomic_file import write_json_atomic

    write_json_atomic(_file(name), {key: rows})


def _entry_id(server: str, slot_name: str) -> str:
    return f"{server}|{slot_name}"


def list_registered_captures(server: str | None = None) -> list[dict[str, Any]]:
    coll = _coll(_REGISTRY)
    if coll is not None:
        query = {"server": server} if server else {}
        return [{k: v for k, v in d.items() if k != "_id"} for d in coll.find(query)]
    rows = _file_rows(_REGISTRY, "captures")
    return [r for r in rows if not server or r.get("server") == server]


def _patch_entry(entry_id: str, patch: dict[str, Any]) -> None:
    coll = _coll(_REGISTRY)
    if coll is not None:
        coll.update_one({"_id": entry_id}, {"$set": patch})
        return
    with _file_locked(_REGISTRY):
        rows = _file_rows(_REGISTRY, "captures")
        _write_file_rows(
            _REGISTRY, "captures",
            [{**e, **patch} if e.get("id") == entry_id else e for e in rows],
        )


def _claim_alert(entry_id: str, state: str, now: str) -> tuple[bool, Any]:
    """Atomically move the row's ``alerted_state`` to ``state``; True if this caller won."""
    coll = _coll(_REGISTRY)
    if coll is not None:
        before = coll.find_one_and_update(
            {"_id": entry_id, "alerted_state": {"$ne": state}},
            {"$set": {"alerted_state": state, "alerted_at": now}},
        )
        return before is not None, (before or {}).get("alerted_state")
    with _file_locked(_REGISTRY):
        rows = _file_rows(_REGISTRY, "captures")
        for row in rows:
            if row.get("id") == entry_id and row.get("alerted_state") != state:
                previous = row.get("alerted_state")
                row.update({"alerted_state": state, "alerted_at": now})
                _write_file_rows(_REGISTRY, "captures", rows)
                return True, previous
    return False, None


def _release_alert(entry_id: str, state: str, previous: Any) -> None:
    """Give a claim back after a failed delivery, so the next beat retries."""
    coll = _coll(_REGISTRY)
    if coll is not None:
        coll.update_one(
            {"_id": entry_id, "alerted_state": state}, {"$set": {"alerted_state": previous}}
        )
        return
    with _file_locked(_REGISTRY):
        rows = _file_rows(_REGISTRY, "captures")
        for row in rows:
            if row.get("id") == entry_id and row.get("alerted_state") == state:
                row["alerted_state"] = previous
        _write_file_rows(_REGISTRY, "captures", rows)


def _legacy_server_key(cfg: dict[str, Any]) -> str:
    """The row key 52268a5b1 wrote: raw ``host:port/database``, loopback folded."""
    host = str(cfg.get("host") or "localhost").strip().lower()
    if host in {"localhost", "127.0.0.1", "::1"}:
        host = "loopback"
    return f"{host}:{str(cfg.get('port') or 5432).strip()}/{str(cfg.get('database') or 'postgres').strip()}"


def migrate_legacy_rows(cfg: dict[str, Any], server: str) -> int:
    """Re-key rows written before the server identity / workspace scope existed.

    A legacy row carries no workspace: it gets its connector's owner workspace
    (none known: platform scope only). Returns how many rows moved.
    """
    from services.connector_store import connector_owner_workspace

    moved = 0
    for row in list_registered_captures(_legacy_server_key(cfg)):
        cid = str(row.get("connector_id") or "")
        new = {
            **{k: v for k, v in row.items() if k != "_id"},
            "id": _entry_id(server, row["slot_name"]),
            "server": server,
            "endpoint": endpoint_key(cfg),
            "workspace_id": row.get("workspace_id") or (connector_owner_workspace(cid)[1] if cid else ""),
            "migrated_from": row.get("server"),
        }
        coll = _coll(_REGISTRY)
        if coll is not None:
            coll.update_one({"_id": new["id"]}, {"$setOnInsert": new}, upsert=True)
            coll.delete_many({"_id": {"$in": [row.get("id")]}})
        else:
            with _file_locked(_REGISTRY):
                rows = [r for r in _file_rows(_REGISTRY, "captures") if r.get("id") != row.get("id")]
                if not any(r.get("id") == new["id"] for r in rows):
                    rows.append(new)
                _write_file_rows(_REGISTRY, "captures", rows)
        moved += 1
    return moved


def forget_capture(cfg: dict[str, Any], slot_name: str) -> None:
    """Close the ownership row of a slot that is gone from the source."""
    try:
        ids = {
            _entry_id(server_identity(cfg), slot_name),
            _entry_id(f"ep:{endpoint_key(cfg)}", slot_name),
        }
        coll = _coll(_REGISTRY)
        if coll is not None:
            coll.delete_many({"_id": {"$in": sorted(ids)}})
            return
        with _file_locked(_REGISTRY):
            rows = _file_rows(_REGISTRY, "captures")
            _write_file_rows(_REGISTRY, "captures", [e for e in rows if e.get("id") not in ids])
    except Exception as exc:  # noqa: BLE001 - a stale row is re-checked on read
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
) -> bool:
    """Record that a Datawrap run created, or resumed on, this slot/publication.

    Called by the change stream on every slot it creates or attaches to (so a
    slot made before the registry existed is adopted on its next run) and on
    every publication it creates. Creation fields are written once; the last
    job and the tables follow the latest run. Names and ids only — never
    secrets. Never raises: returns False when the row could not be written.
    """
    if not (cfg.get("host") or cfg.get("connection_string")):
        # No endpoint to name the server by: nothing trustworthy to record.
        return False
    try:
        from services.connector_store import connector_owner_workspace

        connector_id = str(cfg.get("connector_id") or "")
        workspace_id = connector_owner_workspace(connector_id)[1] if connector_id else ""
        server = server_identity(cfg)
        now = datetime.now(timezone.utc).isoformat()
        job_id = str(cfg.get("job_id") or "")
        current: dict[str, Any] = {
            "server": server,
            "endpoint": endpoint_key(cfg),
            "database": resolved_endpoint(cfg)["database"],
            "slot_name": slot_name,
            "cursor_key": cursor_key,
            "schema": schema or "public",
            "tables": [str(t) for t in tables],
            "plugin": plugin or "",
            "last_job_id": job_id,
            "last_seen_at": now,
        }
        if publication_name:
            current["publication_name"] = publication_name
        once = {
            "id": _entry_id(server, slot_name),
            "connector_id": connector_id,
            "workspace_id": workspace_id,
            "created_by_job": job_id,
            "created_at": now,
        }
        coll = _coll(_REGISTRY)
        if coll is not None:
            coll.update_one(
                {"_id": once["id"]}, {"$set": current, "$setOnInsert": once}, upsert=True
            )
            return True
        with _file_locked(_REGISTRY):
            rows = _file_rows(_REGISTRY, "captures")
            existing = next((r for r in rows if r.get("id") == once["id"]), None)
            if existing is None:
                rows.append({**once, **current})
            else:
                existing.update(current)
            _write_file_rows(_REGISTRY, "captures", rows)
        return True
    except Exception as exc:  # noqa: BLE001 - capture must not fail on bookkeeping
        _logger.warning("CDC capture registry write failed for %s: %s", slot_name, exc)
        return False


# ── Who uses a slot ─────────────────────────────────────────────────────────


def _schedule_slot_names(sched: Any, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Slots the schedule derives, under the raw and the resolved database."""
    out: dict[str, dict[str, Any]] = {}
    variants = [cfg]
    resolved_db = resolved_endpoint(cfg)["database"]
    if resolved_db != str(cfg.get("database") or "postgres"):
        variants.append({**cfg, "database": resolved_db})
    for variant in variants:
        for slot in _derive_slots(sched, variant):
            out.setdefault(slot["slot_name"], slot)
    return list(out.values())


def _derive_slots(sched: Any, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    from services.cdc_capture_release import schedule_capture_slots

    try:
        return schedule_capture_slots(sched, cfg)
    except Exception as exc:  # noqa: BLE001 - one bad schedule must not hide the rest
        _logger.info("capture identity of schedule %s unavailable: %s", sched.id, exc)
        return []


#: Key in the users map for schedules whose slot names could not be derived.
UNDERIVABLE = "__underivable__"


def schedule_slot_users(cfg: dict[str, Any], server: str = "") -> dict[str, list[dict[str, Any]]]:
    """slot name -> every live CDC schedule (any workspace, any connector) deriving it.

    Matched by the derived slot name — it already embeds the database, the
    table and the route's cursor hash — never by server identity, which a
    rotated password or a DNS blip makes unreadable (review: that failed open).
    A PostgreSQL CDC schedule whose names cannot be derived is listed under
    :data:`UNDERIVABLE` with its tables, and every slot on those tables is
    treated as in use (fails closed). ``cfg``/``server`` are kept for callers.
    """
    from services.connector_store import get_connector
    from services.schedule_store import list_schedules
    from services.sync_cursor import normalize_sync_mode
    from services.workspace_filter import bound_workspace_filter

    users: dict[str, list[dict[str, Any]]] = {}
    with bound_workspace_filter(None):
        for sched in list_schedules():
            if normalize_sync_mode(sched.sync_mode) != "cdc":
                continue
            connector = get_connector(sched.source_connector_id)
            if connector is None or (connector.type or "").lower() not in {"postgresql", "postgres"}:
                continue
            other = {**connector.to_dict(), "connector_id": connector.id}
            slots = _schedule_slot_names(sched, other)
            use = {"schedule_id": sched.id, "workspace_id": sched.workspace_id or ""}
            if not slots:
                users.setdefault(UNDERIVABLE, []).append(
                    {**use, "publication_name": "", "tables": _schedule_tables(sched)}
                )
            for slot in slots:
                users.setdefault(slot["slot_name"], []).append(
                    {**use, "publication_name": slot.get("publication_name") or ""}
                )
    return users


def _schedule_tables(sched: Any) -> list[str]:
    from services.cdc_capture_release import _capture_tables

    tables = _capture_tables(sched)
    return [str(t).split(".")[-1].lower() for t in ([tables] if isinstance(tables, str) else tables)]


def _underivable_uses(users: dict[str, list[dict[str, Any]]], slot_name: str,
                      tables: list[str]) -> list[dict[str, Any]]:
    """Schedules we could not derive whose tables this slot may capture."""
    bare = {str(t).split(".")[-1].lower() for t in tables}
    return [
        u for u in users.get(UNDERIVABLE, [])
        if bare & set(u["tables"]) or any(f"_{t}_" in slot_name.lower() for t in u["tables"])
    ]


def _live_jobs() -> list[dict[str, Any]]:
    """Pending/queued/running/retrying jobs (system scope), slim."""
    from services.mongodb_service import get_mongodb_service

    svc = get_mongodb_service()
    statuses = sorted(_LIVE_JOB_STATUSES)
    if type(svc).__name__ == "MemoryMongoDBService":
        return [dict(j) for j in getattr(svc, "_jobs", {}).values()
                if str(j.get("status") or "") in _LIVE_JOB_STATUSES]
    coll = svc.get_database()["transfer_jobs"]
    return list(coll.find(
        {"status": {"$in": statuses}},
        {"cursor_key": 1, "status": 1, "sync_mode": 1, "transfer_request.source": 1,
         "transfer_request.sync_mode": 1},
    ).limit(500))


def _job_holds(job: dict[str, Any], entry: dict[str, Any]) -> bool:
    if entry.get("cursor_key") and job.get("cursor_key") == entry.get("cursor_key"):
        return True
    req = job.get("transfer_request") or {}
    src = req.get("source") or {}
    sync = str(req.get("sync_mode") or job.get("sync_mode") or "").lower()
    if not sync.startswith("cdc") or not entry.get("connector_id"):
        return False
    if str(src.get("connector_id") or "") != str(entry.get("connector_id")):
        return False
    table = str(src.get("table") or "").split(".")[-1].lower()
    return table in {str(t).split(".")[-1].lower() for t in entry.get("tables") or []}


def _route_running(entry: dict[str, Any] | None, slot_name: str) -> bool:
    """A live CDC lease, or a pending/running job, still holds this capture."""
    from services.cdc_lease import lease_view, list_lease_views

    try:
        if slot_name and any(
            not v.get("stale") for v in list_lease_views(resource=f"pg_slot:{slot_name}")
        ):
            return True
        cursor_key = str((entry or {}).get("cursor_key") or "")
        if cursor_key:
            view = lease_view(cursor_key)
            if view is not None and not view.get("stale"):
                return True
    except Exception as exc:  # noqa: BLE001 - unknown is "running": never drop on doubt
        _logger.info("CDC lease check failed for %s: %s", slot_name, exc)
        return True
    job_ids = {str((entry or {}).get(k) or "") for k in ("created_by_job", "last_job_id")} - {""}
    try:
        from services.mongodb_service import get_mongodb_service

        svc = get_mongodb_service()
        if any(
            str((svc.get_job(jid) or {}).get("status") or "") in _LIVE_JOB_STATUSES
            for jid in job_ids
        ):
            return True
        # A queued job of the same route that has no lease and is not named on
        # the row yet (review): by cursor key, or by connector + table + cdc.
        return any(_job_holds(job, entry or {}) for job in _live_jobs())
    except Exception as exc:  # noqa: BLE001
        _logger.info("CDC job check failed for %s: %s", slot_name, exc)
        return True


# ── Inventory ───────────────────────────────────────────────────────────────


def _stem(name: str, pattern: re.Pattern[str], prefix: str) -> str:
    m = pattern.match(name or "")
    return m.group(1) if m else (name or "")[len(prefix):]


def _paired(pub: str, slot_names: list[str]) -> bool:
    """A publication shares its route stem with a slot still on the server."""
    pstem = _stem(pub, _PUB_STEM, "df_pub_")
    for slot in slot_names:
        sstem = _stem(slot, _SLOT_STEM, "df_")
        if not (pstem and sstem):
            continue
        if pstem == sstem:
            return True
        # Names are cut at 63 characters (hash first): a cut name matches on
        # the prefix the other keeps.
        if (len(pub) >= _NAME_MAX and sstem.startswith(pstem)) or (
            len(slot) >= _NAME_MAX and pstem.startswith(sstem)
        ):
            return True
    return False


def _unregistered_pattern(database: str) -> re.Pattern[str]:
    db = re.sub(r"[^a-z0-9_]", "_", (database or "postgres").lower())
    return re.compile(rf"^df_{re.escape(db)}_.+_[0-9a-f]{{8}}$")


def _read_source(cur: Any) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
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
    return slots, pubs


def _tables_present(cur: Any, schema: str, tables: list[str]) -> dict[str, bool]:
    from services.cdc_table_name import split_cdc_tables

    if not tables:
        return {}
    sch, bare = split_cdc_tables(list(tables), schema or "public", dialect="postgresql")
    out: dict[str, bool] = {}
    for name, table in zip(tables, bare):
        cur.execute("SELECT to_regclass(%s)", (f'"{sch or "public"}"."{table}"',))
        row = cur.fetchone()
        out[str(name)] = bool(row and row[0] is not None)
    return out


def _warn(item: dict[str, Any]) -> str:
    wal = item.get("retained_wal_bytes")
    held = f" It retains {wal:,} bytes of WAL on the source." if isinstance(wal, int) else ""
    cleanup = "Clean it up with POST /api/v1/ops/cdc-captures/cleanup (MCP: cleanup_cdc_captures)."
    if item["state"] == "in_use" and item.get("source_table_missing"):
        return (
            f"Replication slot {item['slot_name']} is used by CDC schedule(s) "
            f"{', '.join(item['used_by_schedules']) or '(another workspace)'} but captures "
            f"{', '.join(item['tables'])}, which no longer exists.{held} It is not cleaned "
            "up while a schedule uses it: restore the table, or retire the schedule."
        )
    if item["state"] == "source_table_gone":
        auto = (
            "cdc_orphan_auto_drop is on: it is dropped automatically once its tables "
            "stay gone past the grace period. "
            if auto_drop_enabled()
            else "Nothing drops it automatically (cdc_orphan_auto_drop is off). "
        )
        return (
            f"Replication slot {item['slot_name']} captures "
            f"{', '.join(item['tables']) or 'a table'} which no longer exists on the "
            f"source, so nothing can read it.{held} {auto}{cleanup}"
        )
    if item["state"] == "orphaned":
        return (
            f"Replication slot {item['slot_name']} was created by Datawrap but no CDC "
            f"schedule uses it (a one-shot or retired route).{held} A later run of the "
            f"same route resumes from it; if the route is retired: {cleanup}"
        )
    if item["state"] == "orphaned_publication":
        who = (
            "was created by Datawrap"
            if item["ownership"] == "datawrap"
            else "is named like a Datawrap publication (no ownership record)"
        )
        return (
            f"Publication {item['publication_name']} {who} and no replication slot "
            f"pairs with it; it holds no WAL. {cleanup}"
        )
    return ""


def _scope_allows(row_ws: Any, workspace_id: str, unrestricted: bool) -> bool:
    return unrestricted or str(row_ws or "") == str(workspace_id or "")


def _slot_state(
    slot: dict[str, Any], ownership: str, uses: list[dict[str, Any]], running: bool, gone: bool
) -> str:
    """In order: who reads it, who uses it, who holds it — then what it captures."""
    if ownership == "external":
        return "external"
    if slot["active"]:
        return "active"
    if uses:
        return "in_use"
    if running:
        return "running"
    if gone:
        return "source_table_gone"
    return "orphaned" if ownership == "datawrap" else "unregistered"


def _slot_item(
    slot: dict[str, Any],
    *,
    entry: dict[str, Any] | None,
    uses: list[dict[str, Any]],
    present: dict[str, bool],
    pubs: dict[str, list[str]],
    workspace_id: str,
    unrestricted: bool,
    unregistered: re.Pattern[str],
    users_all: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    name = slot["slot_name"]
    users_all = users_all or {}
    # The slot's own publication only: its row's, or its own schedule's —
    # never a same-table stem match, which is another route's (review B1/S3).
    own_uses = [u for u in uses if _scope_allows(u["workspace_id"], workspace_id, unrestricted)]
    pub = str((entry or {}).get("publication_name") or "") or next(
        (u["publication_name"] for u in own_uses if u.get("publication_name")), ""
    )
    if not uses:
        uses = _underivable_uses(users_all, name, list((entry or {}).get("tables") or []))
    if entry or uses:
        ownership = "datawrap"
    elif unregistered.match(name) and slot.get("plugin") in _UNREGISTERED_PLUGINS:
        ownership = "datawrap_named"
    else:
        ownership = "external"
    running = ownership != "external" and not uses and _route_running(entry, name)
    gone = bool(present) and not any(present.values())
    state = _slot_state(slot, ownership, uses, running, gone)
    visible_uses = [
        u["schedule_id"] for u in uses if _scope_allows(u["workspace_id"], workspace_id, unrestricted)
    ]
    item = {
        **slot,
        "ownership": ownership,
        "state": state,
        "publication_name": pub or None,
        "publication_exists": (pub in pubs) if pub else None,
        "cursor_key": (entry or {}).get("cursor_key"),
        "tables": list((entry or {}).get("tables") or []),
        "tables_present": present,
        "source_table_missing": gone,
        "used_by_schedules": visible_uses,
        "used_by_other_workspaces": len(uses) - len(visible_uses),
        "created_by_job": (entry or {}).get("created_by_job"),
        "created_at": (entry or {}).get("created_at"),
        "source_gone_since": (entry or {}).get("source_gone_since"),
        "alerted_state": (entry or {}).get("alerted_state"),
        "unregistered_match": ownership == "datawrap_named",
    }
    item["cleanup_allowed"] = bool(
        ownership == "datawrap" and state in CLEANABLE_STATES and not slot["active"]
    )
    item["warning"] = _warn(item)
    return item


def _pub_item(
    pub_name: str, pub_tables: list[str], entry: dict[str, Any] | None
) -> dict[str, Any]:
    running = _route_running(entry, str((entry or {}).get("slot_name") or ""))
    item = {
        "slot_name": None,
        "publication_name": pub_name,
        "publication_exists": True,
        "ownership": "datawrap" if entry is not None else "datawrap_named",
        "state": "running" if running else "orphaned_publication",
        "tables": pub_tables,
        "used_by_schedules": [],
        "retained_wal_bytes": 0,
        "unregistered_match": entry is None,
    }
    item["cleanup_allowed"] = entry is not None and not running
    item["warning"] = _warn(item)
    return item


def inventory_pg_captures(
    cfg: dict[str, Any],
    *,
    connector_id: str,
    workspace_id: str = "",
    unrestricted: bool = False,
) -> dict[str, Any]:
    """Every logical slot (and Datawrap publication) on this source, classified.

    ``workspace_id`` / ``unrestricted`` are the caller's scope: rows another
    workspace recorded, schedules of another workspace, unrecorded slots and
    external slots are summarised as ``foreign_count`` without any detail
    (only an unrestricted platform admin sees them). ``ownership``:
    ``datawrap`` / ``datawrap_named`` / ``external``. ``state``: ``active``,
    ``in_use`` (a CDC schedule in any workspace uses it; table gone is a
    warning only), ``running`` (a live lease or job), ``source_table_gone``,
    ``orphaned``, ``unregistered``, ``external``; an unpaired publication is
    ``orphaned_publication``.
    """
    cfg = {**cfg, "connector_id": cfg.get("connector_id") or connector_id}
    unregistered = _unregistered_pattern(resolved_endpoint(cfg)["database"])
    with _pg(cfg) as conn:
        with conn.cursor() as cur:
            sid = _read_system_identifier(cur)
            server = server_identity(cfg, sid=sid or "")
            slots, pubs = _read_source(cur)
            migrate_legacy_rows(cfg, server)
            registry = {
                e["slot_name"]: e
                for e in list_registered_captures(f"ep:{endpoint_key(cfg)}")
                + list_registered_captures(server)
            }
            present_by_slot = {
                s["slot_name"]: _tables_present(
                    cur,
                    str((registry.get(s["slot_name"]) or {}).get("schema") or "public"),
                    list((registry.get(s["slot_name"]) or {}).get("tables") or []),
                )
                for s in slots
            }
        conn.commit()
    users = schedule_slot_users(cfg, server)
    items: list[dict[str, Any]] = []
    foreign = {"count": 0, "retained_wal_bytes": 0}
    slot_names = [s["slot_name"] for s in slots]
    for slot in slots:
        entry = registry.get(slot["slot_name"])
        uses = users.get(slot["slot_name"], [])
        if entry is not None:
            visible = _scope_allows(entry.get("workspace_id"), workspace_id, unrestricted)
        elif uses:
            visible = unrestricted or any(u["workspace_id"] == workspace_id for u in uses)
        else:
            visible = unrestricted
        if not visible:
            foreign["count"] += 1
            foreign["retained_wal_bytes"] += int(slot.get("retained_wal_bytes") or 0)
            continue
        items.append(
            _slot_item(
                slot, entry=entry, uses=uses, present=present_by_slot.get(slot["slot_name"], {}),
                pubs=pubs, workspace_id=workspace_id, unrestricted=unrestricted,
                unregistered=unregistered, users_all=users,
            )
        )
    offered = {i["publication_name"] for i in items if i.get("publication_name")}
    by_pub = {
        str(e.get("publication_name") or ""): e for e in registry.values() if e.get("publication_name")
    }
    for pub_name, pub_tables in pubs.items():
        entry = by_pub.get(pub_name)
        if pub_name in offered or _paired(pub_name, slot_names):
            # A slot of the same route is still on the server: never offered.
            continue
        if entry is not None and str(entry.get("slot_name") or "") in users:
            continue
        visible = (
            _scope_allows(entry.get("workspace_id"), workspace_id, unrestricted)
            if entry is not None
            else unrestricted
        )
        if not visible:
            foreign["count"] += 1
            continue
        items.append(_pub_item(pub_name, pub_tables, entry))
    warnings = [i["warning"] for i in items if i.get("warning")]
    if foreign["count"]:
        warnings.append(
            f"{foreign['count']} replication slot(s)/publication(s) on this server belong "
            "to another workspace or were not created by Datawrap; they are not listed "
            "and never cleaned up from here."
        )
    return {
        "connector_id": connector_id,
        "server": server,
        "captures": items,
        "count": len(items),
        "foreign_count": foreign["count"],
        "foreign_retained_wal_bytes": foreign["retained_wal_bytes"],
        "orphaned_count": sum(
            1 for i in items if i["state"] in CLEANABLE_STATES | {"orphaned_publication"}
        ),
        "retained_wal_bytes_orphaned": sum(
            int(i.get("retained_wal_bytes") or 0) for i in items if i["state"] in CLEANABLE_STATES
        ),
        "warnings": warnings,
        "auto_drop": {
            **auto_drop_state(),
            "setting": _AUTO_DROP_ID,
            "grace_seconds": _grace_seconds(),
            "applies_to": "source_table_gone",
        },
        "ownership": (
            "Datawrap owns a slot or publication when one of its runs created or "
            "resumed on it (capture registry, scoped to the recording workspace) or "
            "a live CDC schedule derives its name. Slots of other workspaces and "
            "slots Datawrap did not create are counted, not shown, and never touched."
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
    dropped = detail.get("slot") == "dropped" or detail.get("publication") == "dropped"
    if dropped and item.get("cursor_key"):
        clear_watermark(str(item["cursor_key"]))
    if item.get("slot_name"):
        forget_capture(cfg, str(item["slot_name"]))
    return {**detail, "action": "dropped" if dropped else "already_absent"}


def _drop_reporting(cfg: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    """:func:`_drop`, with a failure reported as an outcome instead of raised."""
    try:
        return _drop(cfg, item)
    except Exception as exc:  # noqa: BLE001 - report per slot, keep going
        return {"action": "failed", "reason": str(exc)[:300]}


def _publication_users(
    cfg: dict[str, Any], pub: str, slot: str, users: dict[str, list[dict[str, Any]]]
) -> list[str]:
    """Other slot rows (any workspace) or schedules that use this publication."""
    if not pub:
        return []
    out = [
        f"slot {e['slot_name']}"
        for e in list_registered_captures(server_identity(cfg))
        + list_registered_captures(f"ep:{endpoint_key(cfg)}")
        if e.get("publication_name") == pub and e.get("slot_name") != slot
    ]
    out += [
        f"schedule {u['schedule_id']}"
        for name, uses in users.items()
        if name != slot
        for u in uses
        if u.get("publication_name") == pub
    ]
    return out


def _still_unused(cfg: dict[str, Any], item: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Re-check right before a drop: ``(why not, what may go)``.

    The slot goes only when no schedule derives it and no run holds it; its
    publication goes with it only when no other slot row or schedule uses
    that publication — otherwise the slot alone is dropped.
    """
    slot = str(item.get("slot_name") or "")
    pub = str(item.get("publication_name") or "")
    try:
        server = server_identity(cfg)
        registry = {
            e["slot_name"]: e
            for e in list_registered_captures(f"ep:{endpoint_key(cfg)}")
            + list_registered_captures(server)
        }
        entry = registry.get(slot) or next(
            (e for e in registry.values() if pub and e.get("publication_name") == pub), None
        )
        users = schedule_slot_users(cfg, server)
        if slot and (users.get(slot) or _underivable_uses(users, slot, list((entry or {}).get("tables") or []))):
            return "a CDC schedule uses it", item
        if _route_running(entry or {"cursor_key": item.get("cursor_key")}, slot):
            return "a CDC run holds it", item
        others = _publication_users(cfg, pub, slot, users)
        if others and not slot:
            return f"publication {pub} is used by {', '.join(others)}", item
        if others:
            return "", {**item, "publication_name": None, "publication_kept_for": others}
    except Exception as exc:  # noqa: BLE001 - doubt keeps the slot
        return f"in-use check failed: {exc}"[:300], item
    return "", item


def cleanup_pg_captures(
    cfg: dict[str, Any],
    *,
    connector_id: str,
    workspace_id: str = "",
    unrestricted: bool = False,
    slot_names: list[str] | None = None,
    publication_names: list[str] | None = None,
    include_unregistered: bool = False,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Drop the orphaned captures an operator asked for (all cleanable by default).

    Re-inventories the source in the caller's scope: the decision is made on
    the source's current state, never on a listing the caller held, and only
    on rows the caller's workspace recorded. A slot is dropped only when
    Datawrap owns it (``datawrap``; a ``datawrap_named`` slot — this
    database's slot naming, a Datawrap plugin, no ownership record — only for
    an unrestricted caller who names it with ``include_unregistered``), no
    reader is attached, no schedule uses it and no run holds it (re-checked
    immediately before the drop), and it is ``orphaned`` or
    ``source_table_gone``.
    """
    inv = inventory_pg_captures(
        cfg, connector_id=connector_id, workspace_id=workspace_id, unrestricted=unrestricted
    )
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
        results.append(
            _cleanup_one(cfg, item, explicit=explicit, include_unregistered=include_unregistered,
                         dry_run=dry_run)
        )
    if explicit:
        known = {i.get("slot_name") for i in inv["captures"]} | {
            i.get("publication_name") for i in inv["captures"]
        }
        for missing in sorted((wanted_slots | wanted_pubs) - known):
            results.append(
                {"slot_name": missing, "action": "skipped",
                 "reason": "not on the source, or not in your workspace"}
            )
    return {
        "connector_id": connector_id,
        "server": inv["server"],
        "dry_run": bool(dry_run),
        "results": results,
        "dropped": [
            r.get("slot_name") or r.get("publication_name")
            for r in results if r.get("action") == "dropped"
        ],
    }


def _cleanup_one(
    cfg: dict[str, Any], item: dict[str, Any], *, explicit: bool, include_unregistered: bool,
    dry_run: bool,
) -> dict[str, Any]:
    base = {
        "slot_name": item.get("slot_name"),
        "publication_name": item.get("publication_name"),
        "state": item["state"],
        "ownership": item["ownership"],
        "retained_wal_bytes": item.get("retained_wal_bytes"),
    }
    allowed = item["cleanup_allowed"] or (
        include_unregistered
        and explicit
        and item.get("unregistered_match")
        and item["ownership"] == "datawrap_named"
        and item["state"] in {"unregistered", "orphaned_publication"}
    )
    if not allowed:
        reason = {
            "external": "not created by Datawrap — never dropped",
            "active": "a reader is attached",
            "in_use": "a CDC schedule uses it",
            "running": "a CDC run holds it",
            "unregistered": "named like a Datawrap slot but no ownership record; "
            "name it with include_unregistered=true to drop it",
        }.get(item["state"], "not cleanable in this state")
        return {**base, "action": "skipped", "reason": reason}
    if dry_run:
        return {**base, "action": "would_drop"}
    why, target = _still_unused(cfg, item)
    if why:
        return {**base, "action": "skipped", "reason": why}
    out = {**base, **_drop_reporting(cfg, target)}
    if target.get("publication_kept_for"):
        out["publication_kept_for"] = target["publication_kept_for"]
    return out


# ── The automatic-drop setting (shared across replicas) ────────────────────


def auto_drop_state() -> dict[str, Any]:
    """``{enabled, enabled_at, updated_at, updated_by}`` from the shared store.

    Kept in the control-plane database (``platform_flags``), not in the
    per-replica settings file: every replica's beat reads the same value.
    """
    doc: dict[str, Any] | None = None
    try:
        coll = _coll(_FLAGS)
        if coll is not None:
            doc = coll.find_one({"_id": _AUTO_DROP_ID})
        else:
            doc = next((r for r in _file_rows(_FLAGS, "flags") if r.get("id") == _AUTO_DROP_ID), None)
    except Exception as exc:  # noqa: BLE001 - an unreadable setting is "off"
        _logger.warning("cdc_orphan_auto_drop unreadable, treated as off: %s", exc)
    doc = doc or {}
    return {
        "enabled": bool(doc.get("enabled")),
        "enabled_at": doc.get("enabled_at"),
        "updated_at": doc.get("updated_at"),
        "updated_by": doc.get("updated_by"),
    }


def auto_drop_enabled() -> bool:
    return bool(auto_drop_state()["enabled"])


def set_auto_drop(enabled: bool, *, actor: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Write the shared setting; returns ``(before, after)`` for the audit entry."""
    now = datetime.now(timezone.utc).isoformat()
    patch: dict[str, Any] = {"enabled": bool(enabled), "updated_at": now, "updated_by": actor}
    coll = _coll(_FLAGS)
    if coll is not None:
        before_doc = coll.find_one_and_update({"_id": _AUTO_DROP_ID}, {"$set": patch}, upsert=True) or {}
        if bool(enabled) and not before_doc.get("enabled"):
            coll.update_one({"_id": _AUTO_DROP_ID}, {"$set": {"enabled_at": now}})
    else:
        with _file_locked(_FLAGS):
            rows = _file_rows(_FLAGS, "flags")
            before_doc = dict(next((r for r in rows if r.get("id") == _AUTO_DROP_ID), None) or {})
            row = {**before_doc, "id": _AUTO_DROP_ID, **patch}
            if bool(enabled) and not before_doc.get("enabled"):
                row["enabled_at"] = now
            _write_file_rows(
                _FLAGS, "flags", [r for r in rows if r.get("id") != _AUTO_DROP_ID] + [row]
            )
    before = {"enabled": bool(before_doc.get("enabled")), "enabled_at": before_doc.get("enabled_at")}
    return before, auto_drop_state()


# ── Beat: warn, alert, and (only when allowed) drop ────────────────────────


def _grace_seconds() -> float:
    from services.brand_env import getenv_brand

    try:
        return max(0.0, float(getenv_brand("CDC_SOURCE_GONE_GRACE_SEC", 900)))
    except (TypeError, ValueError):
        return 900.0


def _sweep_inventory(cfg: dict[str, Any], connector_id: str) -> list[dict[str, Any]]:
    """The source's captures, or [] when it cannot be read (next beat retries)."""
    try:
        return list(
            inventory_pg_captures(cfg, connector_id=connector_id, unrestricted=True)["captures"]
        )
    except Exception as exc:  # noqa: BLE001 - an unreachable source waits for the next beat
        _logger.info("CDC capture sweep skipped connector %s: %s", connector_id, exc)
        return []


def _deliver_alert(connector: Any, item: dict[str, Any]) -> bool:
    """Send the orphan alert; True when a channel accepted it, or there is none."""
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
            results = notify_workspace(str(getattr(connector, "workspace_id", "") or ""), payload)
    except Exception as exc:  # noqa: BLE001 - the claim is released, the next beat retries
        _logger.info("CDC orphan alert not delivered for %s: %s", item.get("slot_name"), exc)
        return False
    return not results or any(bool(r.get("ok")) for r in results if isinstance(r, dict))


def _alert_once(entry_id: str, connector: Any, item: dict[str, Any], now: datetime) -> bool | None:
    """Claim the (orphan, state) alert atomically; deliver; release on failure.

    None: another replica (or an earlier beat) already alerted this state.
    """
    won, previous = _claim_alert(entry_id, item["state"], now.isoformat())
    if not won:
        return None
    _logger.warning("%s", item["warning"])
    if _deliver_alert(connector, item):
        return True
    _release_alert(entry_id, item["state"], previous)
    return False


def _gone_for(since: Any, now: datetime) -> float:
    try:
        return (now - datetime.fromisoformat(str(since))).total_seconds()
    except ValueError:
        return 0.0


def _sweep_one(
    cfg: dict[str, Any], connector: Any, entry_id: str, item: dict[str, Any],
    *, now: datetime, grace: float, auto_drop: dict[str, Any],
) -> dict[str, Any]:
    """Report (and alert once per state) one owned capture; drop it only when allowed."""
    name = item.get("slot_name")
    out: dict[str, Any] = {
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
    alerted = _alert_once(entry_id, connector, item, now)
    if alerted is not None:
        out["alerted"] = alerted
    if item["state"] != "source_table_gone":
        return out
    since = item.get("source_gone_since")
    if not since:
        _patch_entry(entry_id, {"source_gone_since": now.isoformat()})
        return out
    if not auto_drop.get("enabled"):
        return out
    # The grace runs from the later of "tables gone" and "setting turned on":
    # turning the setting on never drops at once a slot seen gone long ago.
    gone = min(_gone_for(since, now), _gone_for(auto_drop.get("enabled_at") or now.isoformat(), now))
    if gone < grace or not item["cleanup_allowed"]:
        return out
    why, target = _still_unused(cfg, item)
    if why:
        return out
    detail = _drop_reporting(cfg, target)
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
                "setting": _AUTO_DROP_ID,
                "tables": item.get("tables"),
                "source_gone_since": since,
                "retained_wal_bytes": item.get("retained_wal_bytes"),
            },
        )
    return out


def _sweep_one_safely(
    cfg: dict[str, Any], connector: Any, entry_id: str, item: dict[str, Any], **kwargs: Any
) -> dict[str, Any]:
    """One entry's failure is that entry's outcome; the sweep goes on."""
    try:
        return _sweep_one(cfg, connector, entry_id, item, **kwargs)
    except Exception as exc:  # noqa: BLE001
        _logger.warning("CDC capture sweep failed for %s: %s", item.get("slot_name"), exc)
        return {"slot_name": item.get("slot_name"), "action": "failed", "reason": str(exc)[:300]}


def _connector_for_endpoint(entry: dict[str, Any]) -> str:
    """A saved PostgreSQL connector whose resolved endpoint is the row's."""
    from services.connector_store import list_connectors
    from services.workspace_filter import bound_workspace_filter

    endpoint = str(entry.get("endpoint") or "")
    if not endpoint:
        return ""
    with bound_workspace_filter(None):
        for conn in list_connectors():
            if (conn.type or "").lower() in {"postgresql", "postgres"} and endpoint_key(
                conn.to_dict()
            ) == endpoint:
                return conn.id
    return ""


def sweep_source_gone_captures(*, min_interval_sec: float = 300.0) -> list[dict[str, Any]]:
    """Detect orphaned Datawrap slots, warn and alert; drop only when the setting allows.

    For every owned slot that is ``orphaned`` or ``source_table_gone`` the beat
    logs the warning and alerts the connector's workspace channels once per
    state (WAL held included; the claim is atomic across replicas and is given
    back when no channel accepted the alert). The first sweep that sees every
    captured table missing stamps ``source_gone_since``; a table that comes
    back clears it. Only with ``cdc_orphan_auto_drop`` on is a
    source-table-gone slot dropped, once both the tables have been gone and
    the setting on for the grace period — audited. In-use and running slots
    are never touched. Throttled to one pass per ``min_interval_sec``.
    """
    global _last_sweep
    with _sweep_lock:
        now_mono = time.monotonic()
        if _last_sweep and now_mono - _last_sweep < min_interval_sec:
            return []
        _last_sweep = now_mono
    from services.connector_store import get_connector
    from services.workspace_filter import bound_workspace_filter

    outcomes: list[dict[str, Any]] = []
    by_connector: dict[str, list[dict[str, Any]]] = {}
    for entry in list_registered_captures():
        cid = str(entry.get("connector_id") or "") or _connector_for_endpoint(entry)
        if cid:
            by_connector.setdefault(cid, []).append(entry)
        else:
            _logger.warning(
                "CDC capture %s was recorded without a saved connector and none matches "
                "its endpoint %s; the beat cannot inspect it — list it from a connector "
                "on that server.", entry.get("slot_name"), entry.get("endpoint"),
            )
    grace = _grace_seconds()
    auto_drop = auto_drop_state()
    now = datetime.now(timezone.utc)
    for cid, entries in by_connector.items():
        with bound_workspace_filter(None):
            connector = get_connector(cid)
        if connector is None:
            continue
        cfg = {**connector.to_dict(), "connector_id": cid}
        ids = {e["slot_name"]: e["id"] for e in entries}
        for item in _sweep_inventory(cfg, cid):
            if item.get("slot_name") in ids:
                outcomes.append(
                    _sweep_one_safely(
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
