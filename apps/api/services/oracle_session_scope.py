"""Whether this run writes Oracle instants on a confirmed UTC session.

The grade of an offset or instant source into an Oracle ``TIMESTAMP WITH
[LOCAL] TIME ZONE`` column depends on the destination session, not on the
type pair (accuracy review finding 8, :mod:`connectors.oracle_session`). The
run binds what the destination's session read back; the classifier reads it:

* pinned and read back as UTC: the writer binds UTC digits on a UTC session,
  the instant is kept (an offset label, where the source had one, is
  disclosed, D136), no Risk Contract;
* anything else (statement refused, read-back not UTC, not checked, no
  connection): the instant may move, the pair needs a Risk Contract, and the
  reason says why.

A same-instance Oracle to Oracle ``INSERT ... SELECT`` runs server side and
keeps offsets; it counts only once :mod:`services.copy_oracle_oracle` has
committed to that route for this run (:func:`mark_server_side_oracle_copy`),
never from the configs alone (the copy can still decline to the row writer).

Also here, so the god modules stay inside their budgets: the Execute run scope
(:func:`oracle_run_scope`), the Validate decorator
(:func:`with_oracle_session_pin`), the per-route acknowledgement a non-UTC
session default needs before the pin changes what custom SQL, CDC and mirror
routes evaluate, and the advisory for rows an earlier release wrote unpinned.
"""

from __future__ import annotations

import hashlib
import json
import threading
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

_ORACLE = frozenset({"oracle", "oracledb", "oracle_db", "oracle_autonomous", "oracle_autonomous_warehouse",
                     "amazon_rds_oracle"})
_UNCHECKED = (False, "the Oracle destination session time zone was not checked")
_PIN: ContextVar[tuple[bool, str] | None] = ContextVar("df_oracle_utc_pin", default=None)
_SERVER_SIDE: ContextVar[bool] = ContextVar("df_oracle_server_side_copy", default=False)
_ROWS_WRITTEN: ContextVar[list[int] | None] = ContextVar("df_oracle_rows_written", default=None)

#: Job field stamped on every run into Oracle from this release on; a job
#: without it was written before sessions were pinned (finding 8 upgrade note).
JOB_STAMP = "oracle_session"

ZONE_CHANGE_NOTICE = (
    "This Oracle session defaulted to {zone}; DataFlow now pins Oracle sessions to "
    "UTC, so CURRENT_DATE, CURRENT_TIMESTAMP, LOCALTIMESTAMP, implicit comparisons "
    "of timestamp literals, LogMiner renderings of TIMESTAMP WITH LOCAL TIME ZONE, "
    "destination DEFAULT CURRENT_* values and mirror _deleted_at stamps evaluate in "
    "UTC on this route ({why}). Acknowledge once for this route to continue."
)
UPGRADE_ADVISORY = (
    "An earlier release wrote this Oracle table without a UTC session pin: rows it "
    "loaded from offset or instant columns into TIMESTAMP WITH [LOCAL] TIME ZONE may "
    "hold shifted instants — a full re-run (overwrite) rewrites them correctly."
)


def is_oracle(db_type: Any) -> bool:
    return str(db_type or "").strip().lower() in _ORACLE


# --------------------------------------------------------------------------- #
# The pin and the server-side copy, as bound for this run
# --------------------------------------------------------------------------- #


@contextmanager
def bind_oracle_utc_pin(confirmed: bool, reason: str) -> Iterator[None]:
    pin = _PIN.set((bool(confirmed), str(reason or "")))
    rows = _ROWS_WRITTEN.set([0])
    try:
        yield
    finally:
        _ROWS_WRITTEN.reset(rows)
        _PIN.reset(pin)


def oracle_utc_pin() -> tuple[bool, str]:
    """``(confirmed, reason)`` for this run's Oracle destination session."""
    return _PIN.get() or _UNCHECKED


def oracle_pin_bound() -> bool:
    return _PIN.get() is not None


def mark_server_side_oracle_copy(source_cfg: Mapping[str, Any], dest_cfg: Mapping[str, Any]) -> bool:
    """Record that this run committed to the same-instance ``INSERT ... SELECT``.

    Called by the copy once every decline is behind it. The instance test is
    the copy's own (:func:`services.copy_oracle_oracle.oracle_same_instance`).
    """
    from services.copy_oracle_oracle import oracle_same_instance

    same = oracle_same_instance(dict(source_cfg or {}), dict(dest_cfg or {}))
    if same:
        _SERVER_SIDE.set(True)
    return same


def oracle_same_instance() -> bool:
    """True only after this run committed to the server-side Oracle copy."""
    return _SERVER_SIDE.get()


@contextmanager
def server_side_copy_scope() -> Iterator[None]:
    token = _SERVER_SIDE.set(False)
    try:
        yield
    finally:
        _SERVER_SIDE.reset(token)


def note_oracle_rows_written(count: int) -> None:
    box = _ROWS_WRITTEN.get()
    if box is not None:
        box[0] += max(int(count or 0), 0)


def oracle_rows_written() -> int:
    box = _ROWS_WRITTEN.get()
    return box[0] if box else 0


def assert_writer_session(connection: Any) -> None:
    """Refuse to write on a destination connection that is no longer UTC.

    Only while the run was graded on a confirmed pin (no Risk Contract). The
    check reads the zone on the very connection the batch writes through.
    """
    confirmed, _ = oracle_utc_pin()
    if not confirmed:
        return
    from connectors.oracle_session import confirm_utc_session

    ok, reason = confirm_utc_session(connection)
    if ok:
        return
    done = oracle_rows_written()
    raise RuntimeError(
        f"Oracle destination session is not UTC on this batch ({reason}); Validate graded "
        "this run's instants on a UTC session, so this batch is not written. "
        + (f"{done} row(s) written earlier in this run stay as written (their session read "
           "back UTC). " if done else "Nothing was written in this run. ")
        + "Re-run Validate."
    )


# --------------------------------------------------------------------------- #
# Probing a session
# --------------------------------------------------------------------------- #


def probe_oracle_session(cfg: Mapping[str, Any]) -> tuple[bool, str, str]:
    """``(confirmed, reason, zone before the pin)`` on the endpoint's engine. Never raises."""
    try:
        from connectors.generic_sql import _engine
        from connectors.oracle_session import PRIOR_KEY, confirm_utc_session

        with _engine(dict(cfg)).connect() as conn:
            confirmed, reason = confirm_utc_session(conn)
            prior = str((getattr(conn, "info", None) or {}).get(PRIOR_KEY) or "")
            return confirmed, reason, prior
    except Exception as exc:  # noqa: BLE001 - no session, no confirmed pin
        return False, f"the Oracle session could not be opened ({type(exc).__name__})", ""


def probe_oracle_utc_pin(cfg: Mapping[str, Any]) -> tuple[bool, str]:
    confirmed, reason, _ = probe_oracle_session(cfg)
    return confirmed, reason


def oracle_destination_scope(dest_db: Any, dest_cfg: Mapping[str, Any] | None) -> Any:
    """Bind the pin for an Oracle destination (probe once), else a no-op scope.

    An already bound pin (Execute binds it for the whole run) is kept, so the
    gates inside a run judge the session the writer uses.
    """
    if not is_oracle(dest_db) or oracle_pin_bound():
        return nullcontext()
    if not dest_cfg:
        return bind_oracle_utc_pin(*_UNCHECKED)
    return bind_oracle_utc_pin(*probe_oracle_utc_pin(dest_cfg))


# --------------------------------------------------------------------------- #
# Non-UTC session defaults: one acknowledgement per route
# --------------------------------------------------------------------------- #


def zone_sensitive_route(sync_mode: Any, source_query: Any, source_filter: Any) -> str:
    """Why this route's SQL can read differently in UTC, or ``""``."""
    from services.sync_cursor import normalize_sync_mode

    mode = normalize_sync_mode(str(sync_mode or ""), default="")
    if mode == "cdc":
        return "a CDC route"
    if mode == "mirror":
        return "a mirror route"
    if str(source_query or "").strip():
        return "a custom source query"
    if source_filter:
        return "a source filter"
    return ""


def zone_change_notice(prior_zone: str, why: str) -> str:
    from connectors.oracle_session import session_zone_is_utc

    if not why or not prior_zone or session_zone_is_utc(prior_zone):
        return ""
    return ZONE_CHANGE_NOTICE.format(zone=prior_zone, why=why)


def _ack_path() -> Path:
    from services.platform_config import data_dir

    return data_dir() / "oracle_session_route_acks.jsonl"


_ACK_LOCK = threading.Lock()


def _ack_log() -> Any:
    from services.shared_doc_store import SharedLog

    global _ACKS
    if _ACKS is None:
        _ACKS = SharedLog("oracle_session_route_acks", file_path=lambda: _ack_path(), key_field="route_key")
    return _ACKS


_ACKS: Any = None


def route_key(request: Any) -> str:
    """One route: both endpoints' engine, host, port, database, schema, table, service."""
    def side(ep: Any) -> list[str]:
        extra = getattr(ep, "extra", None) or {}
        keys = ("format", "host", "port", "database", "schema", "table")
        return [str(getattr(ep, k, "") or "").lower() for k in keys] + [
            str(extra.get("service_name") or "").lower()]

    raw = json.dumps([side(request.source), side(request.destination)])
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def _file_acks() -> list[dict[str, Any]]:
    try:
        return [json.loads(line) for line in _ack_path().read_text().splitlines() if line.strip()]
    except (OSError, ValueError):
        return []


def route_acknowledged(key: str) -> bool:
    """A recorded acknowledgement (shared store when configured, else the node's file)."""
    coll = _ack_log().backend()
    if coll is not None:
        return _ack_log().get(coll, key) is not None
    return any(r.get("route_key") == key for r in _file_acks())


def record_route_ack(key: str, *, actor: str, reason: str) -> None:
    record = {"route_key": key, "actor": actor, "reason": reason, "at": datetime.now(timezone.utc).isoformat()}
    coll = _ack_log().backend()
    if coll is not None:
        _ack_log().append(coll, record)
        return
    path = _ack_path()
    with _ACK_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")


def require_route_ack(request: Any, prior_zone: str) -> None:
    """Refuse a zone-sensitive route on a non-UTC default until acknowledged once."""
    extra = getattr(request.source, "extra", None) or {}
    notice = zone_change_notice(prior_zone, zone_sensitive_route(
        getattr(request, "sync_mode", ""), extra.get("source_query"), getattr(request, "source_filter", None)))
    if not notice:
        return
    key = route_key(request)
    if route_acknowledged(key):
        return
    actor = str(getattr(request, "acknowledgment_actor", "") or "").strip()
    reason = str(getattr(request, "acknowledgment_reason", "") or "").strip()
    if getattr(request, "session_zone_acknowledged", False) and len(actor) >= 2 and len(reason) >= 8:
        record_route_ack(key, actor=actor, reason=reason)
        return
    raise ValueError(
        notice + " Set session_zone_acknowledged with acknowledgment_actor and acknowledgment_reason."
    )


# --------------------------------------------------------------------------- #
# Upgrade advisory: rows an earlier release wrote unpinned
# --------------------------------------------------------------------------- #


def _zone_mapping(mappings: Any) -> bool:
    from connectors.oracle_tstz import oracle_tz_kind
    from services.type_system import datetime_timezone_polarity

    for m in mappings or []:
        if not isinstance(m, Mapping):
            continue
        src = str(m.get("source_type") or "")
        tgt = str(m.get("target_type") or "")
        if datetime_timezone_polarity(src) in {"tz", "ltz"} and (
            oracle_tz_kind(tgt) or not tgt or m.get("create_new")
        ):
            return True
    return False


_DONE = frozenset({"completed", "completed_with_warnings", "success", "succeeded"})


def unpinned_history(jobs: Any, dest_table: str) -> bool:
    """A completed job into this Oracle table, written before the pin, that may carry instants.

    A job without :data:`JOB_STAMP` predates the pin. Its mappings decide
    when it recorded them; a job that recorded none may have carried instants.
    """
    for job in jobs or []:
        if not isinstance(job, Mapping) or job.get(JOB_STAMP):
            continue
        req = job.get("transfer_request") or {}
        dest = req.get("destination") or {}
        if not is_oracle(dest.get("format") or dest.get("type")):
            continue
        if str(dest.get("table") or "").lower() != str(dest_table or "").lower():
            continue
        if str(job.get("status") or "").lower() not in _DONE:
            continue
        if not req.get("mappings") or _zone_mapping(req.get("mappings")):
            return True
    return False


def upgrade_advisory(dest_db: Any, dest_table: str, jobs_loader: Callable[[], Any] | None = None) -> str:
    if not is_oracle(dest_db) or not dest_table:
        return ""
    try:
        jobs = jobs_loader() if jobs_loader else _recent_jobs()
    except Exception:  # noqa: BLE001 - history is advisory only
        return ""
    return UPGRADE_ADVISORY if unpinned_history(jobs, dest_table) else ""


def _recent_jobs() -> list[dict[str, Any]]:
    from services.mongodb_service import get_mongodb_service

    store = get_mongodb_service()
    listed = store.list_jobs(limit=200, destination_type="oracle") or []
    ids = [str(j.get("job_id") or j.get("_id") or "") for j in listed]
    full = store.get_jobs([i for i in ids if i]) if hasattr(store, "get_jobs") else {}
    return list((full or {}).values()) or listed


# --------------------------------------------------------------------------- #
# Run and Validate scopes
# --------------------------------------------------------------------------- #


@contextmanager
def oracle_run_scope(request: Any, job_id: str = "") -> Iterator[None]:
    """Execute: probe Oracle sessions, gate a non-UTC default, bind the pin, stamp the job."""
    dest = getattr(request, "destination", None)
    src = getattr(request, "source", None)
    touches = [ep for ep in (src, dest) if ep is not None and is_oracle(getattr(ep, "format", ""))]
    if not touches:
        yield
        return
    from src.transfer.adapters import resolve_connector_config

    workspace = getattr(request, "workspace_id", None)

    def probe(ep: Any) -> tuple[bool, str, str]:
        try:
            cfg = resolve_connector_config(ep, workspace_id=workspace)
        except Exception:  # noqa: BLE001 - an unresolved login leaves the pin unchecked
            return False, _UNCHECKED[1], ""
        return probe_oracle_session(cfg)

    probes = {id(ep): probe(ep) for ep in touches}
    prior = next((p[2] for p in probes.values() if p[2]), "")
    require_route_ack(request, prior)
    if dest is None or not is_oracle(getattr(dest, "format", "")) or oracle_pin_bound():
        yield
        return
    confirmed, reason, _ = probes[id(dest)]
    _stamp_job(job_id, confirmed, reason, prior)
    with bind_oracle_utc_pin(confirmed, reason), server_side_copy_scope():
        yield


def _stamp_job(job_id: str, confirmed: bool, reason: str, prior: str) -> None:
    if not job_id:
        return
    try:
        from services.mongodb_service import get_mongodb_service

        update = getattr(get_mongodb_service(), "update_job_fields", None)
        if callable(update):
            update(job_id, {JOB_STAMP: {"utc_pinned": confirmed, "reason": reason, "default_zone": prior}})
    except Exception:  # noqa: BLE001 - a stamp must not fail the transfer
        pass


def with_oracle_session_pin(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Validate outside a run: judge Oracle instants on the destination's read-back zone.

    Execute binds the pin for the whole run; here the destination session is
    probed, so both grade the same way. Without a connected destination config
    the pin stays unchecked and the Oracle zone pairs keep needing a Risk
    Contract. The upgrade advisory and a non-UTC default's notice ride along
    as warnings.
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        dest_db = kwargs.get("destination_db_type") or ""
        cfg = kwargs.get("destination_config") if kwargs.get("destination_connected") else None
        if not is_oracle(dest_db) or oracle_pin_bound():
            return fn(*args, **kwargs)
        confirmed, reason, prior = probe_oracle_session(cfg) if cfg else (False, _UNCHECKED[1], "")
        with bind_oracle_utc_pin(confirmed, reason):
            result = fn(*args, **kwargs)
        source = kwargs.get("source_config") if isinstance(kwargs.get("source_config"), Mapping) else {}
        notes = [n for n in (
            zone_change_notice(prior, zone_sensitive_route(
                kwargs.get("sync_mode"), (source or {}).get("source_query"), kwargs.get("source_filter"))),
            upgrade_advisory(dest_db, str(kwargs.get("destination_table") or "")),
        ) if n]
        if notes and isinstance(result, dict) and isinstance(result.setdefault("warnings", []), list):
            result["warnings"].extend(notes)
        return result

    return wrapper
