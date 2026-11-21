"""Verification as its own tracked phase after the write (owner decision 2).

When a transfer completes and ``DATAWRAP_PARITY_SCALE`` is on, every table
Gate-8 could not compare in full (above the ladder's row limit) on a supported
SQL engine pair gets a Parity at Scale scan. The job's write status is not
touched; its ``parity_scale`` block tracks the scans::

    {"state": "running" | "passed" | "failed" | "incomplete" | "not_eligible" | "error",
     "started_at", "finished_at", "reason",
     "tables": [{"name", "scan_id", "status", "progress", "evidence", "error"}]}

* ``running`` — the Trust Report says "Verification running"; sign-off,
  cutover and any "proven" claim are blocked until it ends;
* ``passed`` — every table compared in full and matched;
* ``failed`` — a difference was found (the verdict becomes what the
  differences make it, a notification is raised, sign-off stays blocked);
* ``incomplete`` — some unit could not be compared (never a pass).

Scans run on a background worker that holds the job's lease
(:mod:`services.parity_scale.lease`) and checkpoint after every unit into the
job document (``parity_scale_checkpoints``). A worker that dies leaves a
running block whose lease expires: readers then see the verification as
stopped (an error with a plain reason), never "running" forever, and the
sweeper (:func:`sweep_once`, started with the app) resumes it from its
checkpoint. Re-verifying one table starts a fresh scan for it and may take
over a stopped one. Every scan has a wall-clock cap.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import replace
from collections.abc import Callable
from typing import Any

from services.parity_scale import parity_scale_enabled

logger = logging.getLogger(__name__)

RUNNING = "running"
TERMINAL = frozenset({"passed", "failed", "incomplete", "not_eligible", "error"})
CHECKPOINTS = "parity_scale_checkpoints"
#: Keys per difference category kept on the job document (counts are never capped).
JOB_KEYS_CAP = 200
#: A scan stops comparing after this much work: what is left is "not compared" (incomplete).
WALL_SECONDS = 6 * 3600
#: Scans this process runs at once; more wait for a slot (renewing their lease).
MAX_CONCURRENT = 2
#: How long one slice of work runs between heartbeats.
SLICE_SECONDS = 5
_ACTIVE: dict[str, threading.Thread] = {}
_ACTIVE_LOCK = threading.Lock()
_SLOTS = threading.BoundedSemaphore(MAX_CONCURRENT)


# ── job document access ──────────────────────────────────────────────────────


def _service():
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


def _now() -> str:
    from services.parity_scale.store import utc_now

    return utc_now()


def block_of(job: dict[str, Any] | None) -> dict[str, Any]:
    raw = (job or {}).get("parity_scale")
    return raw if isinstance(raw, dict) else {}


def effective_block(job: dict[str, Any] | None) -> dict[str, Any]:
    """The block as readers must see it: a "running" table with no live lease has stopped."""
    from services.parity_scale.lease import STOPPED_REASON, lease_live

    block = block_of(job)
    if block.get("state") != RUNNING or lease_live(job):
        return block
    tables = []
    for table in block.get("tables") or []:
        if isinstance(table, dict) and table.get("status") == RUNNING:
            table = {**table, "status": "error", "error": STOPPED_REASON, "stopped": True}
        tables.append(table)
    out = {**block, "tables": tables}
    out["state"] = _overall(tables) if tables else "error"
    out["reason"] = out.get("reason") or STOPPED_REASON
    return out


def live_state(job: dict[str, Any] | None) -> str | None:
    return effective_block(job).get("state")


def is_running(job: dict[str, Any] | None) -> bool:
    return live_state(job) == RUNNING


def table_block(job: dict[str, Any] | None, name: str) -> dict[str, Any] | None:
    for table in effective_block(job).get("tables") or []:
        if isinstance(table, dict) and str(table.get("name")) == str(name):
            return table
    return None


class JobScanStore:
    """Scan checkpoints on the job document (one field per scan, dotted path)."""

    def __init__(self, job_id: str, token: str = "") -> None:
        self.job_id = job_id
        self.token = token

    def save(self, scan_id: str, state: dict[str, Any]) -> None:
        from services.parity_scale.lease import write_if_held

        if self.token:
            # Only the lease holder checkpoints: a worker that lost the job stops here.
            write_if_held(self.job_id, self.token, {f"{CHECKPOINTS}.{scan_id}": state})
        elif not _service().update_job_fields(
            self.job_id, {f"{CHECKPOINTS}.{scan_id}": state}, touch_updated_at=False
        ):
            raise RuntimeError("the scan checkpoint could not be written (fail closed)")

    def load(self, scan_id: str) -> dict[str, Any] | None:
        job = _service().get_job(self.job_id) or {}
        raw = (job.get(CHECKPOINTS) or {}).get(scan_id)
        return raw if isinstance(raw, dict) else None


# ── what to verify ───────────────────────────────────────────────────────────


def _endpoint_cfg(endpoint: Any) -> tuple[dict[str, Any], str]:
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    cfg = resolve_connector_config(endpoint)
    engine = resolve_driver_type(str(cfg.get("type") or getattr(endpoint, "format", "") or "")).lower()
    return cfg, engine


def table_spec(job: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    """``(spec, "")`` for the job's table, or ``(None, why not)``."""
    from services.parity_evidence import (
        RereadImpossible,
        _endpoints,
        key_columns_of,
        mappings_of,
        protected_targets,
        request_of,
        table_name_of,
        target_to_source,
    )
    from services.parity_scale.plan import SUPPORTED_PAIRS, normalize_engine

    try:
        _req, source, destination = _endpoints(job)
    except RereadImpossible as exc:
        return None, str(exc)
    src_cfg, src_engine = _endpoint_cfg(source)
    dst_cfg, dst_engine = _endpoint_cfg(destination)
    pair = (normalize_engine(src_engine), normalize_engine(dst_engine))
    if pair not in SUPPORTED_PAIRS or pair[0] == "sqlite":
        return None, f"no in-engine comparison for {src_engine} to {dst_engine} yet"
    mappings = mappings_of(job)
    if not mappings:
        return None, "the job has no column map"
    t2s = target_to_source(job)
    key_targets = key_columns_of(job)
    return {
        "name": table_name_of(job),
        "source": {"engine": pair[0], "cfg": src_cfg, "table": str(source.table or source.collection or ""),
                   "schema": str(getattr(source, "schema", "") or "")},
        "destination": {"engine": pair[1], "cfg": dst_cfg, "table": str(destination.table or destination.collection or ""),
                        "schema": str(getattr(destination, "schema", "") or "")},
        "mappings": mappings,
        "key_columns": [t2s.get(k, k) for k in key_targets],
        "error_policy": str(request_of(job).get("error_policy") or "quarantine"),
        # Values of these never reach the job document (examples, causes, keys of a protected key).
        "protected": sorted(str(c).lower() for c in protected_targets(job)),
        "key_protected": any(str(k).lower() in {str(c).lower() for c in protected_targets(job)}
                             for k in key_targets),
    }, ""


def needs_scale_verification(job: dict[str, Any]) -> bool:
    """Gate-8 could not compare every row (above the ladder limit)."""
    recon = job.get("reconciliation") if isinstance(job.get("reconciliation"), dict) else {}
    return bool(recon.get("coverage_limit")) or recon.get("verification_verdict_qualifier") == "coverage_limited"


# ── connections and catalog types ────────────────────────────────────────────


def connect(engine: str, cfg: dict[str, Any]) -> Any:
    """A DB-API connection (``%s`` parameters) to one side."""
    host = str(cfg.get("host") or "")
    port = int(cfg.get("port") or 0)
    database = str(cfg.get("database") or "")
    user = str(cfg.get("username") or "")
    password = str(cfg.get("password") or "")
    if engine == "postgresql":
        from connectors.postgresql_conn import get_connection

        return get_connection(host=host, port=port or 5432, database=database, username=user, password=password,
                              connection_string=str(cfg.get("connection_string") or ""),
                              ssl=bool(cfg.get("ssl", False)))
    if engine == "mysql":
        from connectors.mysql_conn import get_connection

        return get_connection(host=host, port=port or 3306, database=database, username=user, password=password,
                              connection_string=str(cfg.get("connection_string") or ""),
                              ssl=bool(cfg.get("ssl", False)), purpose="introspect")
    if engine == "sqlserver":
        # The connector's own driver selection, TLS options and egress check
        # (the SQL Server reader's connection builder), never a bare pymssql dial.
        from connectors.generic_sql import connection_options, get_connection

        from services.egress_guard import current_egress_workspace

        return get_connection(host=host, port=port or 1433, database=database, username=user, password=password,
                              connection_string=str(cfg.get("connection_string") or ""),
                              ssl=bool(cfg.get("ssl", False)), db_type="sqlserver",
                              workspace_id=current_egress_workspace(), **connection_options(cfg))
    raise ValueError(f"no verification connection for {engine}")


def catalog_types(engine: str, conn: Any, schema: str, table: str) -> dict[str, str]:
    """``{column: declared type}`` exactly as the catalog spells it (precision included)."""
    with conn.cursor() as cur:
        if engine == "postgresql":
            cur.execute(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
                "JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.relname = %s AND n.nspname = %s AND a.attnum > 0 AND NOT a.attisdropped",
                (table, schema or "public"),
            )
        elif engine == "mysql":
            cur.execute(
                "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = COALESCE(NULLIF(%s, ''), DATABASE()) AND TABLE_NAME = %s",
                (schema or "", table),
            )
        else:
            cur.execute(
                "SELECT c.name, t.name, c.max_length, c.precision, c.scale FROM sys.columns c "
                "JOIN sys.types t ON t.user_type_id = c.user_type_id "
                "WHERE c.object_id = OBJECT_ID(%s)",
                (f"[{schema or 'dbo'}].[{table}]",),
            )
        rows = cur.fetchall()
    conn.commit()
    if engine != "sqlserver":
        return {str(r[0]): str(r[1]) for r in rows}
    out: dict[str, str] = {}
    for name, typ, max_length, precision, scale in rows:
        typ = str(typ).lower()
        if typ in {"decimal", "numeric"}:
            out[str(name)] = f"{typ}({precision},{scale})"
        elif typ in {"datetime2", "datetimeoffset", "time"}:
            out[str(name)] = f"{typ}({scale})"
        elif typ in {"varchar", "char", "varbinary", "binary"}:
            out[str(name)] = f"{typ}({'max' if max_length == -1 else max_length})"
        elif typ in {"nvarchar", "nchar"}:
            out[str(name)] = f"{typ}({'max' if max_length == -1 else max_length // 2})"
        else:
            out[str(name)] = typ
    return out


def _types_of(side: dict[str, Any]) -> dict[str, str]:
    conn = connect(side["engine"], side["cfg"])
    try:
        return catalog_types(side["engine"], conn, side["schema"], side["table"])
    finally:
        conn.close()


def plan_for(spec: dict[str, Any]) -> Any:
    from services.parity_scale.plan import build_plan

    src, dst = spec["source"], spec["destination"]
    s_types = _types_of(src)
    d_types = _types_of(dst)
    return build_plan(spec["mappings"], source_types=s_types, dest_types=d_types,
                      source_engine=src["engine"], dest_engine=dst["engine"],
                      key_columns=spec["key_columns"], error_policy=spec["error_policy"])


def side_opener(spec: dict[str, Any]) -> Callable[[str, Any], Any]:
    from services.parity_scale.dialects import open_side

    def open_(role: str, plan: Any) -> Any:
        side = spec["source"] if role == "source" else spec["destination"]
        kwargs: dict[str, Any] = {"role": role, "plan": plan, "table": side["table"],
                                  "connection": connect(side["engine"], side["cfg"])}
        if side["schema"] or side["engine"] in {"postgresql", "sqlserver"}:
            kwargs["schema"] = side["schema"] or {"postgresql": "public", "sqlserver": "dbo"}[side["engine"]]
        return open_side(side["engine"], **kwargs)

    return open_


# ── evidence kept on the job ─────────────────────────────────────────────────


def job_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    """The scan's evidence with key lists capped for the job document."""
    out = dict(evidence)
    diff = dict(out.get("differences") or {})
    for name in ("missing_keys", "extra_keys", "changed_keys", "duplicate_key_values", "examples"):
        if isinstance(diff.get(name), list) and len(diff[name]) > JOB_KEYS_CAP:
            diff[name] = diff[name][:JOB_KEYS_CAP]
            diff["keys_capped"] = True
    out["differences"] = diff
    return out


def _overall(tables: list[dict[str, Any]]) -> str:
    if any(t.get("status") == RUNNING for t in tables):
        return RUNNING
    outcomes = [str((t.get("evidence") or {}).get("outcome") or "") for t in tables]
    if any(t.get("status") == "error" for t in tables):
        return "error"
    if any(o == "differences_found" for o in outcomes):
        return "failed"
    if any(o != "match" for o in outcomes):
        return "incomplete"
    return "passed"


def table_state(table: dict[str, Any]) -> str:
    """One table's verification in the block's words: running, passed, failed, incomplete or error."""
    return _overall([table])


# ── the runner ───────────────────────────────────────────────────────────────


def _workspace_of(job: dict[str, Any]) -> str:
    return str(job.get("workspace_id") or "")


def _register(job_id: str, thread: threading.Thread) -> bool:
    """Check and register this process's worker for the job in one step."""
    with _ACTIVE_LOCK:
        running = _ACTIVE.get(job_id)
        if running is not None and running is not thread and (running.is_alive() or not running.ident):
            return False
        _ACTIVE[job_id] = thread
        return True


def _unregister(job_id: str, thread: threading.Thread) -> None:
    with _ACTIVE_LOCK:
        if _ACTIVE.get(job_id) is thread:
            _ACTIVE.pop(job_id, None)


def start_for_job(
    job_id: str,
    *,
    only_table: str | None = None,
    background: bool = True,
    force: bool = False,
    config: Any = None,
) -> dict[str, Any]:
    """Start (or restart one table of) the job's verification; returns the block as readers see it.

    Off unless ``DATAWRAP_PARITY_SCALE`` is on. Without ``force`` a job is
    verified only when Gate-8 could not compare every row, and only once (a
    job that already has a verification keeps it). A run held by another live
    worker is left alone; a stopped one is taken over.
    """
    from services.parity_scale.lease import claim, release, write_if_held
    from services.parity_scale.watermark import release_cursor

    if not parity_scale_enabled():
        return {}
    job = _service().get_job(job_id) or {}
    if not job:
        return {}
    if not force and not only_table:
        if block_of(job):
            return effective_block(job)
        if not needs_scale_verification(job):
            return {}
    if is_running(job):
        return effective_block(job)
    spec, why = table_spec(job)
    if spec is not None and only_table and only_table != spec["name"]:
        raise LookupError(f"the job wrote no table named {only_table!r}")
    holder: dict[str, tuple[Any, ...]] = {}
    worker = threading.Thread(target=lambda: _run_guarded(*holder["work"]), name=f"parity-verify-{job_id}",
                              daemon=True)
    if not _register(job_id, worker):
        return effective_block(job)
    token = claim(job_id)
    if token is None:
        _unregister(job_id, worker)
        return effective_block(_service().get_job(job_id))
    try:
        if spec is None:
            block = {"state": "not_eligible", "reason": why, "tables": [], "finished_at": _now()}
            write_if_held(job_id, token, {"parity_scale": block})
            # Nothing to wait for: Gate-8's own record decides; a parked watermark moves.
            release_cursor(job_id, _service().get_job(job_id) or {}, "passed")
            release(job_id, token)
            _unregister(job_id, worker)
            return block
        import uuid

        scan_id = f"psd1-{uuid.uuid4().hex[:16]}"
        block = dict(block_of(job))
        tables = [t for t in block.get("tables") or [] if t.get("name") != spec["name"]]
        tables.append({"name": spec["name"], "scan_id": scan_id, "status": RUNNING, "progress": {},
                       "started_at": _now(), "evidence": None, "error": None})
        block.update({"state": RUNNING, "started_at": _now(), "finished_at": None, "tables": tables, "reason": ""})
        # One scan's checkpoints at a time: a restart drops the previous scan's.
        write_if_held(job_id, token, {"parity_scale": block, CHECKPOINTS: {}})
    except BaseException:
        release(job_id, token)
        _unregister(job_id, worker)
        raise
    work = (job_id, spec, scan_id, config, False, token, _workspace_of(job), worker)
    if background:
        holder["work"] = work
        worker.start()
    else:
        _run_guarded(*work)
    return effective_block(_service().get_job(job_id))


def _take_slot(job_id: str, token: str) -> None:
    """Wait for one of :data:`MAX_CONCURRENT` slots, keeping the lease alive meanwhile."""
    from services.parity_scale.lease import renew

    while not _SLOTS.acquire(timeout=30):
        renew(job_id, token)


def _run_guarded(job_id: str, spec: dict[str, Any], scan_id: str, config: Any, resume: bool,
                 token: str, workspace_id: str, worker: threading.Thread | None = None) -> None:
    from services.egress_guard import egress_workspace
    from services.parity_scale.lease import LeaseLost, release

    slot = False
    try:
        # Connector dials are judged by the job's workspace allowlist, as the run's were.
        with egress_workspace(workspace_id):
            _take_slot(job_id, token)
            slot = True
            _run(job_id, spec, scan_id, config, resume, token)
    except LeaseLost:
        logger.warning("Parity at Scale verification of job %s was taken over by another worker", job_id)
    except Exception as exc:  # noqa: BLE001 — a failed verification is recorded, never raised into the job
        logger.exception("Parity at Scale verification failed for job %s", job_id)
        # The exception text can name hosts or SQL: it goes to the log, the report gets a sentence.
        try:
            _finish_table(job_id, spec["name"], token, status="error",
                          error=f"The comparison stopped ({type(exc).__name__}). Re-verify the table to try again.")
        except LeaseLost:
            pass
    finally:
        if slot:
            _SLOTS.release()
        release(job_id, token)
        if worker is not None:
            _unregister(job_id, worker)


def _run(job_id: str, spec: dict[str, Any], scan_id: str, config: Any, resume: bool, token: str) -> None:
    from services.parity_scale.driver import ParityScan, ScanConfig
    from services.parity_scale.lease import renew

    store = JobScanStore(job_id, token)
    plan = plan_for(spec)
    opener = side_opener(spec)
    cfg = config or ScanConfig(keys_stored_cap=2000, max_wall_seconds=WALL_SECONDS)
    cfg = replace(cfg, protected=tuple(spec.get("protected") or ()))
    if spec.get("key_protected"):
        # The key itself is protected: no key value is stored; rows are counted, not named.
        # Hash buckets (mode H) keep no key value in the queue either, unlike key-range bounds.
        cfg = replace(cfg, keys_stored_cap=0, examples_cap=0, mode="H")
    if resume:
        scan = ParityScan.resume(scan_id, store=store, open_side=opener, plan=plan)
    else:
        scan = ParityScan.start(plan, store=store, open_side=opener, config=cfg, scan_id=scan_id)
    try:
        while not scan.done:
            progress = scan.run(max_seconds=SLICE_SECONDS)
            renew(job_id, token)
            _update_table(job_id, spec["name"], token, progress=progress)
        evidence = job_evidence(scan.result())
    finally:
        scan.close()
    _finish_table(job_id, spec["name"], token, status="done", evidence=evidence)


def _update_table(job_id: str, name: str, token: str, **fields: Any) -> None:
    from services.parity_scale.lease import write_if_held

    block = dict(block_of(_service().get_job(job_id)))
    for table in block.get("tables") or []:
        if table.get("name") == name:
            table.update(fields)
    write_if_held(job_id, token, {"parity_scale": block})


def _finish_table(job_id: str, name: str, token: str, *, status: str, evidence: dict[str, Any] | None = None,
                  error: str | None = None) -> None:
    from services.parity_scale.lease import write_if_held
    from services.parity_scale.watermark import release_cursor

    job = _service().get_job(job_id) or {}
    block = dict(block_of(job))
    scan_id = ""
    for table in block.get("tables") or []:
        if table.get("name") == name:
            scan_id = str(table.get("scan_id") or "")
            table.update({"status": status, "evidence": evidence, "error": error, "finished_at": _now(),
                          "progress": {k: v for k, v in (table.get("progress") or {}).items()
                                       if k in {"items_done", "items_queued", "elapsed_s", "mode"}}})
    block["state"] = _overall(block.get("tables") or [])
    if block["state"] != RUNNING:
        block["finished_at"] = _now()
    # The finished scan's checkpoint is not kept: the evidence is the record.
    write_if_held(job_id, token, {"parity_scale": block, CHECKPOINTS: {}})
    if evidence:
        # Design §4.2: the scan joins the job's reconciliation (signed with the
        # proof pack) beside Gate-8's own record, which is never edited.
        coverage = evidence.get("coverage") or {}
        diff = evidence.get("differences") or {}
        write_if_held(job_id, token, {"reconciliation.parity_at_scale": {
            "algorithm": evidence.get("algorithm"), "ce_version": evidence.get("ce_version"),
            "fingerprint_version": evidence.get("fingerprint_version"), "hash": evidence.get("hash"),
            "check_kind": evidence.get("check_kind"), "mode": evidence.get("mode"),
            "plan_hash": evidence.get("plan_hash"), "scan_id": evidence.get("scan_id") or scan_id,
            "outcome": evidence.get("outcome"), "complete": bool(coverage.get("complete")),
            "certified": bool((evidence.get("certification") or {}).get("all_certified")),
            "certification_pair": (evidence.get("certification") or {}).get("pair"),
            "consistency": evidence.get("consistency"),
            "rows_source": (evidence.get("population") or {}).get("rows_source"),
            "rows_destination": (evidence.get("population") or {}).get("rows_destination"),
            "missing_in_destination": diff.get("missing_in_destination"),
            "extra_in_destination": diff.get("extra_in_destination"),
            "changed_rows": diff.get("changed_rows"),
            "table": name, "finished_at": _now(),
        }})
    if block["state"] in {"failed", "incomplete", "error"}:
        _notify(job_id, job, block)
    release_cursor(job_id, _service().get_job(job_id) or {}, block["state"])


def _notify(job_id: str, job: dict[str, Any], block: dict[str, Any]) -> None:
    """A verification that found a difference raises an operator event; the write status is not rewritten."""
    from services.ops_events import emit_event

    emit_event(
        logger,
        "parity.verification.finished",
        outcome="failed",
        level=logging.WARNING,
        job_id=str(job_id),
        workspace_id=str(job.get("workspace_id") or "") or None,
        verification_state=str(block.get("state")),
    )


# ── stopped verifications: resume or say so ──────────────────────────────────


def _stop_for_good(job_id: str, token: str, name: str, reason: str) -> None:
    from services.parity_scale.lease import release

    try:
        _finish_table(job_id, name, token, status="error", error=reason)
    finally:
        release(job_id, token)


def resume_job(job_id: str, *, background: bool = True) -> bool:
    """Resume one stopped verification from its checkpoint; ``True`` when a worker took it."""
    from services.parity_scale.lease import claim

    job = _service().get_job(job_id) or {}
    if block_of(job).get("state") != RUNNING or is_running(job):
        return False  # finished, or a live worker holds it
    holder: dict[str, tuple[Any, ...]] = {}
    worker = threading.Thread(target=lambda: _run_guarded(*holder["work"]), name=f"parity-verify-{job_id}",
                              daemon=True)
    if not _register(job_id, worker):
        return False
    token = claim(job_id)
    if token is None:
        _unregister(job_id, worker)
        return False
    spec, why = table_spec(job)
    table = next((t for t in block_of(job).get("tables") or [] if t.get("status") == RUNNING), None)
    checkpoint = (job.get(CHECKPOINTS) or {}).get(str((table or {}).get("scan_id") or ""))
    if spec is None or table is None or table.get("name") != spec["name"] or not isinstance(checkpoint, dict):
        name = str((table or {}).get("name") or (spec or {}).get("name") or "")
        _stop_for_good(job_id, token, name,
                       "Verification stopped before it finished and could not resume"
                       + (f" ({why})" if why else "") + ". Re-verify the table to compare every row.")
        _unregister(job_id, worker)
        return False
    work = (job_id, spec, str(table["scan_id"]), None, True, token, _workspace_of(job), worker)
    if background:
        holder["work"] = work
        worker.start()
    else:
        _run_guarded(*work)
    return True


def sweep_once(limit: int = 50, *, background: bool = True) -> int:
    """Resume every verification whose worker stopped (lease expired); returns how many resumed.

    Reads full job documents (the checkpoints are on them), not the job list's slim rows.
    """
    if not parity_scale_enabled():
        return 0
    resumed = 0
    for job_id in _service().list_job_ids_where({"parity_scale.state": RUNNING}, limit=limit):
        try:
            resumed += bool(resume_job(job_id, background=background))
        except Exception:  # noqa: BLE001 — one bad job must not stop the sweep
            logger.exception("Parity at Scale could not resume the verification of job %s", job_id)
    return resumed


resume_unfinished = sweep_once

_sweeper_stop: threading.Event | None = None
_sweeper_thread: threading.Thread | None = None


def start_sweeper(interval_seconds: float = 60.0) -> bool:
    """Run :func:`sweep_once` now and every ``interval_seconds`` on a daemon thread."""
    global _sweeper_stop, _sweeper_thread
    if not parity_scale_enabled():
        return False
    if _sweeper_thread is not None and _sweeper_thread.is_alive():
        return True
    stop = threading.Event()
    _sweeper_stop = stop

    def _loop() -> None:
        while not stop.is_set():
            try:
                sweep_once()
            except Exception:  # noqa: BLE001 — the sweep runs again next interval
                logger.exception("Parity at Scale verification sweep failed")
            stop.wait(max(5.0, float(interval_seconds)))

    _sweeper_thread = threading.Thread(target=_loop, name="df-parity-verify-sweep", daemon=True)
    _sweeper_thread.start()
    return True


def stop_sweeper(timeout: float = 5.0) -> None:
    global _sweeper_thread
    if _sweeper_stop is not None:
        _sweeper_stop.set()
    if _sweeper_thread is not None:
        _sweeper_thread.join(max(0.0, timeout))
        _sweeper_thread = None


# ── what the verification blocks ─────────────────────────────────────────────


def blocking_reason(job: dict[str, Any] | None) -> str:
    """Why sign-off and cutover must wait for verification, or ``""``."""
    state = live_state(job)
    if state == RUNNING:
        return "Verification is still running; sign once every row has been compared."
    if state == "error":
        return "Verification did not finish; re-verify the table, then sign."
    return ""


# Kept for callers of the phase-3 names.
def hold_cursor_until_verified(*args: Any, **kwargs: Any) -> Any:
    from services.parity_scale.watermark import hold_cursor_until_verified as hold

    return hold(*args, **kwargs)


def schedule_waits_for_verification(sched: Any) -> str:
    from services.parity_scale.watermark import schedule_waits_for_verification as waits

    return waits(sched)
