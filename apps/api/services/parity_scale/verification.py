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

Scans run on a background worker and checkpoint after every unit into the job
document (``parity_scale_checkpoints``), so a restarted API resumes them
(:func:`resume_unfinished`). Re-verifying one table starts a fresh scan for it.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

from services.parity_scale import parity_scale_enabled

logger = logging.getLogger(__name__)

RUNNING = "running"
TERMINAL = frozenset({"passed", "failed", "incomplete", "not_eligible", "error"})
CHECKPOINTS = "parity_scale_checkpoints"
#: Keys per difference category kept on the job document (counts are never capped).
JOB_KEYS_CAP = 200
_ACTIVE: dict[str, threading.Thread] = {}
_ACTIVE_LOCK = threading.Lock()


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


def is_running(job: dict[str, Any] | None) -> bool:
    return block_of(job).get("state") == RUNNING


def table_block(job: dict[str, Any] | None, name: str) -> dict[str, Any] | None:
    for table in block_of(job).get("tables") or []:
        if isinstance(table, dict) and str(table.get("name")) == str(name):
            return table
    return None


def _save_block(job_id: str, block: dict[str, Any]) -> None:
    _service().update_job_fields(job_id, {"parity_scale": block}, touch_updated_at=False)


class JobScanStore:
    """Scan checkpoints on the job document (one field per scan, dotted path)."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id

    def save(self, scan_id: str, state: dict[str, Any]) -> None:
        if not _service().update_job_fields(
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
        import pymssql

        return pymssql.connect(server=host, port=port or 1433, database=database, user=user, password=password,
                               autocommit=False, login_timeout=30)
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


def plan_for(spec: dict[str, Any]) -> Any:
    from services.parity_scale.plan import build_plan

    src, dst = spec["source"], spec["destination"]
    s_conn, d_conn = connect(src["engine"], src["cfg"]), connect(dst["engine"], dst["cfg"])
    try:
        s_types = catalog_types(src["engine"], s_conn, src["schema"], src["table"])
        d_types = catalog_types(dst["engine"], d_conn, dst["schema"], dst["table"])
    finally:
        s_conn.close()
        d_conn.close()
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


def start_for_job(
    job_id: str,
    *,
    only_table: str | None = None,
    background: bool = True,
    force: bool = False,
    config: Any = None,
) -> dict[str, Any]:
    """Start (or restart one table of) the job's verification; returns the block.

    Off unless ``DATAWRAP_PARITY_SCALE`` is on. Without ``force`` a job is
    verified only when Gate-8 could not compare every row.
    """
    if not parity_scale_enabled():
        return {}
    job = _service().get_job(job_id) or {}
    if not job:
        return {}
    if not force and not only_table and not needs_scale_verification(job):
        return block_of(job)
    with _ACTIVE_LOCK:
        if job_id in _ACTIVE and _ACTIVE[job_id].is_alive():
            return block_of(job)
    spec, why = table_spec(job)
    if spec is None:
        block = {"state": "not_eligible", "reason": why, "tables": [], "finished_at": _now()}
        _save_block(job_id, block)
        # Nothing to wait for: Gate-8's own record decides; a parked watermark moves.
        _release_cursor(job_id, _service().get_job(job_id) or {}, "passed")
        return block
    if only_table and only_table != spec["name"]:
        raise LookupError(f"the job wrote no table named {only_table!r}")
    import uuid

    scan_id = f"psd1-{uuid.uuid4().hex[:16]}"
    block = dict(block_of(job))
    tables = [t for t in block.get("tables") or [] if t.get("name") != spec["name"]]
    tables.append({"name": spec["name"], "scan_id": scan_id, "status": RUNNING, "progress": {},
                   "started_at": _now(), "evidence": None, "error": None})
    block.update({"state": RUNNING, "started_at": block.get("started_at") or _now(), "finished_at": None,
                  "tables": tables, "reason": ""})
    _save_block(job_id, block)
    work = (job_id, spec, scan_id, config, False)
    if background:
        thread = threading.Thread(target=_run_guarded, args=work, name=f"parity-verify-{job_id}", daemon=True)
        with _ACTIVE_LOCK:
            _ACTIVE[job_id] = thread
        thread.start()
    else:
        _run_guarded(*work)
    return block_of(_service().get_job(job_id))


def _run_guarded(job_id: str, spec: dict[str, Any], scan_id: str, config: Any, resume: bool) -> None:
    try:
        _run(job_id, spec, scan_id, config, resume)
    except Exception as exc:  # noqa: BLE001 — a failed verification is recorded, never raised into the job
        logger.exception("Parity at Scale verification failed for job %s", job_id)
        # The exception text can name hosts or SQL: it goes to the log, the report gets a sentence.
        _finish_table(job_id, spec["name"], status="error",
                      error=f"The comparison stopped ({type(exc).__name__}). Re-verify the table to try again.")
    finally:
        with _ACTIVE_LOCK:
            _ACTIVE.pop(job_id, None)


def _run(job_id: str, spec: dict[str, Any], scan_id: str, config: Any, resume: bool) -> None:
    from services.parity_scale.driver import ParityScan, ScanConfig

    store = JobScanStore(job_id)
    plan = plan_for(spec)
    opener = side_opener(spec)
    cfg = config or ScanConfig(keys_stored_cap=2000)
    if resume and store.load(scan_id) is not None:
        scan = ParityScan.resume(scan_id, store=store, open_side=opener, plan=plan)
    else:
        scan = ParityScan.start(plan, store=store, open_side=opener, config=cfg, scan_id=scan_id)
    while not scan.done:
        progress = scan.run(max_seconds=5)
        _update_table(job_id, spec["name"], progress=progress)
    _finish_table(job_id, spec["name"], status="done", evidence=job_evidence(scan.result()))


def _update_table(job_id: str, name: str, **fields: Any) -> None:
    job = _service().get_job(job_id) or {}
    block = dict(block_of(job))
    for table in block.get("tables") or []:
        if table.get("name") == name:
            table.update(fields)
    _save_block(job_id, block)


def _finish_table(job_id: str, name: str, *, status: str, evidence: dict[str, Any] | None = None,
                  error: str | None = None) -> None:
    job = _service().get_job(job_id) or {}
    block = dict(block_of(job))
    for table in block.get("tables") or []:
        if table.get("name") == name:
            table.update({"status": status, "evidence": evidence, "error": error, "finished_at": _now()})
    block["state"] = _overall(block.get("tables") or [])
    if block["state"] != RUNNING:
        block["finished_at"] = _now()
    _save_block(job_id, block)
    if evidence:
        # Design §4.2: the scan joins the job's reconciliation (signed with the
        # proof pack) beside Gate-8's own record, which is never edited.
        coverage = evidence.get("coverage") or {}
        diff = evidence.get("differences") or {}
        _service().update_job_fields(job_id, {"reconciliation.parity_at_scale": {
            "algorithm": evidence.get("algorithm"), "ce_version": evidence.get("ce_version"),
            "fingerprint_version": evidence.get("fingerprint_version"), "hash": evidence.get("hash"),
            "check_kind": evidence.get("check_kind"), "mode": evidence.get("mode"),
            "plan_hash": evidence.get("plan_hash"), "scan_id": evidence.get("scan_id"),
            "outcome": evidence.get("outcome"), "complete": bool(coverage.get("complete")),
            "certified": bool((evidence.get("certification") or {}).get("all_certified")),
            "certification_pair": (evidence.get("certification") or {}).get("pair"),
            "rows_source": (evidence.get("population") or {}).get("rows_source"),
            "rows_destination": (evidence.get("population") or {}).get("rows_destination"),
            "missing_in_destination": diff.get("missing_in_destination"),
            "extra_in_destination": diff.get("extra_in_destination"),
            "changed_rows": diff.get("changed_rows"),
            "table": name, "finished_at": _now(),
        }}, touch_updated_at=False)
    if block["state"] in {"failed", "incomplete", "error"}:
        _notify(job_id, job, block)
    _release_cursor(job_id, _service().get_job(job_id) or {}, block["state"])


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


def resume_unfinished(limit: int = 50) -> int:
    """Resume running verifications after a restart (their checkpoints are on the jobs)."""
    if not parity_scale_enabled():
        return 0
    resumed = 0
    jobs = _service().list_jobs(limit=limit)
    for job in jobs or []:
        if not is_running(job):
            continue
        job_id = str(job.get("_id") or job.get("id") or job.get("job_id") or "")
        spec, _why = table_spec(job)
        if spec is None:
            continue
        for table in block_of(job).get("tables") or []:
            if table.get("status") == RUNNING and table.get("name") == spec["name"]:
                thread = threading.Thread(target=_run_guarded,
                                          args=(job_id, spec, table["scan_id"], None, True), daemon=True)
                with _ACTIVE_LOCK:
                    if job_id in _ACTIVE and _ACTIVE[job_id].is_alive():
                        continue
                    _ACTIVE[job_id] = thread
                thread.start()
                resumed += 1
    return resumed


# ── what the verification blocks ─────────────────────────────────────────────


def blocking_reason(job: dict[str, Any] | None) -> str:
    """Why sign-off, cutover and "proven" must wait, or ``""``."""
    state = block_of(job).get("state")
    if state == RUNNING:
        return "Verification is still running; sign-off and cutover wait until it finishes."
    if state in {"failed", "incomplete", "error"}:
        return {
            "failed": "Verification found differences.",
            "incomplete": "Verification could not compare every row.",
            "error": "Verification could not finish.",
        }[state]
    return ""


# ── schedules: wait for verification, and the watermark ─────────────────────


def _verification_pending(job: dict[str, Any] | None) -> bool:
    """Running, or about to start (the beat can finalize before the terminal hook ran)."""
    if is_running(job):
        return True
    return parity_scale_enabled() and not block_of(job) and needs_scale_verification(job or {})


def hold_cursor_until_verified(schedule_id: str, job_id: str, job_doc: dict[str, Any] | None,
                               cursor_value: Any) -> Any:
    """The watermark the schedule may advance to now (``None`` = keep the old one).

    Owner decision 2: an incremental watermark advances only once verification
    has not failed. A run still being verified parks its watermark on the job;
    :func:`_release_cursor` applies it when the verification passes. A failed or
    incomplete verification never advances it, so the next run re-reads the
    same window.
    """
    if cursor_value in (None, ""):
        return cursor_value
    state = block_of(job_doc).get("state")
    if _verification_pending(job_doc):
        _service().update_job_fields(job_id, {"parity_scale_pending_cursor": {
            "schedule_id": schedule_id, "cursor_value": str(cursor_value)}}, touch_updated_at=False)
        return None
    if state in {"failed", "incomplete", "error"}:
        return None
    return cursor_value


def _release_cursor(job_id: str, job: dict[str, Any], state: str) -> None:
    pending = job.get("parity_scale_pending_cursor")
    if not isinstance(pending, dict) or state == RUNNING:
        return
    _service().update_job_fields(job_id, {"parity_scale_pending_cursor": None}, touch_updated_at=False)
    if state != "passed":
        return
    from services.schedule_store import get_schedule, update_schedule

    sched = get_schedule(str(pending.get("schedule_id") or ""))
    # Only the run that is still the schedule's latest moves its watermark.
    if sched is None or str(sched.last_job_id or "") != str(job_id):
        return
    update_schedule(sched.id, {"cursor_value": str(pending.get("cursor_value") or "")})


def schedule_waits_for_verification(sched: Any) -> str:
    """Why this tick waits for the previous tick's verification, or ``""``."""
    if not parity_scale_enabled() or getattr(sched, "wait_for_verification", True) is False:
        return ""
    last = str(getattr(sched, "last_job_id", "") or "")
    if not last:
        return ""
    job = _service().get_job(last) or {}
    if not is_running(job):
        return ""
    return "Waiting for the previous run's verification to finish before the next run starts."
