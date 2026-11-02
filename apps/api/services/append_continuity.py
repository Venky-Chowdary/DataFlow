"""Destination continuity between verified runs (X-97).

Gate-8 proves the rows *this* append wrote, so the rows a prior run already
proved sit outside its scope. A tampered prior population (changed value,
deleted row, stray row) therefore passed every append check.

After each successful run we store an engine-side fingerprint of the whole
destination table (row count + canonical digest). Before the next append
writes anything, the destination is re-read and must match that fingerprint.
A mismatch fails the run before any write, so the destination is left in
exactly its pre-run state. Overwrite/upsert runs re-baseline; other modes drop
the baseline so a later append never compares against a stale state.
"""

from __future__ import annotations

import logging
import os
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from services.atomic_file import write_json_atomic
from services.platform_config import data_dir

logger = logging.getLogger(__name__)

STORE_FILENAME = "append_continuity.json"
REBASELINE_ENDPOINT = "POST /api/v1/transfer/continuity/rebaseline"
#: Both doors to the same audited action — the Pilot/MCP tool and the REST
#: route. Naming only the route left MCP operators hand-crafting HTTP (D100).
REBASELINE_REMEDY = f"MCP tool rebaseline_transfer, or {REBASELINE_ENDPOINT}"
RECOVERY_HINT = (
    "To recover: investigate the change. If the current destination is "
    f"correct, accept it as the new baseline ({REBASELINE_REMEDY}) "
    "(audited; a reason is required), or re-run with full_refresh_overwrite "
    "to rebuild it from the source."
)


MISSING_RECOVERY_HINT = (
    "To recover: if the table was removed on purpose, accept the removal "
    f"({REBASELINE_REMEDY}; audited, a reason is required) so the next append "
    "starts a new verified history, or re-run with full_refresh_overwrite to "
    "rebuild it from the source."
)

_FILE_ENGINES = frozenset({"sqlite", "duckdb"})
#: Engines whose catalog the shared SQLAlchemy object-identity probe can read.
_SQL_PROBE_ENGINES = frozenset({"sqlite", "postgresql", "mysql", "mariadb", "sqlserver", "mssql"})


def store_path() -> Path:
    """Baseline file under the shared ``DATAFLOW_DATA_DIR`` resolver."""
    return data_dir() / STORE_FILENAME
COLLECTION = "append_continuity"

CONTINUITY_ENGINES = frozenset(
    {
        "sqlite",
        "postgresql",
        "mysql",
        "mariadb",
        "mongodb",
        "sqlserver",
        "mssql",
        "azure_sql",
        "duckdb",
        "sftp",
        "s3",
    }
)

_lock = threading.Lock()


@dataclass(frozen=True)
class ContinuityTarget:
    db_type: str
    cfg: dict[str, Any]
    schema: str
    table: str
    key: str
    path: str = ""


class ContinuityStoreCorrupt(RuntimeError):
    """The baseline store exists but cannot be read as a baseline map."""


@dataclass(frozen=True)
class ContinuityViolation:
    message: str
    details: dict[str, Any] = field(default_factory=dict)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dsn_identity(connection_string: str) -> str:
    """Host/port/path of a DSN — never its credentials."""
    raw = (connection_string or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
        host = parts.hostname or ""
        port = parts.port or ""
        return f"{parts.scheme}://{host}:{port}{parts.path}"
    except ValueError:
        return ""


def resolve_target(endpoint: Any) -> ContinuityTarget | None:
    """Destination identity + read config, or ``None`` when not covered."""
    if endpoint is None:
        return None
    table = str(
        getattr(endpoint, "table", "") or getattr(endpoint, "collection", "") or ""
    ).strip()
    if not table:
        return None
    from services.dialect_profiles import schema_from_cfg
    from services.sync_cursor import route_endpoint_identity
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    cfg = resolve_connector_config(endpoint)
    declared = str(cfg.get("type") or getattr(endpoint, "format", "") or "").lower()
    # The declared engine wins when covered: the driver resolver folds some
    # covered engines (duckdb -> generic_sql) into families that are not.
    db_type = (
        declared
        if declared in CONTINUITY_ENGINES
        else resolve_driver_type(declared).lower()
    )
    if db_type not in CONTINUITY_ENGINES:
        return None
    schema = str(schema_from_cfg(db_type, cfg) or "")
    database = str(cfg.get("database") or "")
    if db_type == "sqlite":
        from connectors.sqlite_common import sqlite_file_path

        database = sqlite_file_path(
            database,
            str(cfg.get("connection_string") or ""),
            str(cfg.get("host") or ""),
        )
        if not database or database == ":memory:":
            return None
    elif db_type == "duckdb":
        database = os.path.abspath(os.path.expanduser(database)) if database else ""
        if not database or database == ":memory:":
            return None
    # File engines are identified by their path alone: the engine fills a
    # placeholder host during a run, which must not change the key.
    ident = "" if db_type in _FILE_ENGINES else route_endpoint_identity(endpoint)
    key = "|".join(
        [
            db_type,
            ident,
            _dsn_identity(str(cfg.get("connection_string") or "")),
            database,
            schema,
            table,
        ]
    )
    return ContinuityTarget(
        db_type, cfg, schema, table, key, database if db_type in _FILE_ENGINES else ""
    )


LEGACY_ALGO = "gate8_python_v0"


def preferred_algo(db_type: str) -> str:
    from services.continuity_digest import ALGO, SUPPORTED

    return ALGO if db_type in SUPPORTED else LEGACY_ALGO


def fingerprint(
    target: ContinuityTarget, algo: str | None = None
) -> dict[str, Any] | None:
    """Full-table ``{rows, checksum, algo[, columns]}`` with the given algorithm."""
    from services import continuity_digest

    algo = algo or preferred_algo(target.db_type)
    if algo == continuity_digest.ALGO:
        if target.db_type not in continuity_digest.SUPPORTED:
            return None
        return continuity_digest.digest(
            target.db_type,
            target.cfg,
            schema=target.schema,
            table=target.table,
            path=target.path,
        )
    if algo != LEGACY_ALGO:
        return None
    fp = _legacy_fingerprint(target)
    if fp is not None:
        fp["algo"] = LEGACY_ALGO
    return fp


def _legacy_fingerprint(target: ContinuityTarget) -> dict[str, Any] | None:
    """Gate-8 Python fingerprint (baselines written before ``engine_v1``)."""
    from services.reconciliation import EMPTY_POPULATION_DIGEST, verify_target

    rows, checksum = verify_target(
        target.db_type,
        target.cfg,
        schema=target.schema,
        table_name=target.table,
        fallback_rows=-1,
        fallback_checksum="",
        target_columns=None,
        limit=0,
    )
    if rows is None or int(rows) < 0:
        from services.dest_precount import destination_row_count

        counted = destination_row_count(
            target.db_type, target.cfg, schema=target.schema, table_name=target.table
        )
        if counted == 0:
            return {"rows": 0, "checksum": EMPTY_POPULATION_DIGEST}
        return None
    if int(rows) == 0:
        return {"rows": 0, "checksum": EMPTY_POPULATION_DIGEST}
    if not checksum:
        return None
    return {"rows": int(rows), "checksum": str(checksum)}


def _collection():  # type: ignore[no-untyped-def]
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if not mongo or type(mongo).__name__ == "MemoryMongoDBService":
            return None
        if getattr(mongo, "client", None):
            db = mongo.get_database()
            if db is not None:
                return db[COLLECTION]
    except Exception as exc:  # noqa: BLE001
        logger.warning("append continuity store: Mongo unavailable: %s", exc)
    return None


def _load_file() -> dict[str, Any]:
    import json

    path = store_path()
    if not path.exists():
        return {}
    # A store that exists but cannot be read is not "no baselines": reading it
    # as empty would pass every covered append unchecked, and saving over it
    # would silently drop every other table's baseline.
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise ContinuityStoreCorrupt(
            f"append continuity store {path} is unreadable: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise ContinuityStoreCorrupt(
            f"append continuity store {path} is not a baseline map "
            f"(found {type(data).__name__})"
        )
    return data


def load_baseline(key: str) -> dict[str, Any] | None:
    coll = _collection()
    if coll is not None:
        doc = coll.find_one({"key": key}, {"_id": 0})
        return dict(doc) if doc else None
    with _lock:
        entry = _load_file().get(key)
    return dict(entry) if isinstance(entry, dict) else None


def save_baseline(key: str, fp: dict[str, Any], job_id: str) -> None:
    entry = {
        "key": key,
        "rows": int(fp["rows"]),
        "checksum": str(fp["checksum"]),
        "algo": str(fp.get("algo") or LEGACY_ALGO),
        "columns": fp.get("columns"),
        "unreadable": bool(fp.get("unreadable")),
        "job_id": job_id,
        "recorded_at": _now(),
    }
    coll = _collection()
    if coll is not None:
        coll.replace_one({"key": key}, entry, upsert=True)
        return
    with _lock:
        # A corrupt store must never be silently overwritten — the only
        # sanctioned recovery is the audited rebaseline() path, which
        # removes the file explicitly after its audit event is durable.
        data = _load_file()
        data[key] = entry
        write_json_atomic(store_path(), data, indent=2)


def clear_baseline(key: str) -> None:
    coll = _collection()
    if coll is not None:
        coll.delete_one({"key": key})
        return
    with _lock:
        data = _load_file()
        if data.pop(key, None) is not None:
            write_json_atomic(store_path(), data, indent=2)


def effective_sync_mode(request: Any) -> str | None:
    """The route's sync mode, or ``None`` for multi-stream requests."""
    from services.sync_cursor import (
        resolve_effective_sync_mode,
        resolve_selected_sync_contracts,
    )

    contracts = resolve_selected_sync_contracts(
        getattr(request, "stream_contracts", None)
    )
    if len(contracts) > 1:
        return None
    contract = contracts[0] if contracts else None
    return resolve_effective_sync_mode(
        getattr(request, "sync_mode", "") or "",
        contract.sync_mode if contract else None,
    )


def compare(
    table: str,
    baseline: dict[str, Any],
    current: dict[str, Any] | None,
    *,
    dest_missing: bool | None = None,
) -> ContinuityViolation | None:
    """Verifier: ``None`` when ``current`` equals the stored ``baseline``."""
    exp_rows = int(baseline.get("rows") or 0)
    exp_chk = str(baseline.get("checksum") or "")
    details: dict[str, Any] = {
        "reason": "destination_continuity_mismatch",
        "verification_status": "failed",
        "table": table,
        "expected_rows": exp_rows,
        "expected_checksum": exp_chk,
        "baseline_job_id": baseline.get("job_id"),
        "baseline_recorded_at": baseline.get("recorded_at"),
        "recovery": RECOVERY_HINT,
    }
    if baseline.get("unreadable"):
        details.update(found_rows=None, found_checksum=None, mismatch="no_baseline_fingerprint")
        return ContinuityViolation(
            f"Verification failed for table '{table}': the destination could not "
            "be fingerprinted after the last run "
            f"({baseline.get('job_id')}), so there is no verified baseline to "
            "prove it is unchanged. Nothing was written; the destination is "
            f"unchanged. {RECOVERY_HINT}",
            details,
        )
    base_algo = str(baseline.get("algo") or LEGACY_ALGO)
    if current is not None and str(current.get("algo") or LEGACY_ALGO) != base_algo:
        # Digests from different algorithms are never comparable: fail closed.
        current = None
    if current is not None:
        schema_violation = _schema_drift(table, baseline, current, details)
        if schema_violation is not None:
            return schema_violation
    if current is None and dest_missing is True:
        details.update(
            found_rows=None,
            found_checksum=None,
            mismatch="destination_missing",
            recovery=MISSING_RECOVERY_HINT,
        )
        return ContinuityViolation(
            f"Verification failed for table '{table}': the table no longer "
            f"exists, but the last verified run ({baseline.get('job_id')}) left "
            f"{exp_rows} row{'s' if exp_rows != 1 else ''} in it. It was dropped, "
            "or this run points at a different database. An append would "
            "recreate it without that history and still report success. "
            f"Nothing was written. {MISSING_RECOVERY_HINT}",
            details,
        )
    if current is None:
        details.update(found_rows=None, found_checksum=None, mismatch="unreadable")
        return ContinuityViolation(
            f"Verification failed for table '{table}': the destination could "
            "not be re-read to prove it is unchanged since the last verified "
            f"run ({baseline.get('job_id')}). Nothing was written; the "
            f"destination is unchanged. {RECOVERY_HINT}",
            details,
        )
    got_rows = int(current["rows"])
    got_chk = str(current["checksum"])
    if got_rows == exp_rows and got_chk == exp_chk:
        return None
    if got_rows != exp_rows:
        delta = got_rows - exp_rows
        mismatch = "row_count"
        what = (
            f"row count {exp_rows} → {got_rows} "
            f"({abs(delta)} row{'s' if abs(delta) != 1 else ''} "
            f"{'added' if delta > 0 else 'removed'})"
        )
        if got_chk != exp_chk:
            what += " and the value digest differs"
    else:
        mismatch = "checksum"
        what = (
            f"value digest differs at the same row count ({got_rows}) — rows "
            "were changed, or deleted and replaced"
        )
    details.update(found_rows=got_rows, found_checksum=got_chk, mismatch=mismatch)
    return ContinuityViolation(
        f"Verification failed for table '{table}': the destination changed "
        f"since the last verified run ({baseline.get('job_id')}): {what}. "
        f"Expected digest {exp_chk[:12]}…, found {got_chk[:12]}…. Rows were "
        "modified, deleted or added outside this pipeline, so an append "
        "cannot be proven on top of them. Nothing was written; the "
        f"destination is unchanged. {RECOVERY_HINT}",
        details,
    )


def _schema_drift(
    table: str,
    baseline: dict[str, Any],
    current: dict[str, Any],
    details: dict[str, Any],
) -> ContinuityViolation | None:
    old, new = baseline.get("columns"), current.get("columns")
    if old is None or new is None:
        return None
    old_t = {str(c[0]): str(c[1]) for c in old}
    new_t = {str(c[0]): str(c[1]) for c in new}
    if old_t == new_t:
        return None
    added = sorted(set(new_t) - set(old_t))
    dropped = sorted(set(old_t) - set(new_t))
    retyped = sorted(c for c in set(old_t) & set(new_t) if old_t[c] != new_t[c])
    parts = []
    if added:
        parts.append(f"added {added}")
    if dropped:
        parts.append(f"dropped {dropped}")
    if retyped:
        parts.append(
            "retyped " + ", ".join(f"{c} {old_t[c]} → {new_t[c]}" for c in retyped)
        )
    details.update(
        mismatch="schema",
        reason="destination_schema_changed",
        columns_added=added,
        columns_dropped=dropped,
        columns_retyped=retyped,
        found_rows=current.get("rows"),
    )
    return ContinuityViolation(
        f"Verification failed for table '{table}': the destination schema "
        f"changed since the last verified run ({baseline.get('job_id')}): "
        f"{'; '.join(parts)} (a rename shows as one dropped and one added "
        "column). This is a schema change, not a data mismatch; the data was "
        "not compared. Nothing was written; the destination is unchanged. "
        f"{RECOVERY_HINT}",
        details,
    )


def _upgrade_algo(target: ContinuityTarget, baseline: dict[str, Any]) -> None:
    """After a pass under an older algorithm, re-baseline with the preferred one (audited)."""
    new_algo = preferred_algo(target.db_type)
    old_algo = str(baseline.get("algo") or LEGACY_ALGO)
    if new_algo == old_algo:
        return
    fp = fingerprint(target, new_algo)
    if fp is None:
        return
    from services.audit_log import append_audit_event

    append_audit_event(
        action="append_continuity.algo_upgraded",
        resource=f"table:{target.table}",
        actor="system",
        level="info",
        outcome="success",
        details={
            "db_type": target.db_type,
            "table": target.table,
            "from_algo": old_algo,
            "to_algo": new_algo,
            "verified_rows": baseline.get("rows"),
            "baseline_job_id": baseline.get("job_id"),
        },
    )
    save_baseline(target.key, fp, str(baseline.get("job_id") or "algo-upgrade"))


def destination_missing(target: ContinuityTarget) -> bool | None:
    """Whether the baselined table is gone — ``None`` when it cannot be decided.

    File engines are checked on disk first: opening a missing SQLite path would
    create an empty database, a write the caller has promised not to make.
    """
    if target.path:
        if not os.path.exists(target.path):
            return True
        if target.db_type == "duckdb":
            return None
    if target.db_type not in _SQL_PROBE_ENGINES:
        return None
    from services.preflight_cursor_gate import dest_object_missing

    cfg = dict(target.cfg)
    if target.path:
        cfg["database"] = target.path
    return dest_object_missing(target.db_type, cfg, target.table)


def check_before_write(request: Any, *, resume: bool = False) -> ContinuityViolation | None:
    """Fail an append whose destination drifted since the last verified run."""
    if resume:
        return None
    from services.sync_cursor import is_append_sync

    mode = effective_sync_mode(request)
    if mode is None or not is_append_sync(mode):
        return None
    target = resolve_target(getattr(request, "destination", None))
    if target is None:
        return None
    try:
        baseline = load_baseline(target.key)
    except ContinuityStoreCorrupt as exc:
        logger.error("append continuity: %s", exc)
        return ContinuityViolation(
            f"Verification failed for table '{target.table}': the append "
            "continuity baseline store could not be read, so there is no way to "
            "prove the destination is unchanged since the last verified run. "
            "Nothing was written; the destination is unchanged. To recover: "
            "restore the baseline store from backup, or repair it; then re-run.",
            {
                "table": target.table,
                "mismatch": "baseline_store_unreadable",
                "store_error": str(exc),
            },
        )
    if not baseline:
        return None
    try:
        current = fingerprint(target, str(baseline.get("algo") or LEGACY_ALGO))
    except Exception:  # noqa: BLE001
        logger.warning("continuity re-read failed for %s", target.table, exc_info=True)
        current = None
    missing = None
    if current is None and not baseline.get("unreadable"):
        try:
            missing = destination_missing(target)
        except Exception:  # noqa: BLE001 — unknown stays unknown
            missing = None
    violation = compare(target.table, baseline, current, dest_missing=missing)
    if violation is None:
        try:
            _upgrade_algo(target, baseline)
        except Exception:  # noqa: BLE001
            logger.warning("continuity algo upgrade skipped for %s", target.table, exc_info=True)
    return violation


def record_after_run(request: Any, job_id: str, result: Any) -> None:
    """Re-baseline after a successful run; drop the baseline otherwise unknown."""
    if not getattr(result, "success", False):
        return
    from services.sync_cursor import is_append_sync, is_overwrite_sync, requires_upsert

    target = resolve_target(getattr(request, "destination", None))
    if target is None:
        return
    mode = effective_sync_mode(request)
    if mode is None or not (
        is_append_sync(mode) or is_overwrite_sync(mode) or requires_upsert(mode)
    ):
        clear_baseline(target.key)
        return
    try:
        fp = fingerprint(target)
    except Exception:  # noqa: BLE001
        logger.warning("continuity baseline read failed for %s", target.table, exc_info=True)
        fp = None
    if fp is None:
        # Never drop to "no baseline": that would pass the next append unchecked.
        logger.error(
            "append continuity: %s table '%s' could not be fingerprinted after "
            "job %s; the next append will fail until it is re-baselined",
            target.db_type,
            target.table,
            job_id,
        )
        fp = {
            "rows": -1,
            "checksum": "",
            "algo": preferred_algo(target.db_type),
            "unreadable": True,
        }
    save_baseline(target.key, fp, job_id)


class RebaselineError(ValueError):
    """The destination cannot be accepted as a new baseline."""


def rebaseline(endpoint: Any, *, actor: str, reason: str) -> dict[str, Any]:
    """Operator accepts the current destination as the verified baseline.

    Explicit and audited: the audit event is written before the baseline
    moves, so an audit failure leaves the old baseline in force.
    """
    reason = (reason or "").strip()
    if not reason:
        raise RebaselineError("A reason is required to accept the current destination.")
    target = resolve_target(endpoint)
    if target is None:
        raise RebaselineError(
            "This destination is not covered by append continuity verification."
        )
    try:
        current = fingerprint(target)
    except Exception:  # noqa: BLE001 — decided below: missing vs unreadable
        logger.warning("rebaseline re-read failed for %s", target.table, exc_info=True)
        current = None
    removal = False
    if current is None:
        if destination_missing(target) is not True:
            raise RebaselineError(
                f"Table '{target.table}' could not be re-read; the baseline was not changed."
            )
        # The operator accepts that the verified table is gone: the next append
        # starts a new history. Same audit path; the baseline is cleared, not
        # replaced with an invented fingerprint.
        removal = True
    store_error = ""
    try:
        previous = load_baseline(target.key)
    except ContinuityStoreCorrupt as exc:
        # The whole point of rebaseline is recovery — a corrupt store is not a
        # reason to refuse it. The audit record must carry that the previous
        # baseline was unreadable so the reset is traceable (X-CS).
        previous = None
        store_error = str(exc)
    rebaseline_id = f"rebaseline-{uuid.uuid4().hex[:12]}"
    prev_view = (
        {
            "rows": previous.get("rows"),
            "checksum": previous.get("checksum"),
            "job_id": previous.get("job_id"),
        }
        if previous
        else None
    )
    from services.audit_log import append_audit_event

    append_audit_event(
        action="append_continuity.rebaselined",
        resource=f"table:{target.table}",
        actor=actor or "anonymous",
        level="warn",
        outcome="success",
        details={
            "rebaseline_id": rebaseline_id,
            "db_type": target.db_type,
            "table": target.table,
            "reason": reason,
            "previous": prev_view,
            "previous_store_unreadable": bool(store_error),
            "store_error": store_error,
            "current": dict(current) if current else None,
            "destination_missing": removal,
        },
    )
    if store_error:
        # The audit event above is durable, so the corrupt store may now be
        # reset explicitly. A blind save_baseline() still refuses to
        # overwrite a corrupt store — only this audited path may reset it.
        try:
            store_path().unlink(missing_ok=True)
        except OSError as exc:
            raise RebaselineError(
                f"Continuity store could not be reset after audit: {exc}"
            ) from exc
    if removal:
        clear_baseline(target.key)
    else:
        save_baseline(target.key, current, rebaseline_id)
    logger.warning(
        "append continuity re-baselined %s.%s by %s (%s): %s -> %s rows",
        target.db_type,
        target.table,
        actor,
        rebaseline_id,
        prev_view and prev_view.get("rows"),
        "table removed" if removal else current.get("rows"),
    )
    return {
        "rebaseline_id": rebaseline_id,
        "table": target.table,
        "previous": prev_view,
        "current": dict(current) if current else None,
        "destination_missing": removal,
    }


CONTINUITY_GATE_ID = "g21_append_continuity"


def continuity_preflight_gate(
    destination_endpoint: Any,
    *,
    sync: str,
    contracts: list[dict[str, Any]] | None,
    pass_status: str,
    block_status: str,
) -> dict[str, Any] | None:
    """Validate-time view of the check Execute runs before an append writes.

    Without it Validate reported execute-ready and the run then failed on a
    dropped or edited destination. This is the cheap half: table existence and
    row count against the baseline. The full value digest stays at Execute
    (``check_before_write``) — a matching count is never reported as proof.
    ``None`` when the route is not an append onto a baselined table.
    """
    from services.sync_cursor import is_append_sync

    if len([c for c in contracts or [] if c.get("selected", True)]) > 1:
        return None
    if not sync or not is_append_sync(sync) or destination_endpoint is None:
        return None
    endpoint = destination_endpoint
    if isinstance(endpoint, dict):
        from src.transfer.models import EndpointConfig

        endpoint = EndpointConfig.from_dict(str(endpoint.get("kind") or "database"), endpoint)
    target = resolve_target(endpoint)
    if target is None:
        return None
    gate: dict[str, Any] = {"id": CONTINUITY_GATE_ID, "duration_ms": 0}
    try:
        baseline = load_baseline(target.key)
    except ContinuityStoreCorrupt:
        return {
            **gate,
            "status": block_status,
            "message": (
                f"The append continuity store could not be read, so '{target.table}' "
                "cannot be proven unchanged since its last verified run."
            ),
            "details": {"table": target.table, "mismatch": "baseline_store_unreadable"},
        }
    if not baseline:
        return None
    missing = destination_missing(target)
    violation: ContinuityViolation | None = None
    found: int | None = None
    if baseline.get("unreadable") or missing is True:
        violation = compare(target.table, baseline, None, dest_missing=missing)
    else:
        from services.dest_precount import precount_table

        try:
            found = precount_table(target.db_type, target.cfg, target.table)
        except Exception:  # noqa: BLE001 — an unknown count is not drift
            found = None
        expected = int(baseline.get("rows") or 0)
        if found is not None and found != expected:
            violation = compare(
                target.table,
                baseline,
                {"rows": found, "checksum": "", "algo": baseline.get("algo") or LEGACY_ALGO},
            )
    if violation is not None:
        return {
            **gate,
            "status": block_status,
            "message": violation.message,
            "details": dict(violation.details),
        }
    rows = int(baseline.get("rows") or 0)
    held = (
        f"still holds the {rows} row{'s' if rows != 1 else ''} its last verified run left"
        if found is not None
        else "has a verified baseline"
    )
    return {
        **gate,
        "status": pass_status,
        "message": (
            f"'{target.table}' {held}. Execute re-verifies the full value digest "
            "before the append writes."
        ),
        "details": {
            "table": target.table,
            "expected_rows": rows,
            "found_rows": found,
            "baseline_job_id": baseline.get("job_id"),
            "scope": "row_count_only",
        },
    }
