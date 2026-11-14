"""Sweep ``<table>__dfbak_<job>`` backups a killed run left behind (DATA-13).

A strict keyed PostgreSQL write row-copies the live table to a ``__dfbak``
backup and drops it when the run settles. A SIGKILL between the two leaves
the copy forever: nothing listed or swept it.

Safety rules, in order:

* only a backup DataFlow stamped (``COMMENT`` with its job id and time) is
  ever dropped — an unstamped or foreign table is not ours to judge;
* never the current job's backup, and never one whose job is still running,
  pending, or unknown to the job store: that copy may be the only pre-run
  image of a half-written table;
* a completed job's backup is obsolete and goes;
* a failed or cancelled job's backup goes only after the retention window
  (``DATAFLOW_DFBAK_RETENTION_HOURS``, default 168), so an operator has time
  to restore from it;
* bounded: at most ``limit`` drops per sweep, each under a short lock wait.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)

DFBAK_MARKER = "dataflow:dfbak"
_COMMENT_RE = re.compile(rf"^{re.escape(DFBAK_MARKER)} job=(\S+) at=(\d+)$")
_DONE = frozenset({"completed", "success", "succeeded", "partial_success"})
_ENDED = frozenset({"failed", "cancelled", "canceled", "error"})


def backup_comment(job_id: str, now: float | None = None) -> str:
    return f"{DFBAK_MARKER} job={job_id} at={int(now if now is not None else time.time())}"


def parse_backup_comment(text: Any) -> tuple[str, int] | None:
    match = _COMMENT_RE.match(str(text or "").strip())
    return (match.group(1), int(match.group(2))) if match else None


def retention_seconds() -> float:
    try:
        hours = float(os.environ.get("DATAFLOW_DFBAK_RETENTION_HOURS", "168"))
    except ValueError:
        hours = 168.0
    return max(hours, 1.0) * 3600.0


def _job_status(job_id: str) -> str:
    try:
        from services.mongodb_service import get_mongodb_service

        job = get_mongodb_service().get_job(job_id) or {}
    except Exception as exc:  # noqa: BLE001 - unknown status keeps the backup
        logger.debug("dfbak sweep: job %s status unavailable: %s", job_id, exc)
        return ""
    return str(job.get("status") or "").strip().lower()


def sweep_decision(job_status: str, age_seconds: float, retention: float) -> bool:
    """True when a stamped backup may be dropped."""
    if job_status in _DONE:
        return True
    return job_status in _ENDED and age_seconds > retention


def sweep_pg_backups(
    cur: Any,
    schema: str,
    *,
    keep_job: str = "",
    limit: int = 20,
    now: float | None = None,
    job_status: Callable[[str], str] = _job_status,
) -> list[str]:
    """Drop the obsolete stamped backups in ``schema``; return their names."""
    from connectors.sql_identifiers import quote_table_ref

    now = time.time() if now is None else now
    retention = retention_seconds()
    cur.execute(
        "SELECT c.relname, obj_description(c.oid, 'pg_class') FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relkind = 'r' AND position('__dfbak_' in c.relname) > 0 "
        "ORDER BY c.relname LIMIT 500",
        (schema,),
    )
    dropped: list[str] = []
    for name, comment in list(cur.fetchall() or []):
        if len(dropped) >= max(0, int(limit)):
            break
        stamp = parse_backup_comment(comment)
        if stamp is None:
            continue
        job_id, at = stamp
        if keep_job and job_id == keep_job:
            continue
        if not sweep_decision(job_status(job_id), now - at, retention):
            continue
        # The cursor is autocommit (the backup connection): each DROP stands
        # alone, so a locked backup is skipped without touching the others.
        try:
            cur.execute("SET lock_timeout = '2s'")
            cur.execute("DROP TABLE IF EXISTS " + quote_table_ref(str(name), schema, dialect="postgresql"))
            dropped.append(str(name))
        except Exception as exc:  # noqa: BLE001 - one locked backup must not stop the run
            logger.warning("dfbak sweep: %s.%s not dropped: %s", schema, name, exc)
        finally:
            cur.execute("RESET lock_timeout")
    if dropped:
        logger.info("dfbak sweep dropped %d obsolete backup(s) in %s: %s", len(dropped), schema, dropped)
    return dropped


__all__ = [
    "DFBAK_MARKER",
    "backup_comment",
    "parse_backup_comment",
    "sweep_decision",
    "sweep_pg_backups",
]
