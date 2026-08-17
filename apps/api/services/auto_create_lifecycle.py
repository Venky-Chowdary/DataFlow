"""Auto-create DDL lifecycle — refuse orphan destination tables (audit §2.3).

When Execute auto-creates a destination object and then fails before the first
durable write ack / checkpoint, drop the empty shell so warehouses do not
accumulate ``*_dst_*`` leftovers from failed jobs.

Writers register creations via :func:`register_auto_create`. The transfer engine
calls :func:`rollback_uncommitted_auto_creates` on fail-closed exits before
``rows_written > 0``.
"""

from __future__ import annotations

import logging
import threading
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class AutoCreateRecord:
    db_type: str
    table: str
    schema: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    job_id: str = ""
    committed: bool = False


_LOCK = threading.Lock()
# job_id → records (process-local; durable queue phase will persist this)
_REGISTRY: dict[str, list[AutoCreateRecord]] = {}
_CURRENT_JOB: ContextVar[str] = ContextVar("df_auto_create_job", default="")


def bind_auto_create_job(job_id: str):
    """Context manager: associate auto-creates with a transfer job id."""
    from contextlib import contextmanager

    @contextmanager
    def _cm():
        token = _CURRENT_JOB.set(str(job_id or "").strip())
        try:
            yield
        finally:
            _CURRENT_JOB.reset(token)

    return _cm()


def register_auto_create(
    *,
    db_type: str,
    table: str,
    schema: str | None = None,
    config: dict[str, Any] | None = None,
    job_id: str = "",
) -> None:
    """Record that this job created ``schema.table`` (empty shell until commit)."""
    jid = (job_id or _CURRENT_JOB.get() or "").strip()
    if not jid or not table:
        return
    rec = AutoCreateRecord(
        db_type=str(db_type or "").strip().lower(),
        table=str(table).strip(),
        schema=(str(schema).strip() if schema else None),
        config=dict(config or {}),
        job_id=jid,
    )
    with _LOCK:
        _REGISTRY.setdefault(jid, []).append(rec)


def mark_auto_create_committed(job_id: str) -> None:
    """First durable write succeeded — keep the table (no orphan rollback)."""
    jid = (job_id or "").strip()
    if not jid:
        return
    with _LOCK:
        for rec in _REGISTRY.get(jid, []):
            rec.committed = True


def clear_auto_create_job(job_id: str) -> None:
    with _LOCK:
        _REGISTRY.pop((job_id or "").strip(), None)


def rollback_uncommitted_auto_creates(job_id: str) -> list[str]:
    """DROP auto-created tables that never received a durable write.

    Returns list of dropped ``schema.table`` names (best-effort; logs failures).
    """
    jid = (job_id or "").strip()
    if not jid:
        return []
    with _LOCK:
        records = list(_REGISTRY.pop(jid, []))
    dropped: list[str] = []
    for rec in records:
        if rec.committed:
            continue
        qual = f"{rec.schema}.{rec.table}" if rec.schema else rec.table
        try:
            from connectors.table_manager import drop_table

            drop_table(rec.db_type, rec.config, rec.table, schema=rec.schema)
            dropped.append(qual)
            logger.info(
                "auto_create_rollback job=%s dropped empty %s (%s)",
                jid,
                qual,
                rec.db_type,
            )
        except Exception as exc:
            logger.warning(
                "auto_create_rollback job=%s failed to drop %s: %s",
                jid,
                qual,
                exc,
                exc_info=True,
            )
    return dropped
