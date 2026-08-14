"""Durable checkpoint persistence for resumable transfer jobs.

Checkpoints live inside the `transfer_jobs` MongoDB document so the live job
stream and the resume/retry flow can read them from the same record.  A
checkpoint captures the last successfully committed chunk and the cursor that
must be used to read the *next* chunk.  This makes resume deterministic:
re-read from `cursor_after` (or `offset`) instead of starting over.

Fail-closed
-----------
A rejected checkpoint write means the job has no durable resume point. Callers
must **hard-fail** the transfer (via ``require_save`` or by raising
``CheckpointPersistenceError``) - never continue writing while reporting healthy
progress. Continuing without a checkpoint creates silent resume risk: a crash
would re-read from an older cursor and risk duplicate or skipped work under
at-least-once delivery.
"""

from __future__ import annotations

import logging
from services.brand_env import getenv_brand
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

#: Hard cap on quarantine evidence carried inside a checkpoint document.
#: The checkpoint is rewritten on every chunk and lives inside the job record,
#: so this list is the one field that can push the document past MongoDB's
#: 16 MB limit and break resume entirely. Preview rows are slimmed; the exact
#: count still lives in ``rejected_rows`` and the DLQ ledger.
MAX_REJECTED_DETAILS = int(getenv_brand("MAX_REJECTED_DETAILS", "80") or 80)

#: Operator-facing message when checkpoint persistence fails (fail-closed).
CHECKPOINT_PERSISTENCE_FAILED = (
    "Checkpoint persistence failed - refusing to continue without durable resume point."
)


class CheckpointPersistenceError(RuntimeError):
    """Raised when a checkpoint cannot be durably persisted - job must abort."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Checkpoint:
    """Resume token for a transfer job."""

    job_id: str = ""
    phase: str = "writing"  # reading, writing, reconcile, completed
    source_type: str = ""
    dest_type: str = ""
    chunk_index: int = 0
    chunk_total: int = 0
    rows_processed: int = 0
    # For cursor-based / keyset sources (DB -> DB)
    cursor_column: str = ""
    cursor_value: Any = None
    # For offset-based sources (some DBs, file streaming)
    offset: int = 0
    # For file streaming
    file_offset: int = 0
    # Source-specific continuation tokens
    dynamodb_cursor: dict | None = None
    es_search_after: list | None = None
    redis_scan_state: Any = None
    kafka_cursor: dict | None = None
    # Last destination checksum for cross-check on resume
    checksum: str = ""
    # Write mode and conflict columns used for idempotent writes
    write_mode: str = "insert"
    conflict_columns: list[str] = field(default_factory=list)
    # Retry / error state
    attempt: int = 0
    max_attempts: int = 3
    last_error: str = ""
    status: str = "running"  # running, retrying, paused, failed
    # Metadata
    updated_at: str = field(default_factory=_now)
    rejected_rows: int = 0
    #: Cumulative count of rows whose cell(s) were coerced to NULL and KEPT. Gate-8
    #: conservation is ``source - (rejected - coerced_null) - skipped``; on resume
    #: both counters must be restored or first-pass quarantine is lost and a
    #: correct resumed load falsely fails conservation.
    coerced_null_rows: int = 0
    #: Bounded sample of quarantined rows. ``rejected_rows`` remains the exact
    #: count; this list is evidence for the operator, not the ledger.
    rejected_details: list[dict[str, Any]] = field(default_factory=list)
    #: How many rejection details were dropped once the sample cap was reached,
    #: so the UI can say "showing N of M" instead of implying the list is whole.
    rejected_details_truncated: int = 0

    def add_rejected_details(self, details: list[dict[str, Any]] | None) -> None:
        """Append rejection evidence, keeping the checkpoint document bounded.

        The checkpoint is persisted in full on *every* chunk. An uncapped list
        grew with the number of quarantined rows until the document crossed
        MongoDB's 16 MB limit, at which point every subsequent checkpoint save
        failed and the job silently lost its ability to resume - the failure
        mode was worst exactly when the operator most needed the evidence.
        """
        if not details:
            return
        try:
            from services.job_document_budget import slim_rejected_detail

            slimmed = [slim_rejected_detail(d) for d in details]
        except Exception:
            slimmed = list(details)
        room = MAX_REJECTED_DETAILS - len(self.rejected_details)
        if room > 0:
            self.rejected_details.extend(slimmed[:room])
            overflow = len(slimmed) - room
        else:
            overflow = len(slimmed)
        if overflow > 0:
            self.rejected_details_truncated += overflow

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "phase": self.phase,
            "source_type": self.source_type,
            "dest_type": self.dest_type,
            "chunk_index": self.chunk_index,
            "chunk_total": self.chunk_total,
            "rows_processed": self.rows_processed,
            "cursor_column": self.cursor_column,
            "cursor_value": self.cursor_value,
            "offset": self.offset,
            "file_offset": self.file_offset,
            "dynamodb_cursor": self.dynamodb_cursor,
            "es_search_after": self.es_search_after,
            "redis_scan_state": (
                self.redis_scan_state.to_dict()
                if hasattr(self.redis_scan_state, "to_dict")
                else self.redis_scan_state
            ),
            "kafka_cursor": self.kafka_cursor,
            "checksum": self.checksum,
            "write_mode": self.write_mode,
            "conflict_columns": self.conflict_columns,
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
            "last_error": self.last_error,
            "status": self.status,
            "updated_at": self.updated_at,
            "rejected_rows": self.rejected_rows,
            "coerced_null_rows": self.coerced_null_rows,
            "rejected_details": self.rejected_details,
            "rejected_details_truncated": self.rejected_details_truncated,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Checkpoint":
        if not data:
            return cls()
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class CheckpointService:
    """Store and retrieve checkpoints from the MongoDB job record."""

    def __init__(self, mongo=None) -> None:
        self.mongo = mongo
        #: Number of checkpoint writes the store rejected or could not reach.
        self.failed_saves = 0

    @property
    def has_failed_saves(self) -> bool:
        """True once any checkpoint write has failed (fail-closed signal)."""
        return self.failed_saves > 0

    @property
    def degraded(self) -> bool:
        """Alias for ``has_failed_saves`` - checkpoint durability is broken.

        Callers must abort the job when this is True; continuing without a
        durable resume point is forbidden.
        """
        return self.has_failed_saves

    def _mongo(self):
        if self.mongo is None:
            try:
                from services.mongodb_service import get_mongodb_service
            except ImportError:  # pragma: no cover - tests with api root on path
                from src.services.mongodb_service import get_mongodb_service
            self.mongo = get_mongodb_service()
        return self.mongo

    def save(self, checkpoint: Checkpoint) -> bool:
        """Persist the checkpoint without overwriting the job status.

        ``update_job_status`` returns ``False`` rather than raising when the job
        store is unreachable. Callers **must** treat ``False`` / ``has_failed_saves``
        as a hard failure (prefer ``require_save``). Returning bool lets unit
        tests assert the failure counter without catching exceptions.
        """
        mongo = self._mongo()
        ok = mongo.update_job_status(
            checkpoint.job_id,
            checkpoint.status,
            checkpoint=checkpoint.to_dict(),
            updated_at=datetime.now(timezone.utc),
        )
        if not ok:
            self.failed_saves += 1
            if self.failed_saves == 1:
                logger.error(
                    "Checkpoint write failed for job %s (chunk %s, %s rows). "
                    "%s The job store rejected or could not accept the checkpoint.",
                    checkpoint.job_id,
                    getattr(checkpoint, "chunk_index", "?"),
                    getattr(checkpoint, "rows_processed", "?"),
                    CHECKPOINT_PERSISTENCE_FAILED,
                )
        return ok

    def require_save(self, checkpoint: Checkpoint) -> None:
        """Persist the checkpoint or raise ``CheckpointPersistenceError``.

        Transfer / stream / resilience paths use this so a failed write aborts
        the job instead of continuing with resume risk.
        """
        if not self.save(checkpoint):
            raise CheckpointPersistenceError(CHECKPOINT_PERSISTENCE_FAILED)

    def load(self, job_id: str) -> Checkpoint | None:
        """Load the most recent checkpoint for a job."""
        mongo = self._mongo()
        job = mongo.get_job(job_id)
        if not job:
            return None
        cp = job.get("checkpoint")
        if not cp:
            return None
        return Checkpoint.from_dict(cp)

    def mark_failed(self, job_id: str, error: str, checkpoint: Checkpoint | None = None) -> bool:
        """Mark a job failed with a final checkpoint so retry can resume."""
        mongo = self._mongo()
        updates = {"phase": "failed", "error": error, "checkpoint_status": "failed"}
        if checkpoint:
            updates["checkpoint"] = checkpoint.to_dict()
        return mongo.update_job_status(job_id, "failed", **updates)

    def mark_paused(self, job_id: str, checkpoint: Checkpoint) -> bool:
        """Pause a job (retriable) and persist the checkpoint for resume."""
        return self.save(checkpoint)




def evaluate_resume_safety(
    checkpoint: "Checkpoint | dict | None",
    *,
    job: dict | None = None,
    max_age_hours: float | None = None,
) -> dict[str, Any]:
    """Decide whether Resume is safe for operators.

    Returns ok / age_hours / reasons / warnings. Refuses when there is no
    durable progress token, the checkpoint is older than
    DATAFLOW_RESUME_MAX_AGE_HOURS (when set >0), or write_mode drifted vs
    the saved transfer request. A CDC cursor-gap job is a sanctioned restart
    (``gap_restart``) even without a checkpoint — the durable cursor is the
    problem. Delivery remains at-least-once.
    """
    import os

    out: dict[str, Any] = {
        "ok": False,
        "age_hours": None,
        "reasons": [],
        "warnings": [],
        "checkpoint": None,
        "honesty": (
            "Resume continues from last committed chunk - "
            "at-least-once upsert, not exactly-once."
        ),
    }
    job = job or {}
    from services.cdc_cursor_gap import job_has_cursor_gap

    if job_has_cursor_gap(job):
        out["ok"] = True
        out["gap_restart"] = True
        out["warnings"].append(
            "CDC cursor-gap recovery restarts the run — not a checkpoint continuation. "
            "when_needed snapshots current source keys then streams from the new tip. "
            "Purged-window events are gone. Not migration_proven."
        )
        out["honesty"] = (
            "Gap recovery is at-least-once upsert of the current source population, "
            "not continuous CDC across the lost window."
        )
        return out
    if checkpoint is None:
        out["reasons"].append(
            "No durable checkpoint - use Retry from start or re-run from Transfer Studio."
        )
        return out
    cp = checkpoint if isinstance(checkpoint, Checkpoint) else Checkpoint.from_dict(checkpoint)
    out["checkpoint"] = {
        "chunk_index": cp.chunk_index,
        "rows_processed": cp.rows_processed,
        "write_mode": cp.write_mode,
        "conflict_columns": list(cp.conflict_columns or []),
        "updated_at": cp.updated_at,
        "phase": cp.phase,
        "status": cp.status,
    }
    has_progress = (
        int(cp.chunk_index or 0) > 0
        or int(cp.rows_processed or 0) > 0
        or cp.cursor_value is not None
        or int(cp.offset or 0) > 0
        or int(cp.file_offset or 0) > 0
        or bool(cp.dynamodb_cursor)
        or bool(cp.kafka_cursor)
        or cp.es_search_after is not None
    )
    if not has_progress:
        out["reasons"].append(
            "Checkpoint has no committed progress - refuse Resume to avoid a false restart."
        )
        return out

    age_hours = None
    if cp.updated_at:
        try:
            raw = str(cp.updated_at).replace("Z", "+00:00")
            ts = datetime.fromisoformat(raw)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_hours = max(
                0.0,
                (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds() / 3600.0,
            )
            out["age_hours"] = round(age_hours, 3)
        except Exception:
            out["warnings"].append("Checkpoint updated_at could not be parsed - age unknown.")

    if max_age_hours is None:
        try:
            max_age_hours = float(os.getenv("DATAFLOW_RESUME_MAX_AGE_HOURS", "168") or "168")
        except ValueError:
            max_age_hours = 168.0
    if max_age_hours and age_hours is not None and age_hours > float(max_age_hours):
        out["reasons"].append(
            f"Checkpoint is {age_hours:.1f}h old (max {max_age_hours:g}h) - "
            "refuse stale Resume; Retry from start or re-Validate."
        )
        return out

    job = job or {}
    payload = job.get("transfer_request") if isinstance(job.get("transfer_request"), dict) else {}
    req_mode = str(payload.get("write_mode") or payload.get("load_mode") or "").strip().lower()
    cp_mode = str(cp.write_mode or "").strip().lower()
    if req_mode and cp_mode and req_mode != cp_mode and req_mode not in {"", "auto"}:
        out["reasons"].append(
            f"Write mode drifted (checkpoint={cp_mode}, request={req_mode}) - refuse unsafe Resume."
        )
        return out

    if str(job.get("status") or "").lower() in {"running", "pending"}:
        out["warnings"].append("Job already running/pending - Resume may be a no-op or race.")

    out["ok"] = True
    if age_hours is not None and age_hours > 24:
        out["warnings"].append(
            f"Checkpoint is {age_hours:.1f}h old - confirm destination still matches before Resume."
        )
    return out


def get_checkpoint_service(mongo=None) -> CheckpointService:
    return CheckpointService(mongo)


def resume_or_create_checkpoint(
    job_id: str,
    checkpoint_service: CheckpointService | None = None,
    defaults: dict[str, Any] | None = None,
) -> Checkpoint:
    """Load existing checkpoint or initialize a new one."""
    cp = checkpoint_service or CheckpointService()
    existing = cp.load(job_id)
    if existing:
        return existing
    merged = {"job_id": job_id}
    if defaults:
        merged.update(defaults)
    return Checkpoint.from_dict(merged)
