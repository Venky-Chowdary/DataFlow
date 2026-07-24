"""Durable checkpoint persistence for resumable transfer jobs.

Checkpoints live inside the `transfer_jobs` MongoDB document so the live job
stream and the resume/retry flow can read them from the same record.  A
checkpoint captures the last successfully committed chunk and the cursor that
must be used to read the *next* chunk.  This makes resume deterministic:
re-read from `cursor_after` (or `offset`) instead of starting over.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


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
    rejected_details: list[dict[str, Any]] = field(default_factory=list)

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
            "rejected_details": self.rejected_details,
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

    def _mongo(self):
        if self.mongo is None:
            try:
                from services.mongodb_service import get_mongodb_service
            except ImportError:  # pragma: no cover - tests with api root on path
                from src.services.mongodb_service import get_mongodb_service
            self.mongo = get_mongodb_service()
        return self.mongo

    def save(self, checkpoint: Checkpoint) -> bool:
        """Persist the checkpoint without overwriting the job status."""
        mongo = self._mongo()
        return mongo.update_job_status(
            checkpoint.job_id,
            checkpoint.status,
            checkpoint=checkpoint.to_dict(),
            updated_at=datetime.now(timezone.utc),
        )

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
