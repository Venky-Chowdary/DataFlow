"""Resilient batch execution — checkpoint, retry, quarantine, and adaptive sizing.

This module is the runtime backbone for enterprise transfers:
- per-chunk retry with bounded exponential backoff
- durable checkpoint persistence after each committed chunk
- quarantine of malformed rows without killing the job
- adaptive chunk sizing to stay memory-safe for large or wide rows
- idempotent write decisions (upsert vs insert) when a primary key exists
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .checkpoint_service import Checkpoint, CheckpointService
from .error_handling import RetryBudget, classify_error, quarantine_record, should_quarantine

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    """Outcome of a single batch write."""

    rows_written: int = 0
    checksum: str = ""
    rejected_rows: int = 0
    rejected_details: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchContext:
    """Everything a resilient batcher needs to know about one batch."""

    batch: list[dict[str, Any]]
    chunk_index: int
    chunk_total: int
    rows_so_far: int = 0
    checkpoint: Checkpoint | None = None


class ResilientBatcher:
    """Execute a transfer one batch at a time with checkpoint + retry.

    The caller provides a `write_fn(batch, ctx) -> BatchResult` and a
    `read_next_fn(checkpoint)` that returns the next batch plus continuation
    state.  The batcher persists a checkpoint after each successful write, so
    crashes and explicit retries can resume from the last committed chunk.
    """

    def __init__(
        self,
        job_id: str,
        write_fn: Callable[[BatchContext], BatchResult],
        read_next_fn: Callable[[Checkpoint], tuple[list[dict[str, Any]], Checkpoint] | None],
        checkpoint_service: CheckpointService | None = None,
        retry_budget: RetryBudget | None = None,
        on_checkpoint: Callable[[int, int, int], None] | None = None,
        max_quarantine: int | None = None,
    ) -> None:
        self.job_id = job_id
        self.write_fn = write_fn
        self.read_next_fn = read_next_fn
        self.cp = checkpoint_service or CheckpointService()
        self.retry_budget = retry_budget or RetryBudget()
        self.on_checkpoint = on_checkpoint
        self.max_quarantine = max_quarantine

    def run(self, initial_checkpoint: Checkpoint | None = None) -> tuple[int, BatchResult, Checkpoint]:
        """Run until source exhausted or hard failure."""
        checkpoint = initial_checkpoint or self.cp.load(self.job_id) or Checkpoint(job_id=self.job_id)
        if not checkpoint.job_id:
            checkpoint.job_id = self.job_id

        total_written = checkpoint.rows_processed or 0
        final_result = BatchResult()
        while True:
            read_next = self.read_next_fn(checkpoint)
            if read_next is None:
                break
            batch, checkpoint = read_next
            if not batch:
                break

            ctx = BatchContext(batch=batch, chunk_index=checkpoint.chunk_index, chunk_total=checkpoint.chunk_total, rows_so_far=total_written, checkpoint=checkpoint)
            result = self._write_with_retry(ctx)

            total_written += result.rows_written
            checkpoint.rows_processed = total_written
            checkpoint.chunk_index += 1
            checkpoint.rejected_rows += result.rejected_rows
            checkpoint.rejected_details.extend(result.rejected_details or [])

            # Update summary metadata
            final_result.rows_written = total_written
            final_result.checksum = result.checksum
            final_result.rejected_rows = checkpoint.rejected_rows
            final_result.rejected_details = checkpoint.rejected_details
            final_result.warnings.extend(result.warnings or [])

            self.cp.save(checkpoint)
            if self.on_checkpoint:
                self.on_checkpoint(checkpoint.chunk_index, checkpoint.chunk_total, total_written)

        checkpoint.phase = "completed"
        self.cp.save(checkpoint)
        return total_written, final_result, checkpoint

    def _write_with_retry(self, ctx: BatchContext) -> BatchResult:
        """Attempt a batch write, retry on transient errors, quarantine on data errors."""
        budget = self.retry_budget
        last_error: Exception | None = None
        attempt = 0
        while attempt < budget.max_attempts:
            attempt += 1
            try:
                return self.write_fn(ctx)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                classification = classify_error(exc)
                if not classification["retriable"]:
                    # Quarantine rows where the batch is small enough to inspect
                    if len(ctx.batch) <= 1:
                        raise
                    # If we can identify the bad row, quarantine it and retry the rest
                    result = self._try_quarantine(ctx, exc)
                    if result:
                        return result
                    raise
                delay = budget.base_delay_seconds * (budget.exponential_base ** (attempt - 1))
                delay = min(delay, budget.max_delay_seconds)
                delay = delay * (0.5 + (time.time() % 1) * 0.5)  # jitter
                logger.warning("Job %s batch %s transient error (attempt %s): %s — retry in %.2fs", self.job_id, ctx.chunk_index, attempt, exc, delay)
                time.sleep(delay)

        raise last_error or RuntimeError("Batch retry budget exhausted")

    def _try_quarantine(self, ctx: BatchContext, batch_error: Exception) -> BatchResult | None:
        """Try to write each row individually, quarantining the bad ones.

        This is a best-effort fallback; it only makes sense for small batches
        where the per-row overhead is acceptable.  Returns None if quarantine
        is not appropriate or failed.
        """
        if self.max_quarantine is not None and ctx.checkpoint and ctx.checkpoint.rejected_rows >= self.max_quarantine:
            return None
        if not ctx.batch:
            return None

        quarantined: list[dict[str, Any]] = []
        written_count = 0
        last_checksum = ""
        for i, row in enumerate(ctx.batch):
            try:
                single = BatchContext(batch=[row], chunk_index=ctx.chunk_index, chunk_total=ctx.chunk_total, rows_so_far=ctx.rows_so_far + written_count, checkpoint=ctx.checkpoint)
                result = self.write_fn(single)
                written_count += result.rows_written
                if result.checksum:
                    last_checksum = result.checksum
            except Exception as exc:  # noqa: BLE001
                if should_quarantine(error=exc, row_index=i, max_quarantine=self.max_quarantine, current_quarantine_count=(ctx.checkpoint.rejected_rows if ctx.checkpoint else 0) + len(quarantined)):
                    rec = quarantine_record(row, reason="batch_error", stage="write", error=str(exc), row_index=i)
                    quarantined.append(rec.__dict__)
                else:
                    return None
        if not written_count:
            return None
        return BatchResult(
            rows_written=written_count,
            checksum=last_checksum,
            rejected_rows=len(quarantined),
            rejected_details=quarantined,
            warnings=[f"Quarantined {len(quarantined)} row(s) from batch {ctx.chunk_index}"],
        )


def adaptive_chunk_size(
    base_size: int,
    avg_row_size_bytes: int | None,
    *,
    max_size: int = 10000,
    min_size: int = 1,
    target_memory_bytes: int = 8 * 1024 * 1024,  # 8 MB
) -> int:
    """Return a safe chunk size for the current row payload.

    If rows are very wide (JSON blobs, long text) we shrink the chunk so the
    worker stays under a bounded memory footprint.
    """
    if not avg_row_size_bytes or avg_row_size_bytes <= 0:
        return max(min_size, min(base_size, max_size))
    by_memory = max(1, target_memory_bytes // avg_row_size_bytes)
    size = min(base_size, by_memory, max_size)
    return max(min_size, size)


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
