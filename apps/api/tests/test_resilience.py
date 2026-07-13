"""Tests for the resilient batch execution layer."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest

from services.checkpoint_service import Checkpoint
from services.resilience import (
    BatchContext,
    BatchResult,
    ResilientBatcher,
    adaptive_chunk_size,
)


class _MemoryCheckpointService:
    def __init__(self):
        self._jobs: dict[str, dict] = {}

    def save(self, checkpoint: Checkpoint) -> bool:
        self._jobs[checkpoint.job_id] = {"checkpoint": checkpoint.to_dict()}
        return True

    def load(self, job_id: str) -> Checkpoint | None:
        data = self._jobs.get(job_id, {}).get("checkpoint")
        return Checkpoint.from_dict(data) if data else None

    def get_job(self, job_id: str) -> dict | None:
        return self._jobs.get(job_id)


def _make_batcher(job_id: str, write_fn, read_next_fn, cp=None, max_attempts: int = 3):
    from services.resilience import BatchResult
    from services.error_handling import RetryBudget
    from services.checkpoint_service import Checkpoint

    if cp is None:
        cp = _MemoryCheckpointService()
    return ResilientBatcher(
        job_id=job_id,
        write_fn=write_fn,
        read_next_fn=read_next_fn,
        checkpoint_service=cp,
        retry_budget=RetryBudget(max_attempts=max_attempts, base_delay_seconds=0.001),
        max_quarantine=10,
    )


def test_resilient_batcher_processes_all_batches():
    batches = [
        [{"id": "1"}, {"id": "2"}],
        [{"id": "3"}, {"id": "4"}],
    ]
    calls = []

    def write_fn(ctx: BatchContext) -> BatchResult:
        calls.append(ctx.batch)
        return BatchResult(rows_written=len(ctx.batch))

    def read_next_fn(cp: Checkpoint):
        idx = cp.chunk_index
        if idx >= len(batches):
            return None
        return batches[idx], cp

    batcher = _make_batcher("job-1", write_fn, read_next_fn)
    written, result, cp = batcher.run()
    assert written == 4
    assert result.rows_written == 4
    assert cp.chunk_index == 2
    assert len(calls) == 2


def test_resilient_batcher_retries_transient_failure():
    batches = [[{"id": "1"}]]
    attempts = []

    def write_fn(ctx: BatchContext) -> BatchResult:
        attempts.append(ctx.chunk_index)
        if len(attempts) < 2:
            raise ConnectionError("transient")
        return BatchResult(rows_written=1)

    def read_next_fn(cp: Checkpoint):
        if cp.chunk_index >= len(batches):
            return None
        return batches[cp.chunk_index], cp

    batcher = _make_batcher("job-2", write_fn, read_next_fn, max_attempts=3)
    written, _, _ = batcher.run()
    assert written == 1
    assert len(attempts) == 2


def test_resilient_batcher_quarantines_bad_rows():
    batches = [[{"id": "1"}, {"id": "bad"}]]

    def write_fn(ctx: BatchContext) -> BatchResult:
        if any(row.get("id") == "bad" for row in ctx.batch):
            raise ValueError("invalid value")
        return BatchResult(rows_written=len(ctx.batch))

    def read_next_fn(cp: Checkpoint):
        if cp.chunk_index >= len(batches):
            return None
        return batches[cp.chunk_index], cp

    batcher = _make_batcher("job-3", write_fn, read_next_fn, max_attempts=2)
    written, result, _ = batcher.run()
    assert written == 1
    assert result.rejected_rows == 1


def test_adaptive_chunk_size_respects_memory_target():
    assert adaptive_chunk_size(5000, 100) == 5000
    assert adaptive_chunk_size(5000, 1000, target_memory_bytes=1000, min_size=1) == 1
    assert adaptive_chunk_size(5000, 1000, target_memory_bytes=8 * 1024 * 1024) <= 8192
    assert adaptive_chunk_size(5000, None) == 5000


def test_resilient_batcher_resumes_from_checkpoint():
    cp = Checkpoint(job_id="job-4", chunk_index=1, rows_processed=2)
    service = _MemoryCheckpointService()
    service.save(cp)
    batches = [
        [{"id": "1"}, {"id": "2"}],
        [{"id": "3"}, {"id": "4"}],
    ]

    def write_fn(ctx: BatchContext) -> BatchResult:
        return BatchResult(rows_written=len(ctx.batch))

    def read_next_fn(checkpoint: Checkpoint):
        idx = checkpoint.chunk_index
        if idx >= len(batches):
            return None
        return batches[idx], checkpoint

    batcher = _make_batcher("job-4", write_fn, read_next_fn, cp=service)
    written, _, final = batcher.run()
    assert written == 4
    assert final.chunk_index == 2
    assert final.rows_processed == 4
