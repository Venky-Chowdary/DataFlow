"""A resumed overwrite restarts from the first row instead of seeking past its checkpoint.

A PostgreSQL overwrite loads in one transaction. A cancel rolls it back, yet
the checkpoint still counts the batches the writer had sent. Resuming past
that cursor reloaded only the rows after it, and the keyed resume deadlocked
on its own held TRUNCATE (route certification cancel/resume matrix).
"""

from __future__ import annotations

import pytest

from services.checkpoint_service import Checkpoint
from src.transfer.resume_state import resolve_resume_checkpoint


class _Store:
    def __init__(self, checkpoint: Checkpoint) -> None:
        self._checkpoint = checkpoint

    def load(self, _job_id: str) -> Checkpoint:
        return self._checkpoint


class _Mongo:
    def get_job(self, _job_id: str) -> dict:
        return {"records_processed": 5}


def _resolve(sync_mode: str) -> Checkpoint:
    saved = Checkpoint(job_id="j1", chunk_index=1, offset=5, rows_processed=5)
    saved.cursor_column = "id"
    saved.cursor_value = "5"
    return resolve_resume_checkpoint(
        job_id="j1",
        mongo=_Mongo(),
        checkpoint_service=_Store(saved),
        has_progress=lambda cp: bool(cp and (cp.offset or cp.rows_processed)),
        sync_mode=sync_mode,
    )


@pytest.mark.parametrize("mode", ["full_refresh_overwrite"])
def test_overwrite_resume_starts_at_the_first_row(mode: str) -> None:
    fresh = _resolve(mode)
    assert (fresh.offset, fresh.rows_processed, fresh.chunk_index) == (0, 0, 0)
    assert not getattr(fresh, "cursor_value", None)


@pytest.mark.parametrize("mode", ["full_refresh_append", "upsert"])
def test_append_and_upsert_resume_keep_the_checkpoint(mode: str) -> None:
    kept = _resolve(mode)
    assert (kept.offset, kept.cursor_value) == (5, "5")
