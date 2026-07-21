"""Quarantine DLQ must persist rejected rows (retry, no silent drop)."""

from __future__ import annotations

import pytest


def test_persist_rejected_rows_writes_dlq(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "quarantine_dlq.jsonl")
    ev = dlq.persist_rejected_rows(
        job_id="job_x",
        rejected_details=[{"row": 1, "reason": "bad_int", "values": {"id": "x"}}],
        workspace_id="ws1",
    )
    assert ev is not None
    assert ev["action"] == "quarantine"
    assert ev["rows"] == 1
    listed = dlq.list_dlq_events(job_id="job_x")
    assert len(listed) == 1
    assert listed[0]["details"]["rejected_details"][0]["reason"] == "bad_int"


def test_append_dlq_retries_then_raises(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "readonly" / "q.jsonl")
    # Make parent unwritable by pointing at a file-as-directory conflict
    blocker = tmp_path / "readonly"
    blocker.write_text("not-a-dir", encoding="utf-8")
    with pytest.raises(OSError):
        dlq.append_dlq_event(job_id="j", action="quarantine", rows=1)
