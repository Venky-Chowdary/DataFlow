"""Parity at Scale phase 3: a job with no verification block is byte-identical to before (review N6).

The summary stamp and the stored dashboard summary of a job that never had a
verification must hash exactly as they did before phase 3 (the digests below
were computed with the code before phase 3, dc61397ae), so nothing stored
before is treated as stale and the stored shape does not change with the flag
off.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest


@pytest.fixture(autouse=True)
def _memory(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.delenv("DATAWRAP_PARITY_SCALE", raising=False)
    import services.mongodb_service as ms

    monkeypatch.setattr(ms, "_mongodb_service", None)
    yield
    monkeypatch.setattr(ms, "_mongodb_service", None)


JOB: dict[str, Any] = {
    "_id": "job-flag-off", "status": "completed", "updated_at": "2026-11-01T10:00:00+00:00",
    "destination_summary": {"table": "orders", "primary_key_columns": ["id"]},
    "reconciliation": {"passed": True, "source_rows": 10, "target_rows": 10, "phase": "post_write",
                       "source_checksum": "abc", "target_checksum": "abc",
                       "source_checksum_provenance": "independent_source_reread"},
}


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _summary() -> dict[str, Any]:
    from services.parity_report import build_parity_report, compact_summary

    summary = compact_summary(build_parity_report(dict(JOB), reread=False, job_id=JOB["_id"]), dict(JOB))
    return {k: v for k, v in summary.items() if k not in {"generated_at", "at"}}


def test_the_stamp_of_a_job_without_a_block_is_unchanged() -> None:
    from services.parity_report import summary_stamp

    assert summary_stamp(dict(JOB)) == {"job_status": "completed", "job_updated_at": "2026-11-01T10:00:00+00:00"}
    assert _digest(summary_stamp(dict(JOB))) == STAMP_DIGEST


def test_the_stored_summary_of_a_job_without_a_block_is_unchanged() -> None:
    assert _digest(_summary()) == SUMMARY_DIGEST


STAMP_DIGEST = "f879e6164881f72d5f80344b12b8aeba19ee21a4df41719afba2fe259c96db52"  # dc61397ae
SUMMARY_DIGEST = "9539569ab81060372bb41740e6e0c72ca590b1268ed25222ef7433a032298790"  # dc61397ae
