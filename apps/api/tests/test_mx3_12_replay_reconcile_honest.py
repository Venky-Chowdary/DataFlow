"""MX3-12 — a replay that re-rejects every row must not claim fidelity.

QA: replay_quarantine(transform_overrides={"flt": "none"}) wrote 0 rows,
re-quarantined the row, and still reported ``reconciliation.passed=true``,
``checksum_match=true``, ``assurance_level=full_checksum`` while its own
``verification_ladder`` said ``passed=false``. Two digests of the empty
written set match by construction; that is not evidence.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_mx3_14_cast_and_continue_distinct as _mx314  # noqa: E402
from test_mx3_14_cast_and_continue_distinct import _pg_reachable, _run  # noqa: E402


@pytest.fixture
def replay_route():
    yield from _mx314.cast_route.__wrapped__()


def test_empty_keyed_write_with_rejects_is_not_full_checksum():
    from services.reconcile_coverage import WRITTEN_BATCH_KEYS
    from services.reconciliation import reconcile

    report = reconcile(
        source_rows=1,
        target_rows=3,
        source_checksum="e3b0c442",
        target_checksum="e3b0c442",
        rejected_rows=1,
        allow_extra_rows=True,
        checksum_scope=WRITTEN_BATCH_KEYS,
    )
    assert report.passed is False
    assert report.checksum_match is not True
    assert report.assurance_level != "full_checksum"
    assert "wrote 0 row(s)" in report.message


def test_keyed_write_that_landed_rows_still_verifies():
    from services.reconcile_coverage import WRITTEN_BATCH_KEYS
    from services.reconciliation import reconcile

    report = reconcile(
        source_rows=2,
        target_rows=5,
        source_checksum="abc",
        target_checksum="abc",
        rejected_rows=1,
        allow_extra_rows=True,
        checksum_scope=WRITTEN_BATCH_KEYS,
    )
    assert report.passed is True
    assert report.assurance_level == "full_checksum"


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_replay_that_rejects_again_does_not_report_verified(replay_route):
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    job, _rows = _run(replay_route, {"execution_policy": "QUARANTINE_ROW"}, "r")
    assert job["status"] == "completed_with_quarantine", job
    rp = DataPilotTools().execute(
        "replay_quarantine",
        {"job_id": job["id"], "transform_overrides": {"flt": "none"}},
    )
    assert rp.success, rp.error
    out = spend_pilot_ack(rp.output["ack_id"], "mx312-test")
    assert int(out.get("rows_written") or 0) == 0, out
    rec = out["reconciliation"]
    ladder = rec.get("verification_ladder") or {}
    assert rec.get("passed") is False, rec
    assert rec.get("checksum_match") is not True, rec
    assert rec.get("assurance_level") != "full_checksum", rec
    assert "verified" not in str(rec.get("message") or "").lower().split("nothing was verified")[-1], rec
    if ladder and not ladder.get("skipped"):
        assert bool(rec.get("passed")) == bool(ladder.get("passed")), (rec, ladder)
    assert out["quarantine_closure"]["verdict"] != "closed", out["quarantine_closure"]
