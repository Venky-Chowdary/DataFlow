"""Pilot ground truth — verification and exact statuses are counted exactly.

Review H2: "how many jobs failed verification" and "which jobs failed gate 8"
were answered with the failed-status count, "completed with errors" and
"completed with warnings" with the whole completed class, and "did not
complete" / "not running" with the opposite of what was asked. Verification
words now drive the store's own ``verification`` filter, an exact status is
read from the store's per-status census, and a negation is refused by name.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc)

SPEC = {
    "completed": 4, "completed_with_warnings": 2, "completed_with_errors": 3, "partial": 2,
    "failed": 3, "cancelled": 2, "running": 2, "pending": 1, "queued": 1, "retrying": 1, "paused": 1,
}


@pytest.fixture
def ledger(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    minute = 10
    for status, n in SPEC.items():
        for _ in range(n):
            minute += 1
            pt.seed_job(status, at=NOW - timedelta(minutes=minute))
    # Completed but Gate-8 verification failed; a failed run whose verdict failed too.
    pt.seed_job("completed", at=NOW - timedelta(minutes=5), reconciliation={"passed": False, "rows_differ": 7})
    pt.seed_job("completed", at=NOW - timedelta(minutes=6), reconciliation={"passed": False})
    pt.seed_job("failed", at=NOW - timedelta(minutes=7), reconciliation={"passed": False})
    pt.seed_job("completed", at=NOW - timedelta(minutes=8), reconciliation={"passed": True})
    return pt.agent()


@pytest.mark.parametrize("question,verdict", [
    ("how many jobs failed verification", "failed"),
    ("which jobs failed gate 8", "failed"),
    ("how many jobs failed reconciliation", "failed"),
    ("how many jobs passed verification", "passed"),
])
def test_verification_words_use_the_verification_filter(ledger, question, verdict):
    response = pt.ask(ledger, question)
    truth = int(pt.mongo().count_job_matches(verification=verdict) or 0)
    failed_status = int(pt.mongo().count_job_matches(status="failed") or 0)
    assert truth != failed_status, "fixture must tell the two counts apart"
    assert pt.bold_ints(response.answer)[:1] == [truth], response.answer
    assert "verification" in response.answer.lower()
    assert pt.grounded(response)


@pytest.mark.parametrize("question,status", [
    ("how many jobs completed with errors", "completed_with_errors"),
    ("how many jobs completed with warnings", "completed_with_warnings"),
    ("which jobs are partial", "partial"),
    ("how many jobs are paused", "paused"),
    ("how many jobs are retrying", "retrying"),
])
def test_exact_status_is_counted_exactly(ledger, question, status):
    response = pt.ask(ledger, question)
    truth = int(pt.job_truth()["by_status"].get(status) or 0)
    assert truth == SPEC[status]
    assert pt.bold_ints(response.answer)[:1] == [truth], response.answer
    assert pt.grounded(response)


@pytest.mark.parametrize("question,phrase", [
    ("how many jobs did not complete", "not"),
    ("how many jobs are not running", "not"),
    ("how many jobs didn't fail today", "didn't"),
    ("which jobs are stuck", "stuck"),
    ("how many jobs timed out", "timed out"),
    ("how many unsuccessful jobs", "unsuccessful"),
])
def test_negation_and_unknown_states_are_refused_by_name(ledger, question, phrase):
    response = pt.ask(ledger, question)
    assert phrase in response.answer.lower(), response.answer
    assert "can't filter" in response.answer.lower(), response.answer
    assert not pt.grounded(response)
    assert pt.bold_ints(response.answer) == [], response.answer
