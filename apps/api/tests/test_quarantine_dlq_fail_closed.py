"""Module 5 — Quarantine DLQ must fail closed (never best-effort lose rejects)."""

from __future__ import annotations

import pytest

import services.quarantine_dlq as dlq


def test_no_rejects_is_durable_vacuously():
    summary = {"rejected_details": []}
    dlq.assert_quarantine_durable_or_raise(summary)
    assert dlq.persist_job_quarantine_outcome(summary)["ok"] is True


def test_durable_true_passes():
    summary = {
        "rejected_details": [{"row": 1, "reason": "bad"}],
        "quarantine_durable": True,
    }
    dlq.assert_quarantine_durable_or_raise(summary)
    assert dlq.persist_job_quarantine_outcome(summary)["ok"] is True


def test_durable_false_with_rejects_fails_closed():
    summary = {
        "rejected_details": [{"row": 1, "reason": "bad"}],
        "quarantine_durable": False,
        "quarantine_dlq_error": "disk full",
    }
    # Bind exception class from the live module (suite may reload services.*).
    with pytest.raises(dlq.QuarantineDlqLostError) as ei:
        dlq.assert_quarantine_durable_or_raise(summary)
    assert "disk full" in str(ei.value).lower() or "durable" in str(ei.value).lower()
    out = dlq.persist_job_quarantine_outcome(summary)
    assert out["ok"] is False
    assert out["fail_closed"] is True


def test_missing_durable_flag_with_rejects_fails_closed():
    """Unknown durability with rejects is not a silent success."""
    summary = {"rejected_details": [{"row": 1}]}
    with pytest.raises(dlq.QuarantineDlqLostError):
        dlq.assert_quarantine_durable_or_raise(summary)
