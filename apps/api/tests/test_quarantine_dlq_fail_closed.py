"""Module 5 — Quarantine DLQ must fail closed (never best-effort lose rejects)."""

from __future__ import annotations

import pytest

from services.quarantine_dlq import (
    QuarantineDlqLostError,
    assert_quarantine_durable_or_raise,
    persist_job_quarantine_outcome,
)


def test_no_rejects_is_durable_vacuously():
    summary = {"rejected_details": []}
    assert_quarantine_durable_or_raise(summary)
    assert persist_job_quarantine_outcome(summary)["ok"] is True


def test_durable_true_passes():
    summary = {
        "rejected_details": [{"row": 1, "reason": "bad"}],
        "quarantine_durable": True,
    }
    assert_quarantine_durable_or_raise(summary)
    assert persist_job_quarantine_outcome(summary)["ok"] is True


def test_durable_false_with_rejects_fails_closed():
    summary = {
        "rejected_details": [{"row": 1, "reason": "bad"}],
        "quarantine_durable": False,
        "quarantine_dlq_error": "disk full",
    }
    with pytest.raises(QuarantineDlqLostError) as ei:
        assert_quarantine_durable_or_raise(summary)
    assert "disk full" in str(ei.value).lower() or "durable" in str(ei.value).lower()
    out = persist_job_quarantine_outcome(summary)
    assert out["ok"] is False
    assert out["fail_closed"] is True


def test_missing_durable_flag_with_rejects_fails_closed():
    """Unknown durability with rejects is not a silent success."""
    summary = {"rejected_details": [{"row": 1}]}
    with pytest.raises(QuarantineDlqLostError):
        assert_quarantine_durable_or_raise(summary)
