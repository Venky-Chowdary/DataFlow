"""Prove write-time quarantine details are not stripped before the UI can see them."""

from __future__ import annotations

from services.quarantine_from_preflight import merge_job_quarantine
from src.transfer.adapters import _writer_diagnostics


def test_merge_prefers_job_rejected_details():
    job = {
        "rejected_details": [
            {"row": 1, "column": "column_5", "value": "bad", "reason": "Incorrect datetime"},
        ],
        "destination_summary": {"rejected_details": []},
    }
    rows = merge_job_quarantine(job)
    assert len(rows) == 1
    assert rows[0]["column"] == "column_5"


def test_merge_falls_back_to_destination_summary():
    job = {
        "rejected_details": [],
        "destination_summary": {
            "rejected_details": [
                {"row": 2, "column": "amt", "value": "x", "reason": "bad int"},
            ]
        },
    }
    rows = merge_job_quarantine(job)
    assert rows[0]["column"] == "amt"


def test_writer_diagnostics_caps_but_keeps_details():
    class _WR:
        rejected_rows = 2
        coerced_null_rows = 0
        warnings = ["w"]
        rejected_details = [
            {"row": i, "column": "c", "value": "v", "reason": "r"} for i in range(250)
        ]

    summary = _writer_diagnostics(_WR())
    assert len(summary["rejected_details"]) == 250
    assert summary["rejected_details"][0]["column"] == "c"
    assert summary["rejected_rows"] == 2
