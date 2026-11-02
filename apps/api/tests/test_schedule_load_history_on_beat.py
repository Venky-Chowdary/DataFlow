"""Schedule beat copies dest load-history from the job onto the run entry."""

from __future__ import annotations

from datetime import datetime, timezone

from services.schedule_runner import _run_entry


def test_run_entry_attaches_load_history_from_job():
    started = datetime(2026, 8, 29, tzinfo=timezone.utc)
    entry = _run_entry(
        "job-1",
        "completed",
        1,
        started,
        {
            "records_processed": 10,
            "rejected_rows": 0,
            "load_history_report": {"prior_load_count": 3, "anomalies": ["null_rate"]},
        },
    )
    assert entry["load_history_report"]["prior_load_count"] == 3
    assert entry["load_history_report"]["anomalies"] == ["null_rate"]


def test_run_entry_falls_back_to_destination_summary_history():
    started = datetime(2026, 8, 29, tzinfo=timezone.utc)
    entry = _run_entry(
        "job-2",
        "completed",
        1,
        started,
        {
            "destination_summary": {
                "load_history_report": {"prior_load_count": 1, "warning": "volume drift"},
            }
        },
    )
    assert entry["load_history_report"]["warning"] == "volume drift"


def test_run_entry_omits_history_when_absent():
    started = datetime(2026, 8, 29, tzinfo=timezone.utc)
    entry = _run_entry("job-3", "completed", 1, started, {"records_processed": 1})
    assert "load_history_report" not in entry
