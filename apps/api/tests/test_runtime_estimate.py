"""Runtime estimation must project from measured throughput or say nothing."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.runtime_estimate import (  # noqa: E402
    MAX_MARKS,
    MIN_INTERVALS,
    append_throughput_mark,
    estimate_before_run,
    estimate_for_job_doc,
    estimate_running_job,
)

T0 = datetime(2026, 8, 10, 9, 0, tzinfo=timezone.utc)


def _ckpts(marks: list[tuple[int, int]]) -> list[dict[str, Any]]:
    """(seconds_after_T0, cumulative_rows) -> the store's checkpoint shape."""
    return [
        {"chunk": i, "total": 0, "rows": rows, "at": (T0 + timedelta(seconds=s)).isoformat()}
        for i, (s, rows) in enumerate(marks)
    ]


def _job(**kw: Any) -> dict[str, Any]:
    base = {
        "job_id": "job_1",
        "status": "running",
        "source": "postgresql",
        "destination": "mysql",
        "rows_processed": 0,
        "total_rows": 0,
        "checkpoints": [],
    }
    base.update(kw)
    return base


def test_steady_throughput_projects_the_remaining_window() -> None:
    est = estimate_running_job(
        _job(
            rows_processed=30_000,
            total_rows=100_000,
            checkpoints=_ckpts([(0, 0), (10, 10_000), (20, 20_000), (30, 30_000)]),
        ),
        now=T0 + timedelta(seconds=30),
    )
    assert est.available and est.basis == "observed_checkpoints"
    assert est.rows_per_second_p50 == 1000.0
    assert est.remaining_seconds_p50 == 70.0
    assert est.finishes_at_p50 == (T0 + timedelta(seconds=100)).isoformat()


def test_slow_intervals_widen_the_p90_but_not_the_p50() -> None:
    """A stall must lengthen the pessimistic arm, not be averaged away."""
    est = estimate_running_job(
        _job(
            rows_processed=40_000,
            total_rows=140_000,
            checkpoints=_ckpts(
                [(0, 0), (10, 10_000), (20, 20_000), (30, 30_000), (70, 40_000)]
            ),
        ),
        now=T0 + timedelta(seconds=70),
    )
    assert est.rows_per_second_p50 == 1000.0
    assert est.rows_per_second_p10 == 250.0
    assert est.remaining_seconds_p50 == 100.0
    assert est.remaining_seconds_p90 == 400.0


def test_unknown_population_is_unavailable_not_zero() -> None:
    est = estimate_running_job(
        _job(rows_processed=5_000, checkpoints=_ckpts([(0, 0), (10, 5_000)])),
        now=T0 + timedelta(seconds=10),
    )
    assert not est.available
    assert est.remaining_seconds_p50 is None
    assert "not known" in est.reason


def test_too_few_intervals_declines_rather_than_extrapolating_one_batch() -> None:
    est = estimate_running_job(
        _job(rows_processed=1_000, total_rows=1_000_000, checkpoints=_ckpts([(0, 0), (5, 1_000)])),
        now=T0 + timedelta(seconds=5),
    )
    assert not est.available
    assert str(MIN_INTERVALS) in est.reason


def test_a_resume_restart_does_not_poison_the_rate() -> None:
    """Rows drop back after a resume; that interval is dropped, not negative."""
    est = estimate_running_job(
        _job(
            rows_processed=25_000,
            total_rows=100_000,
            checkpoints=_ckpts(
                [(0, 0), (10, 10_000), (20, 20_000), (30, 5_000), (40, 15_000), (50, 25_000)]
            ),
        ),
        now=T0 + timedelta(seconds=50),
    )
    assert est.available
    assert est.rows_per_second_p50 == 1000.0
    assert est.intervals_observed == 4


def test_overshooting_the_declared_population_clamps_at_zero() -> None:
    est = estimate_running_job(
        _job(
            rows_processed=12_000,
            total_rows=10_000,
            checkpoints=_ckpts([(0, 0), (10, 6_000), (20, 12_000)]),
        ),
        now=T0 + timedelta(seconds=20),
    )
    assert est.rows_remaining == 0
    assert est.remaining_seconds_p50 == 0.0
    assert any("clamped" in note for note in est.notes)


def test_prior_runs_of_the_same_route_size_the_cutover_window() -> None:
    history = [
        _job(
            status="completed",
            source="postgresql",
            destination="mysql",
            checkpoints=_ckpts([(0, 0), (10, 20_000), (20, 40_000)]),
        ),
        _job(
            status="completed",
            source="postgresql",
            destination="snowflake",
            checkpoints=_ckpts([(0, 0), (10, 1), (20, 2)]),
        ),
    ]
    est = estimate_before_run(
        history, source="postgresql", destination="mysql", rows_total=2_000_000, now=T0
    )
    assert est.available and est.basis == "prior_runs" and est.runs_observed == 1
    assert est.rows_per_second_p50 == 2000.0
    assert est.remaining_seconds_p50 == 1000.0


def test_a_failed_prior_run_is_not_evidence_of_throughput() -> None:
    history = [
        _job(status="failed", checkpoints=_ckpts([(0, 0), (10, 20_000), (20, 40_000)])),
    ]
    est = estimate_before_run(
        history, source="postgresql", destination="mysql", rows_total=1_000, now=T0
    )
    assert not est.available
    assert "postgresql → mysql" in est.reason


def test_unmeasured_source_count_refuses_a_cutover_window() -> None:
    est = estimate_before_run(
        [], source="postgresql", destination="mysql", rows_total=None, now=T0
    )
    assert not est.available
    assert "unmeasured" in est.reason


def test_marks_are_bounded_to_the_trailing_window() -> None:
    marks: Any = None
    for i in range(MAX_MARKS + 25):
        marks = append_throughput_mark(marks, i * 1000, now=T0 + timedelta(seconds=i))
    assert len(marks) == MAX_MARKS
    assert marks[-1]["rows"] == (MAX_MARKS + 24) * 1000


def test_an_unreadable_row_count_records_no_mark() -> None:
    assert append_throughput_mark([], None) is None
    assert append_throughput_mark([], "n/a") is None
    assert append_throughput_mark([], -5) is None


def test_job_document_field_names_project_a_window() -> None:
    doc = {
        "records_processed": 400_000,
        "total_rows": 1_000_000,
        "throughput_marks": [
            {"rows": 0, "at": T0.isoformat()},
            {"rows": 200_000, "at": (T0 + timedelta(seconds=100)).isoformat()},
            {"rows": 400_000, "at": (T0 + timedelta(seconds=200)).isoformat()},
        ],
    }
    est = estimate_for_job_doc(doc, now=T0 + timedelta(seconds=200))
    assert est["available"] is True
    assert est["rows_per_second_p50"] == 2000.0
    assert est["rows_remaining"] == 600_000
    assert est["remaining_seconds_p50"] == 300.0


def test_job_document_without_marks_reports_why_not() -> None:
    est = estimate_for_job_doc({"records_processed": 10, "total_rows": 1_000})
    assert est["available"] is False
    assert "checkpoint intervals" in est["reason"]
