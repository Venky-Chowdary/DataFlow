"""Null-rate baselines compare inside one column and one schema version."""

from __future__ import annotations

import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_quality_history import (
    NULL_RATE_METRIC,
    ColumnProfile,
    build_load_history_report,
)


def _profile(name: str, dtype: str, null_count: int, count: int = 100) -> ColumnProfile:
    return ColumnProfile(column=name, dtype=dtype, count=count, null_count=null_count)


def _run(columns: dict[str, ColumnProfile], *, metric: str | None = NULL_RATE_METRIC) -> dict:
    # ``save_profile`` stamps the NULL definition on every run it persists.
    run = {
        "row_count": 100,
        "columns": {name: asdict(col) for name, col in columns.items()},
    }
    if metric is not None:
        run["null_rate_metric"] = metric
    return run


def test_stale_schema_does_not_move_the_null_rate():
    current = {
        "status": _profile("status", "string", 46),
        "note": _profile("note", "string", 0),
    }
    prior = _run({"status": _profile("status", "string", 0)})
    report = build_load_history_report(
        current_profile=current,
        prior_runs=[prior],
        current_row_count=100,
    )
    blob = " ".join(report["anomalies"])
    assert "null-rate" not in blob
    assert "46" not in blob


def test_same_schema_still_reports_a_null_rate_shift():
    current = {"status": _profile("status", "string", 46)}
    prior = _run({"status": _profile("status", "string", 0)})
    report = build_load_history_report(
        current_profile=current,
        prior_runs=[prior],
        current_row_count=100,
    )
    assert any("null-rate" in item and "status" in item for item in report["anomalies"])


def test_run_recorded_under_old_null_definition_is_not_a_baseline():
    """D81: pre-v2 runs counted '' and 'null'/'none' text as NULL."""
    current = {"status": _profile("status", "string", 10)}
    legacy = _run({"status": _profile("status", "string", 33)}, metric=None)
    report = build_load_history_report(
        current_profile=current,
        prior_runs=[legacy],
        current_row_count=100,
    )
    assert not any("null-rate" in item for item in report["anomalies"])


def test_small_prior_run_is_not_a_null_rate_baseline():
    """D2: a 5-row prior load cannot carry a null rate for a 100-row load."""
    current = {"status": _profile("status", "string", 0)}
    small = _run({"status": _profile("status", "string", 2, count=5)})
    report = build_load_history_report(
        current_profile=current,
        prior_runs=[small],
        current_row_count=100,
    )
    assert not any("null-rate" in item for item in report["anomalies"])
