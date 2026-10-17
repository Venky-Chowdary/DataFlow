"""Null-rate baselines compare inside one column and one schema version."""

from __future__ import annotations

import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_quality_history import ColumnProfile, build_load_history_report


def _profile(name: str, dtype: str, null_count: int, count: int = 100) -> ColumnProfile:
    return ColumnProfile(column=name, dtype=dtype, count=count, null_count=null_count)


def _run(columns: dict[str, ColumnProfile]) -> dict:
    return {
        "row_count": 100,
        "columns": {name: asdict(col) for name, col in columns.items()},
    }


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
