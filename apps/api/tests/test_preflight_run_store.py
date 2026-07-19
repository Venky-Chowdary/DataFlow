"""Preflight run IDs for Data Pilot / Validate tracking."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import preflight_run_store as store  # noqa: E402


@pytest.fixture()
def isolated_store(tmp_path, monkeypatch):
    path = tmp_path / "preflight_runs.jsonl"
    monkeypatch.setattr(store, "STORE_PATH", path)
    return path


def test_save_and_get_preflight_run(isolated_store):
    result = {
        "passed": False,
        "passed_count": 7,
        "total_gates": 8,
        "readiness_score": 87.5,
        "gates": [{"id": "g5_dry_run", "status": "block", "message": "format-control"}],
        "blockers": [
            {
                "id": "g5_dry_run",
                "message": "Dry-run / integrity failed: format-control character detected",
                "guidance": {"fix": "Apply strip_controls"},
            }
        ],
    }
    enriched = store.save_preflight_run(
        result,
        source_label="mongodb",
        dest_label="snowflake",
        validation_mode="balanced",
        route={"row_count": 35917},
    )
    assert enriched["run_id"].startswith("pf_")
    loaded = store.get_preflight_run(enriched["run_id"])
    assert loaded is not None
    assert loaded["passed"] is False
    assert loaded["source_label"] == "mongodb"
    assert any(r["kind"] == "normalize_control_chars" for r in loaded["suggested_remediations"])


def test_list_preflight_runs(isolated_store):
    for i in range(3):
        store.save_preflight_run({"passed": True, "passed_count": 8, "total_gates": 8, "gates": [], "blockers": []})
    rows = store.list_preflight_runs(limit=10)
    assert len(rows) == 3
