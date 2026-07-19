"""Plan preflight must honor policies.validation_mode (not default strict)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.transfer_plan_service import run_plan_preflight, sync_plan_mappings
from services.transfer_plan_store import create_plan


@pytest.fixture(autouse=True)
def isolated_stores(tmp_path, monkeypatch):
    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json")
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    yield


def test_run_plan_preflight_passes_validation_mode_to_engine():
    plan = create_plan({
        "name": "mongo-sf",
        "source": {"kind": "database", "format": "mongodb", "connector_id": "src"},
        "destination": {"kind": "database", "format": "snowflake", "connector_id": "dst"},
        "source_columns": ["status", "agent"],
        "source_schema": {"status": "VARCHAR", "agent": "VARCHAR"},
        "target_columns": ["posted_date_estimated", "department"],
        "target_schema": {"posted_date_estimated": "BOOLEAN", "department": "VARCHAR"},
        "sample_rows": [{"status": "active", "agent": "a"}],
        "policies": {"validation_mode": "balanced", "sync_mode": "full_refresh_append"},
    })
    sync_plan_mappings(plan.id, [
        {"source": "status", "target": "posted_date_estimated", "confidence": 0.59},
        {"source": "agent", "target": "department", "confidence": 0.59},
    ])

    captured: dict = {}

    def fake_run_file_preflight(**kwargs):
        captured.update(kwargs)
        return {
            "passed": False,
            "passed_count": 7,
            "total_gates": 8,
            "readiness_score": 87.5,
            "gates": [],
            "blockers": [],
        }

    with patch("services.transfer_plan_service._preflight") as mock_pf, \
         patch("services.transfer_plan_service.read_source_database", side_effect=Exception("skip")):
        mock_pf.return_value = (
            lambda pf, *_a, **_k: pf,
            lambda mode: {"balanced": 0.75, "strict": 0.85}.get((mode or "").lower(), 0.85),
            lambda **_k: {
                "connected": True,
                "table_exists": True,
                "can_create_table": True,
                "db_type": "snowflake",
                "column_types": {"posted_date_estimated": "BOOLEAN", "department": "VARCHAR"},
                "message": "ok",
            },
            fake_run_file_preflight,
            lambda **_k: [],
        )
        run_plan_preflight(plan.id)

    assert captured.get("validation_mode") == "balanced"
    assert captured.get("confidence_threshold") == 0.75
