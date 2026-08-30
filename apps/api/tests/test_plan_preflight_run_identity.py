"""A plan preflight verdict must be citable: it carries a real run id, and the
id handed back is the id stored in plan history (Execute is unlocked against it).
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.transfer_plan_service import run_plan_preflight, sync_plan_mappings
from services.transfer_plan_store import create_plan, get_plan


@pytest.fixture(autouse=True)
def isolated_stores(tmp_path, monkeypatch):
    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json")
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    yield


def _plan_id() -> str:
    plan = create_plan({
        "name": "pg-pg",
        "source": {"kind": "database", "format": "postgresql", "connector_id": "src"},
        "destination": {"kind": "database", "format": "postgresql", "connector_id": "dst"},
        "source_columns": ["id", "amt"],
        "source_schema": {"id": "INTEGER", "amt": "DECIMAL(10,4)"},
        "target_columns": ["id", "amt"],
        "target_schema": {"id": "INTEGER", "amt": "DECIMAL(10,4)"},
        "sample_rows": [{"id": 1, "amt": "12.3456"}],
        "policies": {"validation_mode": "strict", "sync_mode": "full_refresh_append"},
    })
    sync_plan_mappings(plan.id, [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "amt", "target": "amt", "confidence": 0.99},
    ])
    return plan.id


def _patched_preflight(result: dict):
    return patch(
        "services.transfer_plan_service._preflight",
        return_value=(
            lambda pf, *_a, **_k: pf,
            lambda mode: 0.85,
            lambda **_k: {
                "connected": True,
                "table_exists": True,
                "can_create_table": True,
                "db_type": "postgresql",
                "column_types": {"id": "INTEGER", "amt": "NUMERIC(10,4)"},
                "message": "ok",
            },
            lambda **_k: dict(result),
            lambda **_k: [],
        ),
    )


PASSING = {
    "passed": True,
    "passed_count": 8,
    "total_gates": 8,
    "readiness_score": 100.0,
    "gates": [],
    "blockers": [],
}


def test_plan_preflight_returns_a_run_id():
    plan_id = _plan_id()
    with _patched_preflight(PASSING), \
         patch("services.transfer_plan_service.read_source_database", side_effect=Exception("skip")):
        out = run_plan_preflight(plan_id)

    run_id = out.get("run_id")
    assert isinstance(run_id, str) and run_id.strip(), "plan preflight returned no run id"
    # pf_local_ is reserved for browser-only preflight and never unlocks Execute.
    assert not run_id.startswith("pf_local_")


def test_returned_run_id_is_the_id_in_plan_history():
    plan_id = _plan_id()
    with _patched_preflight(PASSING), \
         patch("services.transfer_plan_service.read_source_database", side_effect=Exception("skip")):
        out = run_plan_preflight(plan_id)

    plan = get_plan(plan_id)
    assert plan is not None
    runs = plan.preflight_runs
    assert runs, "preflight run was not persisted"
    assert runs[-1]["id"] == out["run_id"]


def test_engine_supplied_run_id_is_preserved():
    plan_id = _plan_id()
    supplied = dict(PASSING, run_id="pf_engine_supplied_1")
    with _patched_preflight(supplied), \
         patch("services.transfer_plan_service.read_source_database", side_effect=Exception("skip")):
        out = run_plan_preflight(plan_id)

    assert out["run_id"] == "pf_engine_supplied_1"
    plan = get_plan(plan_id)
    assert plan is not None
    assert plan.preflight_runs[-1]["id"] == "pf_engine_supplied_1"


def test_each_run_gets_its_own_id():
    plan_id = _plan_id()
    with _patched_preflight(PASSING), \
         patch("services.transfer_plan_service.read_source_database", side_effect=Exception("skip")):
        first = run_plan_preflight(plan_id)
        second = run_plan_preflight(plan_id)

    assert first["run_id"] != second["run_id"]
