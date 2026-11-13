"""Plan history must name the mapping revision an operation actually used.

Validate, Map and Execute each read a plan, work for a while, then write back.
A UI save made in between must neither be overwritten nor be credited with a
verdict it never received.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from services.transfer_plan_store import (
    add_preflight_run,
    create_plan,
    get_plan,
    sync_ui_mappings,
)


@pytest.fixture(autouse=True)
def isolated_stores(tmp_path, monkeypatch):
    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json")
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    yield


def _maps(target: str) -> list[dict]:
    return [{"source": "username", "target": target, "confidence": 0.99}]


def _plan(*, mapped: bool = True) -> str:
    plan = create_plan(
        {
            "name": "binding",
            "source": {"kind": "file", "format": "csv"},
            "destination": {"kind": "database", "format": "postgresql", "table": "people"},
            "source_columns": ["username"],
            "source_schema": {"username": "VARCHAR"},
            "target_columns": ["username", "login"],
            "target_schema": {"username": "VARCHAR", "login": "VARCHAR"},
            "sample_rows": [{"username": "a"}],
        }
    )
    if mapped:
        sync_ui_mappings(plan.id, _maps("username"))
    return plan.id


# ---------------------------------------------------------------------------
# Validate (preflight) is recorded against the revision it checked
# ---------------------------------------------------------------------------


def test_a_preflight_run_names_the_revision_it_checked_not_the_one_active_at_write_time():
    plan_id = _plan()
    checked = get_plan(plan_id).active_revision()
    sync_ui_mappings(plan_id, _maps("login"))  # a UI save lands while Validate runs

    add_preflight_run(
        plan_id,
        {"passed": True, "run_id": "pf_1"},
        mapping_version=checked.version,
        mapping_hash=checked.mapping_hash,
    )
    plan = get_plan(plan_id)
    run = plan.preflight_runs[-1]
    assert (run["mapping_version"], run["mapping_hash"]) == (1, checked.mapping_hash)
    # v2 was never validated: the plan must not read "preflight passed".
    assert plan.active_version == 2
    assert plan.status == "mapped"


def test_a_preflight_run_of_the_still_active_revision_sets_the_status():
    plan_id = _plan()
    checked = get_plan(plan_id).active_revision()
    add_preflight_run(
        plan_id,
        {"passed": False, "run_id": "pf_2"},
        mapping_version=checked.version,
        mapping_hash=checked.mapping_hash,
    )
    plan = get_plan(plan_id)
    assert plan.status == "preflight_failed"
    assert plan.preflight_runs[-1]["mapping_version"] == 1


def _run_validate(plan_id: str, during=None) -> dict:
    """run_plan_preflight with the engines stubbed; ``during`` runs mid-Validate."""
    from services.transfer_plan_service import run_plan_preflight

    def fake_file_preflight(**_kw):
        if during:
            during()
        return {"passed": True, "passed_count": 1, "total_gates": 1, "gates": [], "blockers": []}

    with patch("services.transfer_plan_service._preflight") as engines, patch(
        "src.transfer.adapters.read_source_database", side_effect=Exception("skip")
    ):
        engines.return_value = (
            lambda pf, *_a, **_k: pf,
            lambda mode: 0.85,
            lambda **_k: {
                "connected": True,
                "table_exists": True,
                "can_create_table": True,
                "db_type": "postgresql",
                "column_types": {"username": "VARCHAR", "login": "VARCHAR"},
                "message": "ok",
            },
            fake_file_preflight,
            lambda **_k: [],
        )
        return run_plan_preflight(plan_id)


def test_validate_records_the_revision_it_read_when_a_ui_save_lands_meanwhile():
    plan_id = _plan()
    v1_hash = get_plan(plan_id).active_revision().mapping_hash
    _run_validate(plan_id, during=lambda: sync_ui_mappings(plan_id, _maps("login")))
    plan = get_plan(plan_id)
    run = plan.preflight_runs[-1]
    assert (run["mapping_version"], run["mapping_hash"]) == (1, v1_hash)
    assert plan.active_version == 2 and plan.status == "mapped"
