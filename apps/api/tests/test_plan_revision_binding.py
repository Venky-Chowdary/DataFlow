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


def _run_validate(plan_id: str, during=None, passed: bool = True) -> dict:
    """run_plan_preflight with the engines stubbed; ``during`` runs mid-Validate."""
    from services.transfer_plan_service import run_plan_preflight

    def fake_file_preflight(**_kw):
        if during:
            during()
        return {"passed": passed, "passed_count": 1, "total_gates": 1, "gates": [], "blockers": []}

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


# ---------------------------------------------------------------------------
# Map appends only onto the revision it started from
# ---------------------------------------------------------------------------


def _map_with_a_ui_save_during_the_pipeline(plan_id: str):
    from services.transfer_plan_service import run_plan_mapping

    def pipeline(*_args, **_kwargs):
        sync_ui_mappings(plan_id, _maps("login"))  # the operator saves while Map runs
        return {"mappings": _maps("username"), "validation": {"passed": True, "issues": []}}

    with patch("services.mapping_pipeline.run_mapping_pipeline", side_effect=pipeline):
        return run_plan_mapping(plan_id, use_llm=False)


def test_map_does_not_replace_a_ui_save_made_while_it_ran():
    from services.transfer_plan_store import PlanVersionConflict

    plan_id = _plan()
    with pytest.raises(PlanVersionConflict) as caught:
        _map_with_a_ui_save_during_the_pipeline(plan_id)
    assert (caught.value.base_version, caught.value.current_version) == (1, 2)
    plan = get_plan(plan_id)
    assert [r.version for r in plan.revisions] == [1, 2]
    assert plan.active_version == 2
    assert plan.active_revision().mappings == _maps("login")


def test_map_route_answers_409_when_the_plan_changed_while_it_ran():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.routers import transfer_router

    plan_id = _plan()
    app = FastAPI()
    app.include_router(transfer_router.router, prefix="/api/v1")

    def pipeline(*_args, **_kwargs):
        sync_ui_mappings(plan_id, _maps("login"))
        return {"mappings": _maps("username"), "validation": {"passed": True, "issues": []}}

    with patch("services.mapping_pipeline.run_mapping_pipeline", side_effect=pipeline):
        res = TestClient(app).post(f"/api/v1/transfer/plans/{plan_id}/map", json={"use_llm": False})
    assert res.status_code == 409, res.text
    detail = res.json()["detail"]
    assert detail["code"] == "plan_version_conflict"
    assert (detail["base_version"], detail["current_version"]) == (1, 2)
    assert "newer mapping revision" in detail["message"]
    assert get_plan(plan_id).active_revision().mappings == _maps("login")


def test_map_without_a_concurrent_edit_still_appends_and_activates():
    from services.transfer_plan_service import run_plan_mapping

    plan_id = _plan()
    result = {"mappings": _maps("login"), "validation": {"passed": True, "issues": []}}
    with patch("services.mapping_pipeline.run_mapping_pipeline", return_value=result):
        out = run_plan_mapping(plan_id, use_llm=False)
    assert out["version"] == 2
    assert get_plan(plan_id).active_revision().mappings == _maps("login")


# ---------------------------------------------------------------------------
# Execute's recovery sync does not land over a save it did not see
# ---------------------------------------------------------------------------


def test_execute_recovery_sync_keeps_a_ui_save_made_after_it_read_the_plan(monkeypatch):
    import services.transfer_plan_service as service

    plan_id = _plan(mapped=False)
    real_get_plan = service.get_plan
    calls = {"n": 0}

    def stale_then_saved(pid):
        snapshot = real_get_plan(pid)
        calls["n"] += 1
        if calls["n"] == 1:
            sync_ui_mappings(pid, _maps("login"))  # the Studio save lands just after the read
        return snapshot

    monkeypatch.setattr(service, "get_plan", stale_then_saved)
    payload = service.merge_plan_into_run(plan_id, request_mappings=_maps("username"))
    plan = get_plan(plan_id)
    assert [r.version for r in plan.revisions] == [1], "the request mappings were appended over the save"
    assert plan.active_revision().mappings == _maps("login")
    assert [m["target"] for m in payload["mappings"]] == ["login"]


def test_execute_recovery_sync_still_saves_request_mappings_onto_an_empty_draft():
    import services.transfer_plan_service as service

    plan_id = _plan(mapped=False)
    payload = service.merge_plan_into_run(plan_id, request_mappings=_maps("username"))
    assert get_plan(plan_id).active_revision().mappings == _maps("username")
    assert [m["target"] for m in payload["mappings"]] == ["username"]


# ---------------------------------------------------------------------------
# base_version is compared with the newest revision version (head), exactly
# ---------------------------------------------------------------------------


def test_a_base_version_ahead_of_the_newest_revision_is_a_conflict():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.routers import transfer_router

    plan_id = _plan()
    app = FastAPI()
    app.include_router(transfer_router.router, prefix="/api/v1")
    res = TestClient(app).post(
        f"/api/v1/transfer/plans/{plan_id}/mappings",
        json={"mappings": _maps("login"), "base_version": 5},
    )
    assert res.status_code == 409, res.text
    assert (res.json()["detail"]["base_version"], res.json()["detail"]["current_version"]) == (5, 1)
    # doc_version is not what base_version is compared with.
    plan = get_plan(plan_id)
    assert plan.doc_version != plan.head_version()
    res = TestClient(app).post(
        f"/api/v1/transfer/plans/{plan_id}/mappings",
        json={"mappings": _maps("login"), "base_version": plan.doc_version},
    )
    assert res.status_code == 409


def test_the_request_model_documents_what_base_version_is_compared_with():
    from src.routers.transfer_router import SyncMappingsRequest

    text = SyncMappingsRequest.model_fields["base_version"].description
    assert "revisions" in text and "doc_version" in text and "409" in text


# ---------------------------------------------------------------------------
# active_version that names no revision (legacy data) / approving one
# ---------------------------------------------------------------------------


def test_validate_sets_the_status_when_active_version_names_no_revision(isolated_stores):
    """A stale active_version falls back to the newest revision, which is what
    Validate checks and Execute runs: its verdict must become the status."""
    import json

    from services import transfer_plan_store as store

    plan_id = _plan()
    sync_ui_mappings(plan_id, _maps("login"))
    raw = json.loads(store.STORE_PATH.read_text())
    raw["plans"][0]["active_version"] = 99  # legacy / hand-edited store
    raw["plans"][0]["status"] = "approved"
    store.STORE_PATH.write_text(json.dumps(raw))

    _run_validate(plan_id, passed=False)
    plan = get_plan(plan_id)
    assert plan.preflight_runs[-1]["mapping_version"] == 2
    assert plan.status == "preflight_failed"


def test_approving_a_version_that_does_not_exist_is_refused():
    from services.transfer_plan_store import UnknownPlanVersion, approve_plan_version

    plan_id = _plan()
    before = get_plan(plan_id)
    with pytest.raises(UnknownPlanVersion):
        approve_plan_version(plan_id, 99)
    after = get_plan(plan_id)
    assert (after.active_version, after.status, after.doc_version) == (
        before.active_version, before.status, before.doc_version
    )


def test_approve_route_answers_400_for_a_version_that_does_not_exist():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.routers import transfer_router

    plan_id = _plan()
    app = FastAPI()
    app.include_router(transfer_router.router, prefix="/api/v1")
    client = TestClient(app)
    res = client.post(f"/api/v1/transfer/plans/{plan_id}/approve", params={"version": 99})
    assert res.status_code == 400, res.text
    assert "99" in res.text and "revision" in res.text
    assert get_plan(plan_id).status == "mapped"
    ok = client.post(f"/api/v1/transfer/plans/{plan_id}/approve", params={"version": 1})
    assert ok.status_code == 200, ok.text
    assert get_plan(plan_id).status == "approved"
