"""Execute must not fail when Studio points at an empty draft plan but sends mappings."""

from __future__ import annotations

from services.transfer_plan_service import build_run_payload, merge_plan_into_run
from services.transfer_plan_store import create_plan, get_plan


def test_merge_plan_into_run_recovers_empty_draft_with_request_mappings(tmp_path, monkeypatch):
    import services.transfer_plan_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "plans.json")

    plan = create_plan(
        {
            "name": "countries.json",
            "source": {"kind": "file", "format": "json", "filename": "countries.json"},
            "destination": {"kind": "database", "format": "redis", "collection": "countries"},
            "source_columns": ["name", "code"],
            "source_schema": {"name": "string", "code": "string"},
        }
    )
    assert plan.active_revision() is None

    try:
        build_run_payload(plan.id)
        raise AssertionError("expected empty draft to fail build_run_payload")
    except ValueError as exc:
        assert "mapping" in str(exc).lower()

    mappings = [
        {"source": "name", "target": "name", "confidence": 0.93},
        {"source": "code", "target": "code", "confidence": 0.93},
    ]
    payload = merge_plan_into_run(plan.id, request_mappings=mappings)
    assert payload["mappings"] == mappings or len(payload["mappings"]) == 2
    assert payload["plan_id"] == plan.id

    reloaded = get_plan(plan.id)
    assert reloaded is not None
    assert reloaded.active_revision() is not None
    assert len(reloaded.active_revision().mappings) == 2


def test_merge_plan_into_run_still_fails_without_request_mappings(tmp_path, monkeypatch):
    import services.transfer_plan_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "plans.json")
    plan = create_plan({"name": "empty", "source": {}, "destination": {}})
    try:
        merge_plan_into_run(plan.id, request_mappings=[])
        raise AssertionError("expected failure")
    except ValueError as exc:
        assert "mapping" in str(exc).lower()
