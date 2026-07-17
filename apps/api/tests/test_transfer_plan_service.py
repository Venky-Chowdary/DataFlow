"""Transfer plan service orchestration tests."""

import pytest

from services.transfer_plan_service import patch_plan, sync_plan_mappings
from services.transfer_plan_store import create_plan, get_plan


@pytest.fixture(autouse=True)
def isolated_plan_store(tmp_path, monkeypatch):
    path = tmp_path / "transfer_plans.json"
    audit = tmp_path / "audit.jsonl"
    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", path)
    monkeypatch.setattr("services.audit_log.STORE_PATH", audit)
    yield path


def test_patch_plan_updates_destination():
    plan = create_plan({
        "source_columns": ["id"],
        "source_schema": {"id": "INTEGER"},
        "destination": {"format": "postgresql", "database": "src_db"},
    })
    updated = patch_plan(plan.id, {
        "destination": {"format": "postgresql", "database": "prod_db", "table": "users"},
        "target_columns": ["user_id"],
        "target_schema": {"user_id": "BIGINT"},
    })
    assert updated.destination["database"] == "prod_db"
    assert updated.target_columns == ["user_id"]


def test_sync_plan_mappings_creates_ui_revision():
    plan = create_plan({
        "source_columns": ["order_id", "amount"],
        "source_schema": {"order_id": "INTEGER", "amount": "DECIMAL"},
        "target_columns": ["order_id", "amount"],
    })
    synced = sync_plan_mappings(plan.id, [
        {"source": "order_id", "target": "order_id", "confidence": 1.0},
        {"source": "amount", "target": "payment_amount", "confidence": 0.98},
    ])
    rev = synced.active_revision()
    assert rev is not None
    assert len(rev.mappings) == 2
    assert rev.plan_summary.get("edited_in_ui") is True
    loaded = get_plan(plan.id)
    assert loaded is not None
    assert loaded.status == "mapped"
