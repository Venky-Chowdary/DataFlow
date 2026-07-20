"""Tests for persisted transfer plans."""

import pytest

from services.schema_fingerprint import fingerprint_mappings, fingerprint_schema
from services.transfer_plan_store import (
    add_mapping_revision,
    add_preflight_run,
    approve_plan_version,
    create_plan,
    get_plan,
)


@pytest.fixture(autouse=True)
def isolated_plan_store(tmp_path, monkeypatch):
    path = tmp_path / "transfer_plans.json"
    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", path)
    yield path


def test_create_and_get_plan():
    plan = create_plan({
        "name": "Orders sync",
        "source_columns": ["order_id", "amount"],
        "source_schema": {"order_id": "INTEGER", "amount": "DECIMAL"},
        "target_columns": ["order_id", "amount"],
        "target_schema": {"order_id": "BIGINT", "amount": "NUMERIC"},
    })
    loaded = get_plan(plan.id)
    assert loaded is not None
    assert loaded.name == "Orders sync"
    assert loaded.status == "draft"


def test_mapping_revision_hashes():
    plan = create_plan({
        "source_columns": ["AMT"],
        "source_schema": {"AMT": "DECIMAL"},
        "target_columns": ["payment_amount"],
        "target_schema": {"payment_amount": "NUMERIC"},
    })
    pipeline = {
        "mappings": [{"source": "AMT", "target": "payment_amount", "confidence": 0.92}],
        "transforms": [{"source": "AMT", "target": "payment_amount", "transform": "decimal"}],
        "validation": {"passed": True, "issues": []},
        "agents_used": ["MappingReasonerAgent"],
        "plan_summary": {"mapped_count": 1},
    }
    updated = add_mapping_revision(plan.id, pipeline)
    assert updated is not None
    rev = updated.active_revision()
    assert rev.version == 1
    assert rev.mapping_hash == fingerprint_mappings(pipeline["mappings"])
    assert rev.source_schema_hash == fingerprint_schema(["AMT"], {"AMT": "DECIMAL"})


def test_mapping_revision_persists_mapping_proof():
    plan = create_plan({
        "source_columns": ["AMT"],
        "source_schema": {"AMT": "DECIMAL"},
        "target_columns": ["payment_amount"],
        "target_schema": {"payment_amount": "NUMERIC"},
        "destination": {"kind": "database", "format": "snowflake"},
        "source": {"kind": "file", "format": "csv"},
    })
    pipeline = {
        "mappings": [{
            "source": "AMT",
            "target": "payment_amount",
            "source_type": "DECIMAL",
            "target_type": "NUMERIC",
            "confidence": 0.92,
            "transform": "decimal",
        }],
        "transforms": [{"source": "AMT", "target": "payment_amount", "transform": "decimal"}],
        "validation": {"passed": True, "issues": []},
        "agents_used": ["MappingReasonerAgent"],
        "plan_summary": {"mapped_count": 1},
        "mapping_proof": {
            "dest_mode": "match_existing",
            "mappings": [{"source": "AMT", "target": "payment_amount", "confidence": 0.92}],
            "summary": {"mapped_count": 1},
        },
    }
    updated = add_mapping_revision(plan.id, pipeline)
    rev = updated.active_revision()
    assert rev.mapping_proof.get("dest_mode") == "match_existing"
    assert rev.mapping_proof["mappings"][0]["source"] == "AMT"

    from services.transfer_plan_store import sync_ui_mappings
    from services.transfer_plan_service import build_run_payload

    synced = sync_ui_mappings(plan.id, pipeline["mappings"])
    assert synced.active_revision().mapping_proof.get("mappings")
    payload = build_run_payload(plan.id)
    assert payload["mapping_proof"]["mappings"][0]["source"] == "AMT"
    assert payload["mapping_hash"] == synced.active_revision().mapping_hash


def test_preflight_run_persisted():
    plan = create_plan({"source_columns": ["a"], "target_columns": ["a"]})
    add_mapping_revision(plan.id, {
        "mappings": [{"source": "a", "target": "a", "confidence": 0.9}],
        "transforms": [],
        "validation": {"passed": True},
    })
    updated = add_preflight_run(plan.id, {"passed": True, "readiness_score": 100, "gates": [], "blockers": []})
    assert updated.status == "preflight_passed"
    assert len(updated.preflight_runs) == 1


def test_approve_plan_version():
    plan = create_plan({"source_columns": ["x"], "target_columns": ["x"]})
    add_mapping_revision(plan.id, {
        "mappings": [{"source": "x", "target": "x", "confidence": 0.95}],
        "transforms": [],
        "validation": {"passed": True},
    })
    approved = approve_plan_version(plan.id)
    assert approved.status == "approved"
    assert approved.active_revision().approved is True
