"""Prove mapping_proof survives plan → run-payload and job stamp rebuild paths."""

from __future__ import annotations

from services.mapping_proof import mapping_proof_or_build
from services.transfer_plan_service import build_run_payload
from services.transfer_plan_store import (
    add_mapping_revision,
    approve_plan_version,
    create_plan,
)


def test_run_payload_includes_mapping_proof(tmp_path, monkeypatch):
    path = tmp_path / "transfer_plans.json"
    monkeypatch.setattr("services.transfer_plan_store.STORE_PATH", path)

    plan = create_plan({
        "name": "Proof plan",
        "source": {"kind": "file", "format": "csv"},
        "destination": {"kind": "database", "format": "postgresql"},
        "source_columns": ["order_id", "amount"],
        "source_schema": {"order_id": "INTEGER", "amount": "DECIMAL"},
        "target_columns": ["order_id", "amount"],
        "target_schema": {"order_id": "INTEGER", "amount": "NUMERIC"},
        "policies": {"sync_mode": "full_refresh_overwrite"},
    })
    add_mapping_revision(plan.id, {
        "mappings": [
            {
                "source": "order_id",
                "target": "order_id",
                "source_type": "INTEGER",
                "target_type": "INTEGER",
                "confidence": 0.95,
                "transform": "none",
            },
            {
                "source": "amount",
                "target": "amount",
                "source_type": "DECIMAL",
                "target_type": "NUMERIC",
                "confidence": 0.91,
                "transform": "decimal",
            },
        ],
        "transforms": [],
        "validation": {"passed": True},
        "agents_used": ["MappingReasonerAgent"],
        "plan_summary": {"mapped_count": 2},
    })
    approve_plan_version(plan.id)
    payload = build_run_payload(plan.id)
    assert payload["mapping_proof"]["mappings"]
    assert payload["mapping_proof"]["summary"]["mapped_count"] == 2
    assert payload["mapping_version"] == 1


def test_stamp_rebuild_from_transfer_request_mappings():
    proof = mapping_proof_or_build(
        [
            {
                "source": "sku",
                "target": "sku",
                "source_type": "VARCHAR",
                "target_type": "VARCHAR",
                "confidence": 0.88,
                "create_new": True,
                "assignment_strategy": "identity_passthrough",
            }
        ],
        destination_db_type="snowflake",
        source_kind="file",
        dest_kind="database",
        sync_mode="full_refresh_overwrite",
    )
    assert proof["dest_mode"] == "create_new"
    assert proof["mappings"][0]["source"] == "sku"
    assert proof["summary"]["mapped_count"] == 1
