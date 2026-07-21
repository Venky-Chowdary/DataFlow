"""Enterprise run-policy gates must protect incremental and schema-drift runs."""

from __future__ import annotations

from src.services.preflight_service import (
    apply_policy_gates,
    confidence_threshold_for_mode,
    run_transfer_policy_gates,
)


def test_cdc_policy_blocks_without_cursor_and_primary_key():
    gates = run_transfer_policy_gates(
        sync_mode="cdc",
        schema_policy="manual_review",
        validation_mode="strict",
        stream_contracts=[{"name": "orders", "selected": True, "field_count": 8}],
    )
    blockers = [g for g in gates if g["status"] == "block"]

    assert blockers
    assert blockers[0]["id"] == "g9_sync_contract"
    assert "Missing cursor" in str(blockers[0]["details"])
    assert "Missing primary key" in str(blockers[0]["details"])


def test_policy_gates_merge_into_preflight_result():
    base = {
        "passed": True,
        "passed_count": 8,
        "total_gates": 8,
        "readiness_score": 100,
        "gates": [{"id": f"g{i}", "status": "pass", "message": "ok"} for i in range(1, 9)],
        "blockers": [],
    }
    merged = apply_policy_gates(
        base,
        run_transfer_policy_gates(
            sync_mode="incremental_deduped",
            schema_policy="propagate_columns",
            validation_mode="maximum",
            stream_contracts=[{
                "name": "orders",
                "selected": True,
                "cursor_field": "updated_at",
                "primary_key": "order_id",
            }],
            backfill_new_fields=True,
        ),
    )

    assert merged["passed"] is True
    assert merged["total_gates"] == 11
    assert merged["readiness_score"] == 100
    assert confidence_threshold_for_mode("maximum") == 0.95


def test_stuck_backfill_under_manual_review_does_not_block():
    """Studio toggle stuck true after switching back to manual_review must not fail Execute."""
    gates = run_transfer_policy_gates(
        sync_mode="append",
        schema_policy="manual_review",
        validation_mode="strict",
        stream_contracts=[],
        backfill_new_fields=True,
    )
    g10 = next(g for g in gates if g["id"] == "g10_schema_policy")
    assert g10["status"] == "pass"
    assert g10["details"].get("policy_coerced_from_manual_review") is True
    assert g10["details"].get("schema_policy") == "propagate_columns"


def test_backfill_still_blocks_under_type_locked():
    gates = run_transfer_policy_gates(
        sync_mode="append",
        schema_policy="type_locked",
        validation_mode="strict",
        stream_contracts=[],
        backfill_new_fields=True,
    )
    g10 = next(g for g in gates if g["id"] == "g10_schema_policy")
    assert g10["status"] == "block"
    assert "conflicts with schema policy" in str(g10["details"])

