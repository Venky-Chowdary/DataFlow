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
                "cursor_semantics": "modification_timestamp",
            }],
            backfill_new_fields=True,
        ),
    )

    assert merged["passed"] is True
    assert merged["total_gates"] == 12
    assert merged["readiness_score"] == 100
    assert confidence_threshold_for_mode("maximum") == 0.95


def test_scd2_blocks_on_non_sql_destination():
    gates = run_transfer_policy_gates(
        sync_mode="scd2",
        schema_policy="manual_review",
        validation_mode="strict",
        stream_contracts=[{
            "name": "orders",
            "selected": True,
            "primary_key": "order_id",
        }],
        source_columns=["order_id", "updated_at"],
        dest_type="mongodb",
    )
    g9 = next(g for g in gates if g["id"] == "g9_sync_contract")
    assert g9["status"] == "block"
    assert "SQL table destination" in str(g9["details"])


def test_cdc_passes_for_database_source_with_cursor_and_pk():
    gates = run_transfer_policy_gates(
        sync_mode="cdc",
        schema_policy="manual_review",
        validation_mode="strict",
        stream_contracts=[{
            "name": "orders",
            "selected": True,
            "cursor_field": "updated_at",
            "primary_key": "order_id",
            "cursor_semantics": "cdc_position",
        }],
        source_columns=["order_id", "updated_at"],
        source_kind="database",
        source_type="mysql",
        dest_type="postgresql",
    )
    g9 = next(g for g in gates if g["id"] == "g9_sync_contract")
    assert g9["status"] != "block"


def test_cdc_blocks_for_file_source():
    gates = run_transfer_policy_gates(
        sync_mode="cdc",
        schema_policy="manual_review",
        validation_mode="strict",
        stream_contracts=[{
            "name": "orders",
            "selected": True,
            "cursor_field": "updated_at",
            "primary_key": "order_id",
        }],
        source_columns=["order_id", "updated_at"],
        source_kind="file",
        dest_type="postgresql",
    )
    g9 = next(g for g in gates if g["id"] == "g9_sync_contract")
    assert g9["status"] == "block"
    assert "database source" in str(g9["details"]).lower()


def test_staging_blocks_on_unsupported_destination():
    gates = run_transfer_policy_gates(
        sync_mode="full_refresh_append",
        schema_policy="manual_review",
        validation_mode="strict",
        stream_contracts=[],
        dest_type="mongodb",
        write_via_staging=True,
    )
    g12 = next(g for g in gates if g["id"] == "g12_staging_policy")
    assert g12["status"] == "block"
    assert "not supported" in str(g12["details"]).lower()


def test_pilot_plan_transfer_signature_accepts_write_via_staging():
    """Pilot must be able to pass G12 the same way Studio /preflight/run does."""
    import inspect

    from src.ai.copilot.transfer_tools import plan_transfer

    params = inspect.signature(plan_transfer).parameters
    assert "write_via_staging" in params


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


def test_cdc_cursor_typo_blocks_against_live_source_columns():
    gates = run_transfer_policy_gates(
        sync_mode="cdc",
        schema_policy="manual_review",
        validation_mode="strict",
        stream_contracts=[{
            "name": "orders",
            "selected": True,
            "cursor_field": "update_att",
            "primary_key": "order_id",
        }],
        source_columns=["order_id", "updated_at", "amount"],
    )
    g9 = next(g for g in gates if g["id"] == "g9_sync_contract")
    assert g9["status"] == "block"
    assert "Cursor field not in source schema" in str(g9["details"])


def test_cdc_cursor_and_pk_pass_when_present_in_source_columns():
    gates = run_transfer_policy_gates(
        sync_mode="incremental_deduped",
        schema_policy="propagate_columns",
        validation_mode="strict",
        stream_contracts=[{
            "name": "orders",
            "selected": True,
            "cursor_field": "updated_at",
            "primary_key": "order_id",
            "cursor_semantics": "modification_timestamp",
        }],
        source_columns=["order_id", "updated_at", "amount"],
    )
    g9 = next(g for g in gates if g["id"] == "g9_sync_contract")
    assert g9["status"] == "pass"


def test_incremental_deduped_blocks_an_undeclared_cursor():
    """Nothing about `updated_at` proves the source moves it when a row changes.

    Live evidence (cursor_semantics_live_results.json): under a `created_at`
    cursor the destination kept a stale row and the run reported success.
    """
    gates = run_transfer_policy_gates(
        sync_mode="incremental_deduped",
        schema_policy="propagate_columns",
        validation_mode="strict",
        stream_contracts=[{
            "name": "orders",
            "selected": True,
            "cursor_field": "updated_at",
            "primary_key": "order_id",
        }],
        source_columns=["order_id", "updated_at", "amount"],
    )
    g9 = next(g for g in gates if g["id"] == "g9_sync_contract")
    assert g9["status"] == "block"
    verdicts = g9["details"]["cursor_semantics"]
    assert verdicts and verdicts[0]["primary_action"]

