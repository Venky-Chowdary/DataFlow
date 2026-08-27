"""Enterprise run-policy gates must protect incremental and schema-drift runs."""

from __future__ import annotations

from src.services.preflight_service import (
    apply_policy_gates,
    confidence_threshold_for_mode,
    run_transfer_policy_gates,
)


def test_readiness_caps_when_g9_uniqueness_is_sample_only():
    from services.preflight_service import apply_readiness_honesty_caps

    capped = apply_readiness_honesty_caps({
        "passed": True,
        "passed_count": 14,
        "total_gates": 14,
        "readiness_score": 100.0,
        "gates": [
            {"id": "g1_source", "status": "pass", "message": "ok", "details": {}},
            {
                "id": "g9_data_integrity",
                "status": "pass",
                "message": "Integrity checks passed (Validate sample only — population uniqueness not proven)",
                "details": {"coverage": "sample"},
            },
        ],
    })
    assert capped["readiness_score"] == 92.0
    assert capped["population_uniqueness_proven"] is False
    assert capped["readiness_cap_reason"] == "g9_sample_uniqueness"


def test_readiness_caps_when_g5_dry_run_is_sample_only():
    from services.preflight_service import apply_readiness_honesty_caps

    capped = apply_readiness_honesty_caps({
        "passed": True,
        "passed_count": 14,
        "total_gates": 14,
        "readiness_score": 100.0,
        "gates": [
            {
                "id": "g5_dry_run",
                "status": "pass",
                "message": "Dry-run passed on 1000 sample rows",
                "details": {"sample_cap": 1000, "coverage": "sample"},
            },
            {"id": "g9_data_integrity", "status": "pass", "message": "ok", "details": {}},
        ],
    })
    assert capped["readiness_score"] == 92.0
    assert "g5_sample_dry_run" in capped["readiness_cap_reason"]


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


def test_cdc_exactly_once_blocks_on_file_dest():
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
        dest_type="csv",
        delivery_guarantee="exactly_once",
    )
    g16 = next(g for g in gates if g["id"] == "g16_cdc_delivery")
    assert g16["status"] == "block"
    assert g16["details"]["reason"] == "exactly_once_dest_not_transactional"


def test_cdc_exactly_once_passes_on_wired_sql_dest():
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
        delivery_guarantee="exactly_once",
    )
    g16 = next(g for g in gates if g["id"] == "g16_cdc_delivery")
    assert g16["status"] == "pass"
    assert g16["details"]["wired"] is True
    assert g16["details"]["platform_claimed"] is False


def test_cdc_default_delivery_gate_is_at_least_once():
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
    g16 = next(g for g in gates if g["id"] == "g16_cdc_delivery")
    assert g16["status"] == "pass"
    assert g16["details"]["delivery_guarantee"] == "at_least_once"


def test_full_refresh_omits_cdc_delivery_gate():
    gates = run_transfer_policy_gates(
        sync_mode="full_refresh_overwrite",
        schema_policy="manual_review",
        validation_mode="strict",
        stream_contracts=[],
        dest_type="postgresql",
    )
    assert all(g["id"] != "g16_cdc_delivery" for g in gates)


def test_row_cap_gate_names_execute_limit_without_blocking():
    silent = run_transfer_policy_gates(
        sync_mode="full_refresh_append",
        schema_policy="manual_review",
        dest_type="postgresql",
    )
    assert all(g["id"] != "g17_row_cap" for g in silent)

    gates = run_transfer_policy_gates(
        sync_mode="full_refresh_append",
        schema_policy="manual_review",
        dest_type="postgresql",
        priority_column="updated_at",
        priority_direction="asc",
        row_limit=2500,
    )
    g17 = next(g for g in gates if g["id"] == "g17_row_cap")
    assert g17["status"] == "pass"
    assert g17["details"]["row_limit"] == 2500
    assert g17["details"]["priority_column"] == "updated_at"
    assert "uncapped source" in g17["message"]
    assert "capped write" in g17["message"]


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

