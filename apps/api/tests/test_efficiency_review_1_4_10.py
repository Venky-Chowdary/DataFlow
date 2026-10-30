"""Efficiency review items 1–4 and 10.

Schema policy, dest-only auto-pass, sync-now idempotency, gate cause
counts, empty-table vacuous pass, will-create, stratified sample, and
row-count plus checksum accounting.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.schema_drift import (  # noqa: E402
    classify_schema_change,
    detect_schema_drift,
    execute_must_pause,
    normalize_schema_policy,
    resolve_schema_evolution,
)
from services.schema_fingerprint import fingerprint_schema  # noqa: E402


def test_default_policy_propagates_a_nullable_add():
    assert normalize_schema_policy("") == "propagate_columns"
    assert normalize_schema_policy("block") == "block_all"
    assert normalize_schema_policy("approve") == "manual_review"
    evo = resolve_schema_evolution(
        {
            "additive": [{"kind": "add_column", "column": "note", "nullable": True}],
            "breaking": [],
            "severity": "additive",
            "renamed": [],
        },
    )
    assert evo["policy"] == "propagate_columns"
    assert evo["action"] == "propagate"
    assert evo["audit"] and evo["audit"][0]["decision"] == "auto_pass"


def test_block_all_pauses_a_nullable_add_and_accepts_for_one_run():
    evo = resolve_schema_evolution(
        {
            "additive": [{"kind": "add_column", "column": "note", "nullable": True}],
            "breaking": [],
            "severity": "additive",
            "renamed": [],
        },
        schema_policy="block_all",
    )
    assert evo["should_pause"] is True
    assert evo["hard_breaking"] == []
    assert execute_must_pause(evo, schema_drift_acknowledged=False) is True
    assert execute_must_pause(evo, schema_drift_acknowledged=True) is False


def test_hard_break_cannot_be_acknowledged_away():
    evo = resolve_schema_evolution(
        {
            "additive": [],
            "breaking": [{"kind": "narrow_type", "column": "email"}],
            "severity": "breaking",
            "renamed": [],
        },
        schema_policy="propagate_columns",
    )
    assert evo["should_pause"] is True
    assert execute_must_pause(evo, schema_drift_acknowledged=True) is True


def test_destination_only_columns_do_not_block_a_succeeded_fingerprint():
    mapped = ["id", "email"]
    types = {"id": "INTEGER", "email": "VARCHAR"}
    stored = fingerprint_schema(mapped, types)
    live_cols = mapped + ["created_at", "is_active", "metadata_json", "payload_b64", "record_uuid"]
    live_types = {
        **types,
        "created_at": "TIMESTAMP",
        "is_active": "BOOLEAN",
        "metadata_json": "JSONB",
        "payload_b64": "TEXT",
        "record_uuid": "UUID",
    }
    for policy in ("manual_review", "block_all", "propagate_columns"):
        report = detect_schema_drift(
            source_columns=mapped,
            source_schema=types,
            target_columns=live_cols,
            target_schema=live_types,
            stored_target_fp=stored,
            mappings=[{"source": c, "target": c, "confidence": 1.0} for c in mapped],
            destination_db_type="postgresql",
            schema_policy=policy,
            table_exists=True,
        )
        assert report["schema_evolution"]["should_pause"] is False, policy
        assert report["schema_evolution"]["action"] == "continue", policy
        assert report["destination_only_columns"]
        assert any(
            item.get("kind") == "destination_only"
            for item in report["non_breaking_audit"]
        ), policy


def test_rename_uses_similarity_and_type_and_refuses_a_false_friend():
    renamed = classify_schema_change(
        {"customer_name": "VARCHAR"},
        {"customername": "VARCHAR"},
    )
    assert renamed["renamed"] == [{"from": "customer_name", "to": "customername"}]
    assert not any(item.get("kind") == "drop" for item in renamed["breaking"] if item.get("kind") == "drop" and item.get("column") == "customer_name")
    false_friend = classify_schema_change(
        {"AMT": "DECIMAL(12,2)"},
        {"quantity": "DECIMAL(12,2)"},
    )
    assert false_friend["renamed"] == []
    assert any(item.get("kind") == "drop" and item.get("column") == "AMT" for item in false_friend["breaking"])
    assert any(item.get("kind") == "add_column" and item.get("column") == "quantity" for item in false_friend["additive"])


def test_position_sample_includes_head_tail_and_middle():
    from services.sample_strategy import position_reservoir_sample

    rows = [{"id": i} for i in range(100)]
    sample = position_reservoir_sample(rows, 10, seed="efficiency")
    ids = [row["id"] for row in sample]
    assert ids[0] == 0
    assert 99 in ids
    assert any(1 <= i <= 98 for i in ids)
    assert ids != list(range(10))
    again = position_reservoir_sample(rows, 10, seed="efficiency")
    assert [row["id"] for row in again] == ids


def test_empty_measured_source_is_a_vacuous_pass_note():
    from services.preflight_proof_bundle import build_preflight_proof_bundle

    bundle = build_preflight_proof_bundle(
        columns=["id"],
        sample_rows=[],
        mappings=[{"source": "id", "target": "id", "confidence": 1.0, "transform": "none"}],
        source_measured_empty=True,
    )
    decision = bundle["transfer_decision"]
    blob = " ".join(decision.get("blockers") or [])
    assert "Sample quality not profiled" not in blob
    assert bundle["quality_grade"] == "vacuous"
    assert "0 rows" in bundle["evidence_summary"]


def test_missing_target_is_will_create_not_a_fidelity_collapse():
    from services.coercion_probe import analyze_coercion
    from services.mapping_proof import mapping_fidelity

    report = analyze_coercion(
        sample_rows=[{"employee_id": "EMP0000001"}],
        mappings=[{"source": "employee_id", "target": "employee_id", "target_type": ""}],
        source_types={"employee_id": "VARCHAR"},
        dest_types={},
        dest_db_type="mysql",
        table_exists=False,
    )
    col = report["by_source"]["employee_id"]
    assert col["will_create"] is True
    assert col["fidelity_collapse"] is False
    assert col["severity"] == "pass"
    verdict = mapping_fidelity(
        {"source": "employee_id", "target": "employee_id", "transform": "none"},
        declared_source_type="VARCHAR",
        destination_db_type="mysql",
        dest_table_exists=False,
    )
    assert verdict["verdict"] == "will_create"
    assert verdict["type_narrowing"] is False


def test_gate_contract_dedupes_drift_and_counts_sum():
    from services.preflight_policy_gates import gate_operator_code, stamp_gate_contract

    out = stamp_gate_contract({
        "gates": [
            {"id": "g9_data_integrity", "status": "pass", "message": "ok"},
            {"id": "g9_sync_contract", "status": "pass", "message": "ok"},
            {"id": "g3f_population_fit", "status": "skip", "message": "skipped"},
            {"id": "schema_drift", "status": "block", "message": "Schema change detected"},
            {"id": "proof_0", "status": "block", "message": "Schema change detected — policy review"},
            {"id": "g4_mapping_confidence", "status": "warn", "message": "review"},
        ],
    })
    ids = [g["id"] for g in out["gates"]]
    assert ids.count("schema_drift") == 1
    assert "proof_0" not in ids
    assert gate_operator_code("g9_sync_contract") == "G12"
    assert gate_operator_code("g9_data_integrity") == "G9"
    summary = out["gate_summary"]
    assert summary["passed"] + summary["blocked"] + summary["skipped"] + summary["review"] + summary["pending"] == summary["total"]
    assert summary["total"] == len(out["gates"])
    assert summary["blocked"] == 1
    assert summary["review"] == 1


def test_fast_scope_keeps_only_drift_and_connectivity():
    from services.preflight_policy_gates import apply_fast_preflight_scope

    out = apply_fast_preflight_scope(
        {
            "passed": False,
            "gates": [
                {"id": "g1_source", "status": "pass"},
                {"id": "g4_mapping_confidence", "status": "block", "message": "low"},
                {"id": "schema_drift", "status": "pass"},
            ],
            "blockers": [
                {"id": "g4_mapping_confidence", "message": "low"},
            ],
        },
        "drift_connectivity",
    )
    assert [g["id"] for g in out["gates"]] == ["g1_source", "schema_drift"]
    assert out["passed"] is True
    assert out["scoped_out_gates"][0]["id"] == "g4_mapping_confidence"


def test_sync_now_is_idempotent_and_full_resync_names_its_plan(tmp_path):
    from src.ai.copilot.ack_ledger import PilotAckLedger

    from services.sync_now import (
        clear_sync_now_for_tests,
        full_resync_plan,
        prepare_sync,
    )

    clear_sync_now_for_tests()
    ledger = PilotAckLedger(path=tmp_path / "acks.json")
    parent = {
        "sync_mode": "incremental_append",
        "primary_key": "id",
        "mappings": [{"source": "id", "target": "id", "transform": "none"}],
        "stream_contracts": [{"primary_key": "id"}],
    }
    first = prepare_sync(
        mode="sync",
        parent_request=parent,
        parent_id="job-parent",
        source_connected=True,
        destination_connected=True,
        ledger=ledger,
    )
    second = prepare_sync(
        mode="sync",
        parent_request=parent,
        parent_id="job-parent",
        source_connected=True,
        destination_connected=True,
        ledger=ledger,
    )
    assert first["ack_id"]
    assert second["ack_id"] == first["ack_id"]
    assert second["idempotent"] is True
    plan = full_resync_plan(parent)
    assert plan["sync_mode"] == "upsert"  # D208: full-source, no watermark
    assert plan["destructive"] is False
    full = prepare_sync(
        mode="full",
        parent_request=parent,
        parent_id="job-parent",
        source_connected=True,
        destination_connected=True,
        ledger=ledger,
    )
    assert full["ack_id"] != first["ack_id"]
    assert full["preview"]["destructive"] is False


def test_sync_now_refuses_when_connectivity_fails():
    import pytest

    from services.sync_now import SyncNowRefused, prepare_sync
    with pytest.raises(SyncNowRefused):
        prepare_sync(
            mode="sync",
            parent_request={"mappings": [], "sync_mode": "full_refresh_append"},
            parent_id="job-down",
            source_connected=False,
            destination_connected=True,
            source_error="source down",
        )


def test_quarantine_is_the_runtime_default_and_rows_checksum_account():
    from connectors.writer_common import transform_error_policy
    from services.transfer_accounting import account_transfer

    assert transform_error_policy(None) == "quarantine"
    source = [{"id": i, "email": f"u{i}@example.com"} for i in range(10)]
    landed = source[:8]
    quarantined = source[8:]
    report = account_transfer(
        source_rows=source,
        landed_rows=landed,
        quarantined_rows=quarantined,
        sync_mode="full_refresh_append",
    )
    assert report["rows_loaded"] + report["rows_quarantined"] + report["rows_skipped"] == report["rows_read"]
    assert report["checksums_match"] is True
    assert report["balanced"] is True
    assert report["delivery"] == "at_least_once_upsert"
