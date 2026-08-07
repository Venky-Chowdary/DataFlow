"""Map Migration Risk Contract must survive Validate — MappingItem + adapters.

Regression: operators sign CAST_AND_CONTINUE on Map (TEXT→INTEGER fidelity
collapse) then Validate still blocked because:
  1) MappingItem stripped risk_contract / fidelity on /preflight/run
  2) FilePreflightContext adapters omitted those fields for G9 + coercion_report
  3) coercion_probe forced severity=block on fidelity_collapse ignoring contract

Charter: boolean ``risk_acknowledged`` alone never clears — need a verified
continue-policy Migration Risk Contract.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

_PREFLIGHT_SRC = Path(__file__).resolve().parents[3] / "packages" / "preflight" / "src"
if str(_PREFLIGHT_SRC) not in sys.path:
    sys.path.insert(0, str(_PREFLIGHT_SRC))


def _cast_continue_contract(column: str = "country_auto_detected") -> dict:
    from services.migration_risk_contract import create_migration_risk_contract

    return create_migration_risk_contract(
        column=column,
        source_type="TEXT COLLATE UTF8MB4_0900_AI_CI",
        destination_type="INTEGER",
        approved_by="ops@dataflow.app",
        reason="TEXT→INTEGER intentional cast for country flag codes",
        execution_policy="CAST_AND_CONTINUE",
    ).to_dict()


def test_mapping_item_preserves_risk_ack_and_fidelity():
    from src.routers.preflight_router import MappingItem

    item = MappingItem(
        source="country_auto_detected",
        target="country_auto_detected",
        confidence=0.9,
        source_type="TEXT",
        target_type="INTEGER",
        create_new=True,
        fidelity="lossy_cast",
        type_narrowing=True,
        risk_acknowledged=True,
    )
    dumped = item.model_dump()
    assert dumped["risk_acknowledged"] is True
    assert dumped["fidelity"] == "lossy_cast"
    assert dumped["type_narrowing"] is True


def test_coercion_probe_warns_declared_collapse_when_risk_acked():
    from services.coercion_probe import analyze_coercion

    contract = _cast_continue_contract()
    report = analyze_coercion(
        sample_rows=[
            {"country_auto_detected": "0"},
            {"country_auto_detected": "1"},
            {"country_auto_detected": "0"},
        ],
        mappings=[
            {
                "source": "country_auto_detected",
                "target": "country_auto_detected",
                "source_type": "TEXT COLLATE UTF8MB4_0900_AI_CI",
                "target_type": "INTEGER",
                "create_new": True,
                "risk_acknowledged": True,
                "risk_contract": contract,
            }
        ],
        source_types={"country_auto_detected": "TEXT COLLATE UTF8MB4_0900_AI_CI"},
        dest_types={"country_auto_detected": "INTEGER"},
        dest_db_type="postgresql",
        table_exists=False,
        validation_mode="strict",
    )
    cols = report.get("columns") or []
    assert cols, report
    col = next(c for c in cols if c.get("source") == "country_auto_detected")
    assert col.get("fidelity_collapse") is True
    assert col["severity"] == "warn", col
    assert col["failed"] == 0


def test_coercion_probe_still_blocks_declared_collapse_without_ack():
    from services.coercion_probe import analyze_coercion

    report = analyze_coercion(
        sample_rows=[
            {"country_auto_detected": "0"},
            {"country_auto_detected": "1"},
        ],
        mappings=[
            {
                "source": "country_auto_detected",
                "target": "country_auto_detected",
                "source_type": "TEXT COLLATE UTF8MB4_0900_AI_CI",
                "target_type": "INTEGER",
                "create_new": True,
                # boolean ack alone must NOT clear (charter)
                "risk_acknowledged": True,
            }
        ],
        source_types={"country_auto_detected": "TEXT COLLATE UTF8MB4_0900_AI_CI"},
        dest_types={"country_auto_detected": "INTEGER"},
        dest_db_type="postgresql",
        table_exists=False,
        validation_mode="strict",
    )
    col = (report.get("columns") or [])[0]
    assert col["severity"] == "block"
    assert col.get("fidelity_collapse") is True


def test_file_preflight_adapters_forward_risk_ack():
    """Integrity + coercion_report must see signed Risk Contract (plan path)."""
    from preflight.models import (
        ColumnMapping,
        ColumnSchema,
        DestinationConfig,
        SourceConfig,
        TransferPlan,
    )
    from services.preflight_service import FilePreflightContext

    col = "country_auto_detected"
    contract = _cast_continue_contract(col)
    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="mysql",
            columns=[
                ColumnSchema(
                    name=col,
                    inferred_type="TEXT COLLATE UTF8MB4_0900_AI_CI",
                ),
            ],
            connected=True,
            row_count_estimate=100,
        ),
        destination=DestinationConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            table_exists=False,
            can_create_table=True,
            can_write=True,
            target_columns=[
                ColumnSchema(name=col, inferred_type="INTEGER"),
            ],
        ),
        mappings=[
            ColumnMapping(
                source=col,
                target=col,
                confidence=0.9,
                target_type="INTEGER",
                create_new=True,
                fidelity="lossy_cast",
                risk_acknowledged=True,
                risk_contract=contract,
            )
        ],
        sync_mode="full_refresh_append",
        validation_mode="strict",
    )
    ctx = FilePreflightContext(
        plan=plan,
        sample_rows=[{col: "0"}, {col: "1"}, {col: "0"}],
    )

    report = ctx.coercion_report()
    cols = report.get("columns") or []
    assert cols, report
    assert cols[0]["severity"] != "block", cols[0]

    integrity = ctx.run_integrity_audit()
    coercion = next(
        (c for c in integrity.get("checks") or [] if c.get("check") == "coercion_safety"),
        None,
    )
    assert coercion is not None, integrity
    assert coercion.get("blocks_transfer") is False, coercion


def test_run_file_preflight_honors_risk_ack_on_text_to_int():
    """End-to-end: TEXT→INTEGER with signed Risk Contract must not leave G3/G4/G9 blocking."""
    from services.preflight_service import run_file_preflight

    col = "country_auto_detected"
    src_type = "TEXT COLLATE UTF8MB4_0900_AI_CI"
    contract = _cast_continue_contract(col)
    result = run_file_preflight(
        columns=[col],
        column_types={col: src_type},
        row_count=3,
        mappings=[
            {
                "source": col,
                "target": col,
                "confidence": 0.92,
                "source_type": src_type,
                "target_type": "INTEGER",
                "create_new": True,
                "fidelity": "lossy_cast",
                "type_narrowing": True,
                "risk_acknowledged": True,
                "risk_contract": contract,
                "user_override": True,
            }
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="mysql",
        sync_mode="full_refresh_append",
        sample_rows=[{col: "0"}, {col: "1"}, {col: "0"}],
        confidence_threshold=0.85,
        validation_mode="strict",
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
    )
    blockers = result.get("blockers") or []
    gate_by_id = {g["id"]: g for g in (result.get("gates") or [])}
    for gid in ("g3_schema_contract", "g4_mapping_confidence", "g9_data_integrity"):
        g = gate_by_id.get(gid)
        assert g is not None, f"missing {gid}"
        assert g.get("status") != "block", (gid, g.get("message"), g.get("details"))
    fidelity_msgs = [
        b.get("message", "")
        for b in blockers
        if "fidelity" in (b.get("message") or "").lower()
        or "lossy" in (b.get("message") or "").lower()
    ]
    assert not fidelity_msgs, fidelity_msgs
    # UI honesty: coercion_report must not keep severity=block after Accept risk.
    cr_cols = (result.get("coercion_report") or {}).get("columns") or []
    for c in cr_cols:
        if c.get("source") == col:
            assert c.get("severity") != "block", c
