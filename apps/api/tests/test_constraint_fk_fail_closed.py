"""Host wiring: unmapped dest FK findings fail closed on Validate."""

from __future__ import annotations

from services.preflight_service import run_file_preflight


def test_unmapped_fk_blocks_strict_validate():
    result = run_file_preflight(
        columns=["id"],
        column_types={"id": "INTEGER"},
        row_count=1,
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
        sample_rows=[{"id": 1}],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        destination_table_exists=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_column_types={"id": "INTEGER", "customer_id": "INTEGER"},
        destination_foreign_keys=[
            {
                "columns": ["customer_id"],
                "referenced_table": "customers",
                "referenced_columns": ["id"],
            }
        ],
        validation_mode="strict",
        destination_db_type="postgresql",
    )
    assert any(b.get("id") == "constraint_fk" for b in result.get("blockers") or []), result.get("blockers")
    assert result["passed"] is False
    findings = result.get("constraint_findings") or result.get("constraint_hints") or []
    assert findings
    assert findings[0]["code"] == "fk_column_unmapped"
    ri = result.get("referential_integrity") or {}
    assert ri.get("proven") is False
    assert ri.get("coverage") == "destination_fk_metadata"


def test_unmapped_fk_clears_with_ack_without_claiming_ri():
    result = run_file_preflight(
        columns=["id"],
        column_types={"id": "INTEGER"},
        row_count=1,
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
        sample_rows=[{"id": 1}],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        destination_table_exists=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_column_types={"id": "INTEGER", "customer_id": "INTEGER"},
        destination_foreign_keys=[
            {
                "columns": ["customer_id"],
                "referenced_table": "customers",
                "referenced_columns": ["id"],
            }
        ],
        validation_mode="strict",
        destination_db_type="postgresql",
        fk_risk_acknowledged=True,
        acknowledgment_actor="operator@example.com",
        acknowledgment_reason="Accept FK mapping risk for fixture transfer",
    )
    assert not any(b.get("id") == "constraint_fk" for b in result.get("blockers") or [])
    ri = result.get("referential_integrity") or {}
    assert ri.get("proven") is False
    assert ri.get("fk_risk_acknowledged") is True
