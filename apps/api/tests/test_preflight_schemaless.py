"""Preflight behavior for schemaless destinations (MongoDB, DynamoDB, Redis)."""

from __future__ import annotations

import pytest

from src.services.preflight_service import run_file_preflight


@pytest.mark.parametrize("dest_type", ["mongodb", "dynamodb", "redis"])
def test_preflight_passes_high_null_rate_for_schemaless(dest_type: str) -> None:
    """Optional/missing fields are normal in documents; they must not block."""
    sample_rows = [
        {"_id": "1", "name": "Alice", "address": ""},
        {"_id": "2", "name": "Bob", "address": ""},
        {"_id": "3", "name": "Charlie", "address": ""},
    ]
    columns = list(sample_rows[0].keys())
    column_types = {c: "VARCHAR" for c in columns}
    mappings = [{"source": c, "target": c, "confidence": 0.99} for c in columns]

    result = run_file_preflight(
        columns=columns,
        column_types=column_types,
        row_count=len(sample_rows),
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sample_rows=sample_rows,
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_db_type=dest_type,
        validation_mode="strict",
    )
    assert result["passed"] is True
    gate_status = {g["id"]: g["status"] for g in result["gates"]}
    assert gate_status.get("g6_target_ddl") == "pass"
    assert gate_status.get("g5_dry_run") == "pass"


def test_preflight_allows_duplicate_user_id_for_mongodb() -> None:
    """Non-primary *_id fields in MongoDB are foreign keys and may repeat."""
    sample_rows = [
        {"_id": "1", "user_id": "U1", "name": "Alice"},
        {"_id": "2", "user_id": "U1", "name": "Bob"},
        {"_id": "3", "user_id": "U2", "name": "Charlie"},
    ]
    columns = list(sample_rows[0].keys())
    column_types = {c: "VARCHAR" for c in columns}
    mappings = [{"source": c, "target": c, "confidence": 0.99} for c in columns]

    result = run_file_preflight(
        columns=columns,
        column_types=column_types,
        row_count=len(sample_rows),
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sample_rows=sample_rows,
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_db_type="mongodb",
        validation_mode="strict",
    )
    assert result["passed"] is True
    gate_status = {g["id"]: g["status"] for g in result["gates"]}
    assert gate_status.get("g5_dry_run") == "pass"
    assert gate_status.get("g6_target_ddl") == "pass"


def test_preflight_still_blocks_high_null_rate_for_sql() -> None:
    """Relational targets should keep the high-null-rate guard."""
    sample_rows = [
        {"id": "1", "name": "Alice", "address": ""},
        {"id": "2", "name": "Bob", "address": ""},
        {"id": "3", "name": "Charlie", "address": ""},
    ]
    columns = list(sample_rows[0].keys())
    column_types = {c: "VARCHAR" for c in columns}
    mappings = [{"source": c, "target": c, "confidence": 0.99} for c in columns]

    result = run_file_preflight(
        columns=columns,
        column_types=column_types,
        row_count=len(sample_rows),
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sample_rows=sample_rows,
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_db_type="postgresql",
        validation_mode="strict",
    )
    assert result["passed"] is False
    gate_status = {g["id"]: g["status"] for g in result["gates"]}
    assert gate_status.get("g6_target_ddl") == "block"
