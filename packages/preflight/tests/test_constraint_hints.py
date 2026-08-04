"""Unit tests for soft constraint hints (not a GateId)."""

from __future__ import annotations

from preflight import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    PreflightContext,
    SourceConfig,
    TransferPlan,
    assess_constraint_compatibility,
)


def test_empty_schema_returns_empty_hints():
    plan = TransferPlan(
        source=SourceConfig(kind="file", columns=[]),
        destination=DestinationConfig(kind="database", target_columns=[]),
    )
    assert assess_constraint_compatibility(PreflightContext(plan=plan)) == []
    assert assess_constraint_compatibility({}) == []


def test_fk_mismatch_returns_warning_string():
    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="id", inferred_type="INTEGER")],
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            table_exists=True,
            target_columns=[
                ColumnSchema(name="id", inferred_type="INTEGER"),
                ColumnSchema(name="customer_id", inferred_type="INTEGER"),
            ],
        ),
        mappings=[
            ColumnMapping(source="id", target="id", confidence=1.0),
        ],
        destination_foreign_keys=[
            {
                "columns": ["customer_id"],
                "referenced_table": "customers",
                "referenced_columns": ["id"],
            }
        ],
    )
    hints = assess_constraint_compatibility(PreflightContext(plan=plan))
    assert len(hints) == 1
    assert "customer_id" in hints[0]
    assert "foreign key" in hints[0].lower()


def test_mapped_fk_produces_no_hint():
    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            connected=True,
            columns=[
                ColumnSchema(name="id", inferred_type="INTEGER"),
                ColumnSchema(name="customer_id", inferred_type="INTEGER"),
            ],
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            table_exists=True,
            target_columns=[
                ColumnSchema(name="id", inferred_type="INTEGER"),
                ColumnSchema(name="customer_id", inferred_type="INTEGER"),
            ],
        ),
        mappings=[
            ColumnMapping(source="id", target="id", confidence=1.0),
            ColumnMapping(source="customer_id", target="customer_id", confidence=1.0),
        ],
        destination_foreign_keys=[
            {
                "columns": ["customer_id"],
                "referenced_table": "customers",
                "referenced_columns": ["id"],
            }
        ],
    )
    assert assess_constraint_compatibility(PreflightContext(plan=plan)) == []
