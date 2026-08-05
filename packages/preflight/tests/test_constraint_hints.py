"""Unit tests for constraint findings (schema FK coverage)."""

from __future__ import annotations

from preflight import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    PreflightContext,
    SourceConfig,
    TransferPlan,
    assess_constraint_compatibility,
    constraint_hint_messages,
)


def test_empty_schema_returns_empty_hints():
    plan = TransferPlan(
        source=SourceConfig(kind="file", columns=[]),
        destination=DestinationConfig(kind="database", target_columns=[]),
    )
    assert assess_constraint_compatibility(PreflightContext(plan=plan)) == []
    assert assess_constraint_compatibility({}) == []


def test_fk_mismatch_returns_structured_finding():
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
    findings = assess_constraint_compatibility(PreflightContext(plan=plan))
    assert len(findings) == 1
    assert findings[0]["code"] == "fk_column_unmapped"
    assert "customer_id" in findings[0]["message"]
    assert "foreign key" in findings[0]["message"].lower()
    msgs = constraint_hint_messages(findings)
    assert len(msgs) == 1
    assert "customer_id" in msgs[0]


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
