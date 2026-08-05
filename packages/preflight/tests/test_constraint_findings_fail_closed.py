"""FK / constraint findings — fail-closed honesty for referential coverage."""

from __future__ import annotations

from preflight.constraint_hints import (
    assess_constraint_compatibility,
    constraint_findings_block_transfer,
    referential_integrity_posture,
)
from preflight.models import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)


def _plan_with_unmapped_fk(*, table_exists: bool = True) -> TransferPlan:
    return TransferPlan(
        source=SourceConfig(
            kind="database",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="id", inferred_type="INTEGER")],
        ),
        destination=DestinationConfig(
            kind="database",
            connected=True,
            table_exists=table_exists,
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


def test_unmapped_fk_is_structured_finding_not_soft_only_string():
    findings = assess_constraint_compatibility(PreflightContext(plan=_plan_with_unmapped_fk()))
    assert findings
    assert isinstance(findings[0], dict)
    assert findings[0]["code"] == "fk_column_unmapped"
    assert findings[0]["coverage"] == "destination_fk_metadata"
    assert findings[0]["severity"] in {"block", "ack_required"}
    assert "customer_id" in findings[0]["message"]


def test_strict_mode_unmapped_fk_blocks_without_ack():
    findings = assess_constraint_compatibility(
        PreflightContext(plan=_plan_with_unmapped_fk()),
        validation_mode="strict",
        fk_risk_acknowledged=False,
    )
    assert constraint_findings_block_transfer(
        findings, validation_mode="strict", fk_risk_acknowledged=False
    ) is True


def test_strict_mode_unmapped_fk_clears_with_ack():
    findings = assess_constraint_compatibility(
        PreflightContext(plan=_plan_with_unmapped_fk()),
        validation_mode="strict",
        fk_risk_acknowledged=True,
    )
    assert constraint_findings_block_transfer(
        findings, validation_mode="strict", fk_risk_acknowledged=True
    ) is False
    # Still surfaces for audit — never invent "RI proven".
    posture = referential_integrity_posture(findings, population_orphan_probe_ran=False)
    assert posture["proven"] is False
    assert posture["coverage"] == "destination_fk_metadata"


def test_referential_integrity_never_claimed_from_hints_alone():
    findings = assess_constraint_compatibility(PreflightContext(plan=_plan_with_unmapped_fk()))
    posture = referential_integrity_posture(findings, population_orphan_probe_ran=False)
    assert posture["proven"] is False
    assert posture["population_orphan_probe_ran"] is False
    assert "population" in posture["note"].lower() or "not proven" in posture["note"].lower()
