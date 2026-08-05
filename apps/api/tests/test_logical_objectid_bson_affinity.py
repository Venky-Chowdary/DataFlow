"""LOGICAL_OBJECTID first-class + schemaless G3 BSON affinity."""

from __future__ import annotations

from preflight import (
    ColumnMapping,
    ColumnSchema,
    DestinationConfig,
    PreflightContext,
    SourceConfig,
    TransferPlan,
)
from preflight.gates import gate_g3_schema_contract
from preflight.models import GateStatus


def test_logical_objectid_first_class():
    from services.type_system import (
        LOGICAL_OBJECTID,
        create_new_mapping_target_type,
        ddl_type,
        is_lossy_coercion,
        normalize_logical_type,
        objectid_would_collapse,
    )

    assert normalize_logical_type("OBJECTID") == LOGICAL_OBJECTID
    assert normalize_logical_type("objectId") == LOGICAL_OBJECTID
    assert ddl_type("postgresql", "OBJECTID") == "VARCHAR(24)"
    assert ddl_type("mysql", "OBJECTID") == "CHAR(24)"
    assert ddl_type("mongodb", "OBJECTID") == "objectId"
    assert create_new_mapping_target_type("OBJECTID", "mysql") == "CHAR(24)"
    assert is_lossy_coercion("OBJECTID", "VARCHAR(24)") is False
    assert is_lossy_coercion("OBJECTID", "CHAR(24)") is False
    assert is_lossy_coercion("OBJECTID", "TEXT") is True
    assert objectid_would_collapse("OBJECTID", "TEXT") is True
    assert objectid_would_collapse("OBJECTID", "VARCHAR(24)") is False
    assert objectid_would_collapse("OBJECTID", "CHAR(24)") is False


def test_assess_bson_affinity_blocks_objectid_to_number():
    from services.type_system import assess_bson_affinity

    risks = assess_bson_affinity(
        "OBJECTID",
        "INTEGER",
        destination_db_type="mongodb",
    )
    assert risks
    assert risks[0]["kind"] == "bson_affinity_block"
    assert risks[0]["severity"] == "block"


def test_g3_schemaless_still_skip_when_types_compatible():
    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="postgresql",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="email", inferred_type="VARCHAR")],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="mongodb",
            db_type="mongodb",
            connected=True,
            can_write=True,
            target_columns=[],
        ),
        mappings=[ColumnMapping(source="email", target="email", confidence=0.9)],
        dry_run_passed=True,
        ddl_compatible=True,
        estimated_bytes=1000,
        available_staging_bytes=10_000_000,
    )
    result = gate_g3_schema_contract(PreflightContext(plan=plan))
    assert result.status == GateStatus.SKIP
    assert "schemaless" in result.message.lower()


def test_g3_schemaless_bson_affinity_blocks_objectid_to_integer():
    plan = TransferPlan(
        source=SourceConfig(
            kind="database",
            db_type="mongodb",
            connected=True,
            parseable=True,
            columns=[ColumnSchema(name="_id", inferred_type="OBJECTID")],
            row_count_estimate=10,
        ),
        destination=DestinationConfig(
            kind="mongodb",
            db_type="mongodb",
            connected=True,
            can_write=True,
            target_columns=[ColumnSchema(name="legacy_id", inferred_type="INTEGER")],
        ),
        mappings=[
            ColumnMapping(
                source="_id",
                target="legacy_id",
                confidence=0.9,
                target_type="INTEGER",
            ),
        ],
        dry_run_passed=True,
        ddl_compatible=True,
        estimated_bytes=1000,
        available_staging_bytes=10_000_000,
    )
    blocked = gate_g3_schema_contract(PreflightContext(plan=plan))
    assert blocked.status == GateStatus.BLOCK
    blob = str((blocked.details or {}).get("issues", [])) + blocked.message
    assert "affinity" in blob.lower() or "ObjectId" in blob or "objectid" in blob.lower()

    plan.mappings[0].risk_acknowledged = True
    cleared = gate_g3_schema_contract(PreflightContext(plan=plan))
    assert cleared.status == GateStatus.SKIP
    assert (cleared.details or {}).get("bson_affinity") or "affinity" in cleared.message.lower()
