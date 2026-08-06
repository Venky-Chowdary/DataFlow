"""Real create-new type matrix — Mongo/PG specialties must not false-block Validate.

Fixtures use realistic ObjectId hex, UUID, and INET values (not mocks-as-product).
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


REAL_OBJECT_IDS = [
    "507f1f77bcf86cd799439011",
    "507f191e810c19729de860ea",
    "64f0c2a1b2c3d4e5f6789012",
]
REAL_UUIDS = [
    "550e8400-e29b-41d4-a716-446655440000",
    "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
]
REAL_IPS = ["192.168.1.1", "2001:db8::1", "10.0.0.42"]


def test_objectid_create_new_mysql_stamps_logical_and_passes_strict():
    from services.semantic_mapper import map_columns
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )
    from services.type_system import (
        ddl_type,
        is_lossy_coercion,
        specialty_carrier_would_collapse,
    )

    assert ddl_type("mysql", "OBJECTID") == "CHAR(24)"
    assert specialty_carrier_would_collapse("OBJECTID", "VARCHAR(24)") is False
    assert specialty_carrier_would_collapse("OBJECTID", "CHAR(24)") is False
    assert is_lossy_coercion("OBJECTID", "VARCHAR(24)") is False
    assert is_lossy_coercion("OBJECTID", "CHAR(24)") is False

    mappings = map_columns(
        source_columns=["_id"],
        target_columns=[],
        source_schemas=[{
            "name": "_id",
            "inferred_type": "OBJECTID",
            "samples": REAL_OBJECT_IDS,
        }],
        destination_db_type="mysql",
        destination_table_exists=False,
    )
    assert mappings[0]["create_new"] is True
    # Stamp physical off-engine sink so Map matches CREATE (not silent OBJECTID→OBJECTID).
    assert mappings[0]["target_type"] == "CHAR(24)"
    assert "CHAR(24)" in mappings[0]["reasoning"]

    issues = validate_mapping_coercions(
        mappings=[{
            "source": "_id",
            "target": "id",
            "confidence": 0.93,
            "create_new": True,
        }],
        source_types={"_id": "OBJECTID"},
        target_types={"id": "CHAR(24)"},
        validation_mode="strict",
    )
    assert coerce_blocks_transfer(issues) is False

    # Physical wire after writer ddl_type also must not block.
    issues_phys = validate_mapping_coercions(
        mappings=[{"source": "_id", "target": "id", "confidence": 0.93}],
        source_types={"_id": "OBJECTID"},
        target_types={"id": "CHAR(24)"},
        validation_mode="strict",
    )
    assert coerce_blocks_transfer(issues_phys) is False


def test_objectid_bare_text_still_collapses():
    from services.type_system import specialty_carrier_would_collapse

    assert specialty_carrier_would_collapse("OBJECTID", "TEXT") is True
    assert specialty_carrier_would_collapse("OBJECTID", "VARCHAR") is True


def test_inet_create_new_off_engine_stamps_logical():
    from services.semantic_mapper import map_columns
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )

    mappings = map_columns(
        source_columns=["ip"],
        target_columns=[],
        source_schemas=[{
            "name": "ip",
            "inferred_type": "INET",
            "samples": REAL_IPS,
        }],
        destination_db_type="snowflake",
        destination_table_exists=False,
    )
    # Off-engine: stamp physical VARCHAR so Accept risk matches CREATE.
    stamped = (mappings[0]["target_type"] or "").upper()
    assert stamped == "VARCHAR" or stamped.startswith("VARCHAR")
    assert stamped != "INET"
    issues = validate_mapping_coercions(
        mappings=[{
            "source": "ip",
            "target": "ip",
            "confidence": 0.93,
            "create_new": True,
            "risk_acknowledged": True,
        }],
        source_types={"ip": "INET"},
        target_types={"ip": mappings[0]["target_type"]},
        validation_mode="strict",
    )
    assert coerce_blocks_transfer(issues) is False


def test_decimal_narrow_same_logical_blocks_g9_coercion():
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )

    issues = validate_mapping_coercions(
        mappings=[{"source": "amt", "target": "amt", "confidence": 0.99}],
        source_types={"amt": "DECIMAL(38,10)"},
        target_types={"amt": "DECIMAL(10,2)"},
        validation_mode="strict",
    )
    assert coerce_blocks_transfer(issues) is True
    assert issues[0]["lossy"] is True


def test_float_to_decimal_remediation_not_bare_varchar():
    from services.validation_assistant import _remap_to_type_for_mismatch

    # Default dest_db is postgresql — physical IEEE64 stamp is DOUBLE PRECISION.
    assert _remap_to_type_for_mismatch("FLOAT", "DECIMAL(38,10)") == "DOUBLE PRECISION"
    assert _remap_to_type_for_mismatch("FLOAT", "DECIMAL(38,10)", dest_db="mysql") == "DOUBLE"
    # ObjectId remap stamps the width-safe create-new wire (not bare specialty token).
    assert _remap_to_type_for_mismatch("OBJECTID", "VARCHAR") == "VARCHAR(24)"
    assert _remap_to_type_for_mismatch("UUID", "TEXT") == "UUID"


def test_integrity_coercion_safety_objectid_create_new_real_rows():
    from services.data_integrity import _check_coercion_safety

    rows = [{"_id": oid, "name": f"user-{i}"} for i, oid in enumerate(REAL_OBJECT_IDS)]
    report = _check_coercion_safety(
        mappings=[{
            "source": "_id",
            "target": "id",
            "confidence": 0.93,
            "create_new": True,
        }],
        source_types={"_id": "OBJECTID"},
        target_types={"id": "VARCHAR(24)"},
        dest_kind="mysql",
        validation_mode="strict",
        rows=rows,
    )
    assert report["passed"] is True
    assert report["blocks_transfer"] is False


def test_uuid_mysql_create_new_still_solid_with_real_uuids():
    from services.semantic_mapper import map_columns
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )

    mappings = map_columns(
        source_columns=["device_id"],
        target_columns=[],
        source_schemas=[{
            "name": "device_id",
            "inferred_type": "UUID",
            "samples": REAL_UUIDS,
        }],
        destination_db_type="mysql",
        destination_table_exists=False,
    )
    assert mappings[0]["target_type"] == "CHAR(36)"
    issues = validate_mapping_coercions(
        mappings=[{
            "source": "device_id",
            "target": "device_id",
            "confidence": 0.93,
            "create_new": True,
            "risk_acknowledged": True,
        }],
        source_types={"device_id": "UUID"},
        target_types={"device_id": "CHAR(36)"},
        validation_mode="strict",
    )
    assert coerce_blocks_transfer(issues) is False
    stamped_risks = mappings[0].get("create_new_risks") or []
    assert any((r.get("kind") == "uuid_domain") for r in stamped_risks)


def test_uuid_bigquery_create_new_stamps_string_and_warns_not_silent_green():
    """BQ has no UUID type — Validate must warn polarity, not green UUID→UUID."""
    from services.migration_risk_contract import create_migration_risk_contract
    from services.semantic_mapper import map_columns
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )
    from services.type_system import create_new_mapping_target_type

    # Width-safe create-new wires — bare STRING still collapses ObjectId/UUID polarity.
    assert create_new_mapping_target_type("UUID", "bigquery") == "STRING(36)"
    assert create_new_mapping_target_type("UUID", "databricks") == "VARCHAR(36)"
    assert create_new_mapping_target_type("UUID", "sqlite") == "TEXT"

    mappings = map_columns(
        source_columns=["device_id"],
        target_columns=[],
        source_schemas=[{
            "name": "device_id",
            "inferred_type": "UUID",
            "samples": REAL_UUIDS,
        }],
        destination_db_type="bigquery",
        destination_table_exists=False,
    )
    assert mappings[0]["target_type"] == "STRING(36)"

    contract = create_migration_risk_contract(
        column="device_id",
        source_type="UUID",
        destination_type="STRING",
        approved_by="ops@dataflow.app",
        reason="UUID→STRING create-new accepted",
        execution_policy="CAST_AND_CONTINUE",
    )
    issues = validate_mapping_coercions(
        mappings=[{
            "source": "device_id",
            "target": "device_id",
            "confidence": 0.93,
            "create_new": True,
            "risk_acknowledged": True,
            "risk_contract": contract.to_dict(),
        }],
        source_types={"device_id": "UUID"},
        target_types={"device_id": "STRING"},
        validation_mode="strict",
    )
    assert coerce_blocks_transfer(issues) is False
    assert issues and issues[0]["severity"] == "warn"
    assert issues[0]["lossy"] is True

    # Strict without Accept risk stays blocked (domain polarity).
    blocked = validate_mapping_coercions(
        mappings=[{
            "source": "device_id",
            "target": "device_id",
            "confidence": 0.93,
            "create_new": True,
        }],
        source_types={"device_id": "UUID"},
        target_types={"device_id": "STRING"},
        validation_mode="strict",
    )
    assert coerce_blocks_transfer(blocked) is True


def test_create_new_stamped_target_type_authoritative_without_live_ddl():
    """Missing live dest types must not invent UUID→UUID green for BQ create-new."""
    from services.migration_risk_contract import create_migration_risk_contract
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )
    from services.type_system import resolve_mapping_target_type

    contract = create_migration_risk_contract(
        column="device_id",
        source_type="UUID",
        destination_type="STRING",
        approved_by="ops@dataflow.app",
        reason="UUID→STRING create-new accepted",
        execution_policy="CAST_AND_CONTINUE",
    )
    mapping = {
        "source": "device_id",
        "target": "device_id",
        "confidence": 0.93,
        "create_new": True,
        "target_type": "STRING",
        "risk_acknowledged": True,
        "risk_contract": contract.to_dict(),
    }
    assert resolve_mapping_target_type(
        mapping, target_types={}, source_type="UUID"
    ) == "STRING"

    issues = validate_mapping_coercions(
        mappings=[mapping],
        source_types={"device_id": "UUID"},
        target_types={},  # live DDL absent — stamped type must still warn
        validation_mode="strict",
    )
    assert coerce_blocks_transfer(issues) is False
    assert issues and issues[0]["severity"] == "warn"
    assert issues[0]["target_type"] == "STRING"


def test_existing_column_live_ddl_wins_over_stale_stamp():
    from services.type_system import resolve_mapping_target_type

    assert resolve_mapping_target_type(
        {
            "source": "amount",
            "target": "amount",
            "create_new": False,
            "target_type": "VARCHAR",
        },
        target_types={"amount": "DECIMAL(18,2)"},
        source_type="DECIMAL(18,2)",
    ) == "DECIMAL(18,2)"
