"""UUID create-new must not false-block as lossy UUID→TEXT/VARCHAR."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_mysql_uuid_ddl_is_char36():
    from services.type_system import ddl_type

    assert ddl_type("mysql", "UUID") == "CHAR(36)"
    # Round-trip must not collapse CHAR(36) → TEXT via STRING fall-through.
    assert ddl_type("mysql", "CHAR(36)") == "CHAR(36)"
    assert ddl_type("mysql", "VARCHAR(36)") == "VARCHAR(36)"
    # Plain VARCHAR(36) must NOT become PostgreSQL UUID (non-UUID text would fail).
    assert ddl_type("postgresql", "VARCHAR(36)") == "VARCHAR(36)"
    assert ddl_type("postgresql", "UUID") == "UUID"
    # Ordinary string widths must NOT be rewritten to UUID DDL (silent truncate).
    assert ddl_type("mysql", "VARCHAR(50)") != "CHAR(36)"
    assert ddl_type("mysql", "VARCHAR(255)") != "CHAR(36)"
    assert ddl_type("mysql", "VARCHAR(45)") != "CHAR(36)"
    assert "36" not in ddl_type("mysql", "VARCHAR(50)")


def test_uuid_capacity_carrier_not_lossy():
    from services.type_system import (
        is_lossy_coercion,
        is_precision_collapse_coercion,
        uuid_would_collapse,
    )

    assert uuid_would_collapse("UUID", "CHAR(36)") is False
    assert uuid_would_collapse("UUID", "VARCHAR(36)") is False
    assert uuid_would_collapse("UUID", "VARCHAR2(36)") is False
    assert is_precision_collapse_coercion("UUID", "CHAR(36)") is False
    assert is_lossy_coercion("UUID", "CHAR(36)") is False

    # Bare / opaque sinks still collapse (wave 77 honesty).
    assert uuid_would_collapse("UUID", "VARCHAR") is True
    assert uuid_would_collapse("UUID", "TEXT") is True
    assert uuid_would_collapse("UUID", "STRING") is True
    assert is_lossy_coercion("UUID", "VARCHAR") is True


def test_create_new_mapping_keeps_logical_uuid():
    from services.semantic_mapper import map_columns
    from services.type_system import create_new_mapping_target_type

    assert create_new_mapping_target_type("UUID", "mysql") == "UUID"
    assert create_new_mapping_target_type("VARCHAR", "mysql") == "TEXT"

    mappings = map_columns(
        source_columns=["meta_deviceId"],
        target_columns=[],
        source_schemas=[
            {"name": "meta_deviceId", "inferred_type": "UUID", "samples": []}
        ],
        destination_db_type="mysql",
        destination_table_exists=False,
    )
    assert len(mappings) == 1
    assert mappings[0]["create_new"] is True
    assert mappings[0]["target_type"] == "UUID"
    assert "CHAR(36)" in mappings[0]["reasoning"]


def test_validate_coercion_uuid_to_uuid_create_new_passes_strict():
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )

    issues = validate_mapping_coercions(
        mappings=[
            {
                "source": "meta_deviceId",
                "target": "meta_device_id",
                "confidence": 0.93,
                "create_new": True,
            }
        ],
        source_types={"meta_deviceId": "UUID"},
        target_types={"meta_device_id": "UUID"},
        validation_mode="strict",
    )
    assert coerce_blocks_transfer(issues) is False


def test_validate_coercion_uuid_to_char36_passes_strict():
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )

    issues = validate_mapping_coercions(
        mappings=[
            {
                "source": "meta_deviceId",
                "target": "meta_device_id",
                "confidence": 0.93,
                "create_new": True,
            }
        ],
        source_types={"meta_deviceId": "UUID"},
        target_types={"meta_device_id": "CHAR(36)"},
        validation_mode="strict",
    )
    assert coerce_blocks_transfer(issues) is False


def test_uuid_mismatch_remediation_suggests_uuid_not_varchar():
    from services.validation_assistant import explain_validation

    explained = explain_validation(
        {
            "passed": False,
            "blockers": [
                {
                    "id": "g3_schema_contract",
                    "message": (
                        "Lossy coercion: meta_deviceId (UUID) → meta_device_id (VARCHAR)"
                    ),
                },
                {
                    "id": "g9_data_integrity",
                    "message": (
                        "Data integrity failed: meta_deviceId (UUID) → meta_device_id (VARCHAR)"
                    ),
                },
            ],
            "gates": [],
        },
        use_llm=False,
    )
    actions = explained.get("suggested_actions") or []
    uuid_fix = [
        a for a in actions
        if a.get("kind") == "change_target_type" and a.get("to_type") == "UUID"
    ]
    assert uuid_fix, actions
    assert not any(
        a.get("kind") == "change_target_type" and a.get("to_type") == "VARCHAR"
        for a in actions
    )
