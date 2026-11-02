"""Client-deploy invent SSOT: Validate, drift dest_db, by-target Map resolve."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_resolve_mapping_target_type_refuses_source_invent_on_existing():
    from services.type_system import resolve_mapping_target_type

    mapping = {
        "source": "skills",
        "target": "skills",
        "create_new": False,
        # no target_type stamp
    }
    # No live dest types — must not invent ARRAY from source.
    assert (
        resolve_mapping_target_type(
            mapping,
            target_types={},
            source_type="ARRAY",
            dest_db_type="mysql",
        )
        == ""
    )
    # Live wins.
    assert (
        resolve_mapping_target_type(
            mapping,
            target_types={"skills": "JSON"},
            source_type="ARRAY",
            dest_db_type="mysql",
        )
        == "JSON"
    )
    # Map stamp wins when live absent.
    stamped = {**mapping, "target_type": "JSON"}
    assert (
        resolve_mapping_target_type(
            stamped,
            target_types={},
            source_type="ARRAY",
            dest_db_type="mysql",
        )
        == "JSON"
    )


def test_schema_drift_array_json_not_narrow_with_dest_db():
    from services.schema_drift import classify_schema_change

    old = {"columns": {"skills": "ARRAY"}, "nullable": {"skills": True}, "primary_key": []}
    new = {"columns": {"skills": "JSON"}, "nullable": {"skills": True}, "primary_key": []}
    # Without dest_db — fail closed (lossy).
    bare = classify_schema_change(old, new)
    assert bare["severity"] == "breaking"
    assert any(b.get("kind") == "narrow_type" for b in bare["breaking"])
    # MySQL create-new ARRAY wire is JSON — representation, not narrow.
    mysql = classify_schema_change(old, new, dest_db="mysql")
    assert not any(b.get("kind") == "narrow_type" for b in mysql["breaking"]), mysql


def test_resolve_mapping_dest_types_by_target_not_index_zip():
    from connectors.writer_common import resolve_mapping_dest_types

    # Mappings reordered vs target_cols — index zip would stamp wrong types.
    target_cols = ["id", "skills", "title"]
    mappings = [
        {"source": "title", "target": "title", "target_type": "TEXT"},
        {"source": "skills", "target": "skills", "target_type": "JSON"},
        {"source": "id", "target": "id", "target_type": "BIGINT"},
    ]
    out = resolve_mapping_dest_types(
        target_cols,
        mappings,
        {},
        logical_types=["VARCHAR", "VARCHAR", "VARCHAR"],
        default="VARCHAR",
    )
    assert out["id"] == "BIGINT"
    assert out["skills"] == "JSON"
    assert out["title"] == "TEXT"


def test_coercion_validator_blocks_pending_dest_stamp():
    from services.type_coercion_validator import validate_mapping_coercions

    for mode in ("strict", "balanced", "review"):
        issues = validate_mapping_coercions(
            mappings=[
                {
                    "source": "skills",
                    "target": "skills",
                    "confidence": 0.9,
                    "create_new": False,
                }
            ],
            source_types={"skills": "ARRAY"},
            target_types={},
            dest_db_type="mysql",
            validation_mode=mode,
        )
        assert issues and issues[0]["severity"] == "block", mode
        assert "pending" in (issues[0].get("reason") or "").lower()


def test_semantic_mapper_pending_dest_leaves_target_type_empty():
    from services.semantic_mapper import map_columns

    mappings = map_columns(
        source_columns=["skills"],
        target_columns=[],
        source_schemas=[{"name": "skills", "inferred_type": "ARRAY"}],
        target_schemas=None,
        destination_db_type="mysql",
        destination_table_exists=True,
    )
    assert mappings and mappings[0]["assignment_strategy"] == "pending_dest_schema"
    assert not str(mappings[0].get("target_type") or "").strip()
    assert mappings[0].get("create_new") is False


def test_g9_coercion_safety_passes_dest_db_and_blocks_schemaless_invent():
    from services.data_integrity import _check_coercion_safety
    from services.type_system import (
        create_new_mapping_target_type,
        resolve_mapping_target_type,
    )

    mapping = {
        "source": "id",
        "target": "id",
        "create_new": True,
        "confidence": 0.95,
    }
    # Without dest_db, empty create-new stamp falls back to source identity.
    assert (
        resolve_mapping_target_type(
            mapping, target_types={}, source_type="UUID", dest_db_type=""
        )
        == "UUID"
    )
    # With dest_db, BQ create-new stamps width-safe STRING — G9 must thread dest_kind.
    bq_stamp = create_new_mapping_target_type("UUID", "bigquery")
    assert resolve_mapping_target_type(
        mapping,
        target_types={},
        source_type="UUID",
        dest_db_type="bigquery",
    ) == bq_stamp
    assert "STRING" in bq_stamp.upper()

    # Dynamo schemaless: pending invent still blocks (not demoted to warning).
    ddb = _check_coercion_safety(
        [
            {
                "source": "skills",
                "target": "skills",
                "create_new": False,
                "confidence": 0.9,
            }
        ],
        {"skills": "ARRAY"},
        {},
        dest_kind="dynamodb",
        validation_mode="strict",
    )
    assert ddb.get("blocks_transfer") is True
    assert ddb.get("passed") is False
