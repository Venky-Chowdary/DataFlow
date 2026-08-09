"""Mongo→Postgres (and peer routes) must not false-block value-preserving wires.

Live symptom (Transfer Studio): existing ``portpolios`` table with TEXT id /
JSONB theme_colors failed Validate with ``Lossy / fidelity collapse`` on
ObjectId→TEXT and VARCHAR→JSONB. Create-new still invents VARCHAR(24) / JSONB
honestly; existing-table loads must match Airbyte/Fivetran-class behavior.
"""

from __future__ import annotations

import pytest

from services.ddl_compatibility import evaluate_ddl_compatibility
from services.type_system import (
    document_domain_would_invent,
    is_lossy_coercion,
    objectid_would_collapse,
    specialty_carrier_would_collapse,
    specialty_domain_would_invent,
)


@pytest.mark.parametrize(
    "src,tgt,dest,exists,expect_lossy",
    [
        # Mongo ObjectId → existing PG TEXT / MySQL LONGTEXT / BQ STRING
        ("OBJECTID", "TEXT", "postgresql", True, False),
        ("OBJECTID", "TEXT", "postgresql", False, False),
        ("OBJECTID", "VARCHAR(24)", "postgresql", True, False),
        ("OBJECTID", "VARCHAR(12)", "postgresql", True, True),
        ("OBJECTID", "LONGTEXT", "mysql", True, False),
        ("OBJECTID", "STRING", "bigquery", True, False),
        ("OBJECTID", "INTEGER", "postgresql", True, True),
        # Nested document → PG JSONB (create-new representation wire)
        ("ARRAY", "JSONB", "postgresql", True, False),
        ("ARRAY", "JSONB", "postgresql", False, False),
        ("STRUCT<a:INT>", "JSONB", "postgresql", True, False),
        # Open string → JSONB invents document domain on create-new only
        ("VARCHAR", "JSONB", "postgresql", False, True),
        ("VARCHAR", "JSONB", "postgresql", True, False),
        ("TEXT", "JSON", "mysql", True, False),
        ("TEXT", "JSON", "mysql", False, True),
        # Common widen / text peers
        ("INTEGER", "BIGINT", "postgresql", True, False),
        ("VARCHAR", "TEXT", "postgresql", True, False),
    ],
)
def test_cross_route_fidelity_matrix(src, tgt, dest, exists, expect_lossy):
    assert (
        is_lossy_coercion(src, tgt, dest_db=dest, dest_table_exists=exists)
        is expect_lossy
    ), (src, tgt, dest, exists)


def test_objectid_text_specialty_and_objectid_helpers():
    assert specialty_carrier_would_collapse("OBJECTID", "TEXT") is False
    assert objectid_would_collapse("OBJECTID", "TEXT") is False
    assert specialty_carrier_would_collapse("OBJECTID", "VARCHAR(10)") is True


def test_document_invent_existing_vs_create_new():
    assert document_domain_would_invent(
        "VARCHAR", "JSONB", dest_db="postgresql", dest_table_exists=True
    ) is False
    assert specialty_domain_would_invent(
        "VARCHAR", "JSONB", dest_db="postgresql", dest_table_exists=True
    ) is False
    assert document_domain_would_invent(
        "VARCHAR", "JSONB", dest_db="postgresql", dest_table_exists=False
    ) is True
    assert specialty_domain_would_invent(
        "VARCHAR", "JSONB", dest_db="postgresql", dest_table_exists=False
    ) is True
    # TEXT→INET still invents (not a document load exception).
    assert specialty_domain_would_invent("TEXT", "INET", dest_table_exists=True) is True


def test_ddl_compat_mongo_portpolios_existing_table_green():
    """Recreate the live job mapping shape: ObjectId→TEXT + VARCHAR→JSONB."""
    ok, issues = evaluate_ddl_compatibility(
        mappings=[
            {"source": "_id", "target": "id"},
            {"source": "userId", "target": "user_id"},
            {"source": "theme_colors", "target": "theme_colors"},
        ],
        source_schema={
            "_id": "OBJECTID",
            "userId": "OBJECTID",
            "theme_colors": "VARCHAR",
        },
        target_schema={
            "id": "TEXT",
            "user_id": "TEXT",
            "theme_colors": "JSONB",
        },
        table_exists=True,
        dest_connected=True,
        dest_db_type="postgresql",
        allow_create=False,
    )
    lossy = [i for i in issues if "Lossy" in i or "lossy" in i.lower()]
    assert lossy == [], issues
    assert ok is True


def test_ddl_compat_create_new_varchar_jsonb_still_blocks():
    _ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "theme_colors", "target": "theme_colors"}],
        source_schema={"theme_colors": "VARCHAR"},
        target_schema={"theme_colors": "JSONB"},
        table_exists=False,
        dest_connected=True,
        dest_db_type="postgresql",
        allow_create=True,
    )
    blob = " ".join(issues)
    assert "Lossy" in blob


def test_map_coercion_validator_honors_existing_table():
    """Bugbot: Map must pass dest_table_exists — not only G3/DDL."""
    from services.type_coercion_validator import (
        coerce_blocks_transfer,
        validate_mapping_coercions,
    )

    mappings = [
        {"source": "_id", "target": "id", "confidence": 0.95},
        {"source": "theme_colors", "target": "theme_colors", "confidence": 0.95},
    ]
    source_types = {"_id": "OBJECTID", "theme_colors": "VARCHAR"}
    target_types = {"id": "TEXT", "theme_colors": "JSONB"}
    blocked = validate_mapping_coercions(
        mappings,
        source_types=source_types,
        target_types=target_types,
        dest_db_type="postgresql",
        dest_table_exists=False,
    )
    assert coerce_blocks_transfer(blocked) is True
    cleared = validate_mapping_coercions(
        mappings,
        source_types=source_types,
        target_types=target_types,
        dest_db_type="postgresql",
        dest_table_exists=True,
    )
    assert coerce_blocks_transfer(cleared) is False
