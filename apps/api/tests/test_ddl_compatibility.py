"""DDL compatibility (G6) validation tests."""

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) in sys.path:
    sys.path.remove(str(_API_ROOT))
sys.path.insert(0, str(_API_ROOT))

from services.ddl_compatibility import evaluate_ddl_compatibility


def test_passes_compatible_mapping():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "id", "target": "user_id", "confidence": 0.95}],
        source_schema={"id": "INTEGER"},
        target_schema={"user_id": "BIGINT"},
        table_exists=True,
        dest_connected=True,
        sample_rows=[{"id": "1"}, {"id": "2"}],
    )
    assert ok
    assert issues == []


def test_fails_missing_target_column():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "email", "target": "email_address", "confidence": 0.9}],
        source_schema={"email": "VARCHAR"},
        target_schema={"email": "TEXT"},
        table_exists=True,
        dest_connected=True,
    )
    assert not ok
    assert any("does not exist" in i for i in issues)


def test_fails_varchar_width_overflow():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "code", "target": "code", "confidence": 0.9}],
        source_schema={"code": "VARCHAR"},
        target_schema={"code": "VARCHAR(5)"},
        table_exists=True,
        dest_connected=True,
        sample_rows=[{"code": "ABCDEFGHIJ"}],
    )
    assert not ok
    assert any("width overflow" in i.lower() for i in issues)


def test_fails_duplicate_pk_in_source_sample():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "order_id", "target": "order_id", "confidence": 1.0}],
        source_schema={"order_id": "INTEGER"},
        target_schema={"order_id": "BIGINT"},
        table_exists=True,
        dest_connected=True,
        sample_rows=[{"order_id": "1"}, {"order_id": "1"}],
    )
    assert not ok
    assert any("duplicate" in i.lower() for i in issues)


def test_passes_when_target_column_case_differs():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "Email", "target": "email_address", "confidence": 0.9}],
        source_schema={"Email": "VARCHAR"},
        target_schema={"EMAIL_ADDRESS": "TEXT"},
        table_exists=True,
        dest_connected=True,
    )
    assert ok
    assert issues == []


def test_mongodb_schemaless_does_not_block_on_sql_ddl_rules():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "description", "target": "description", "confidence": 0.95}],
        source_schema={"description": "VARCHAR"},
        target_schema={},
        table_exists=False,
        dest_connected=True,
        dest_db_type="mongodb",
        sample_rows=[{"description": "x" * 1200}],
    )
    assert ok
    assert issues == []


@pytest.mark.parametrize("dest_db_type", ["postgresql", "mysql", "snowflake", "bigquery", "mongodb", "dynamodb", "redis"])
def test_connector_matrix_simple_mapping_is_not_falsely_blocked(dest_db_type: str):
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "id", "target": "id", "confidence": 0.99}],
        source_schema={"id": "INTEGER"},
        target_schema={},
        table_exists=False,
        dest_connected=True,
        dest_db_type=dest_db_type,
        sample_rows=[{"id": 1}, {"id": 2}],
    )
    assert ok
    assert issues == []


@pytest.mark.parametrize("dest_db_type", ["mongo", "mongodb+srv", "mongodb_atlas", "documentdb", "cosmos-mongodb"])
def test_mongo_aliases_are_treated_as_schemaless(dest_db_type: str):
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "notes", "target": "notes", "confidence": 0.97}],
        source_schema={"notes": "VARCHAR"},
        target_schema={},
        table_exists=False,
        dest_connected=True,
        dest_db_type=dest_db_type,
        sample_rows=[{"notes": "x" * 2500}],
    )
    assert ok
    assert issues == []


def test_mongodb_non_id_suffix_fields_do_not_trigger_pk_duplicate_block():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "customer_id", "target": "customer_id", "confidence": 0.9}],
        source_schema={"customer_id": "VARCHAR"},
        target_schema={},
        table_exists=False,
        dest_connected=True,
        dest_db_type="mongodb",
        sample_rows=[{"customer_id": "A1"}, {"customer_id": "A1"}],
    )
    assert ok
    assert issues == []


def test_mongodb_explicit_id_mapping_still_blocks_duplicate_id_values():
    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "id", "target": "_id", "confidence": 0.99}],
        source_schema={"id": "VARCHAR"},
        target_schema={},
        table_exists=False,
        dest_connected=True,
        dest_db_type="mongodb",
        sample_rows=[{"id": "dup"}, {"id": "dup"}],
    )
    assert not ok
    assert any("duplicate" in i.lower() for i in issues)
