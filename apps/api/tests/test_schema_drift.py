"""Schema drift detection tests."""

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) in sys.path:
    sys.path.remove(str(_API_ROOT))
sys.path.insert(0, str(_API_ROOT))

from services.schema_drift import classify_schema_change, detect_schema_drift
from services.schema_fingerprint import fingerprint_schema


def test_no_drift_when_schemas_match():
    cols = ["id", "email"]
    schema = {"id": "INTEGER", "email": "VARCHAR"}
    fp = fingerprint_schema(cols, schema)
    mappings = [
        {"source": "id", "target": "user_id", "confidence": 0.95},
        {"source": "email", "target": "email", "confidence": 0.99},
    ]
    report = detect_schema_drift(
        source_columns=cols,
        source_schema=schema,
        target_columns=["user_id", "email"],
        target_schema={"user_id": "INTEGER", "email": "VARCHAR"},
        stored_source_fp=fp,
        stored_target_fp=fingerprint_schema(["user_id", "email"], {"user_id": "INTEGER", "email": "VARCHAR"}),
        mappings=mappings,
    )
    assert report["drift_detected"] is False
    assert report["severity"] == "none"
    assert report["mapping_coverage"] == 1.0


def test_detects_source_schema_change():
    cols = ["id", "email", "created_at"]
    old_fp = fingerprint_schema(["id", "email"], {"id": "INTEGER", "email": "VARCHAR"})
    report = detect_schema_drift(
        source_columns=cols,
        source_schema={"id": "INTEGER", "email": "VARCHAR", "created_at": "TIMESTAMP"},
        target_columns=["user_id", "email"],
        target_schema={"user_id": "INTEGER", "email": "VARCHAR"},
        stored_source_fp=old_fp,
        mappings=[{"source": "id", "target": "user_id", "confidence": 0.9}],
    )
    assert report["source_changed"] is True
    assert report["drift_detected"] is True
    assert report["severity"] == "breaking"
    assert "created_at" in report["unmapped_sources"]


def test_warns_on_unmapped_destination_columns():
    report = detect_schema_drift(
        source_columns=["id"],
        source_schema={"id": "INTEGER"},
        target_columns=["id", "legacy_flag"],
        target_schema={"id": "INTEGER", "legacy_flag": "BOOLEAN"},
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
    )
    assert report["orphan_targets"] == ["legacy_flag"]
    assert report["severity"] == "warning"


def test_ignores_case_only_target_name_differences():
    report = detect_schema_drift(
        source_columns=["id"],
        source_schema={"id": "INTEGER"},
        target_columns=["USER_ID"],
        target_schema={"USER_ID": "INTEGER"},
        mappings=[{"source": "id", "target": "user_id", "confidence": 1.0}],
    )
    assert report["orphan_targets"] == []


def test_mongodb_does_not_raise_type_mismatch_drift_for_document_fields():
    report = detect_schema_drift(
        source_columns=["amount", "status"],
        source_schema={"amount": "DECIMAL", "status": "VARCHAR"},
        target_columns=["amount", "status"],
        target_schema={"amount": "DOUBLE", "status": "STRING"},
        mappings=[
            {"source": "amount", "target": "amount", "confidence": 0.95},
            {"source": "status", "target": "status", "confidence": 0.98},
        ],
        destination_db_type="mongodb",
    )
    assert report["type_mismatches"] == []
    assert report["severity"] == "none"
    assert report["drift_detected"] is False


def test_varchar_to_number_not_breaking_drift_when_samples_coerce():
    report = detect_schema_drift(
        source_columns=["population"],
        source_schema={"population": "VARCHAR"},
        target_columns=["population"],
        target_schema={"population": "NUMBER(38,0)"},
        mappings=[{"source": "population", "target": "population", "confidence": 0.93}],
        destination_db_type="snowflake",
        sample_rows=[{"population": "331002651"}, {"population": "42"}],
    )
    assert report["type_mismatches"] == []
    assert report["severity"] == "none"


def test_mongo_aliases_treated_as_schemaless_in_drift_engine():
    report = detect_schema_drift(
        source_columns=["amount"],
        source_schema={"amount": "DECIMAL"},
        target_columns=["amount"],
        target_schema={"amount": "INT"},
        mappings=[{"source": "amount", "target": "amount", "confidence": 0.9}],
        destination_db_type="mongodb+srv",
    )
    assert report["type_mismatches"] == []
    assert report["severity"] == "none"


def test_redis_target_fingerprint_churn_is_not_breaking():
    """Schemaless dests synthesize target columns from mappings — fingerprint churn ≠ DDL break."""
    cols = ["id", "skills"]
    schema = {"id": "VARCHAR", "skills": "ARRAY"}
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "skills", "target": "skills", "confidence": 0.93},
    ]
    old_target_fp = fingerprint_schema(["id"], {"id": "VARCHAR"})
    report = detect_schema_drift(
        source_columns=cols,
        source_schema=schema,
        target_columns=["id", "skills"],
        target_schema={},
        stored_target_fp=old_target_fp,
        mappings=mappings,
        destination_db_type="redis",
    )
    assert report["target_changed"] is False
    assert report["severity"] == "none"
    assert not any("Destination schema changed" in i for i in report["issues"])


def test_snowflake_target_fingerprint_churn_is_breaking():
    cols = ["id"]
    schema = {"id": "INTEGER"}
    old_fp = fingerprint_schema(["id"], {"id": "VARCHAR"})
    report = detect_schema_drift(
        source_columns=cols,
        source_schema=schema,
        target_columns=["id"],
        target_schema={"id": "INTEGER"},
        stored_target_fp=old_fp,
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
        destination_db_type="snowflake",
    )
    assert report["target_changed"] is True
    assert report["severity"] == "breaking"
    assert any("Destination schema changed" in i for i in report["issues"])


def test_classify_no_change():
    schema = {
        "columns": {"id": "INTEGER", "name": "VARCHAR"},
        "nullable": {"id": False, "name": True},
        "primary_key": ["id"],
    }
    report = classify_schema_change(schema, schema)
    assert report["severity"] == "none"
    assert report["additive"] == []
    assert report["breaking"] == []


def test_classify_additive_nullable_column_and_widen():
    old = {
        "columns": {"id": "INTEGER", "amount": "INTEGER"},
        "nullable": {"id": False, "amount": True},
        "primary_key": ["id"],
    }
    new = {
        "columns": {"id": "INTEGER", "amount": "DECIMAL", "note": "VARCHAR"},
        "nullable": {"id": False, "amount": True, "note": True},
        "primary_key": ["id"],
    }
    report = classify_schema_change(old, new)
    assert report["severity"] == "additive"
    kinds = {c["kind"] for c in report["additive"]}
    assert "add_column" in kinds
    assert "widen_type" in kinds
    assert report["breaking"] == []


def test_classify_breaking_drop_and_narrow():
    old = {
        "columns": {"id": "INTEGER", "amount": "DECIMAL", "legacy": "VARCHAR"},
        "nullable": {"id": False, "amount": True, "legacy": True},
        "primary_key": ["id"],
    }
    new = {
        "columns": {"id": "INTEGER", "amount": "INTEGER"},
        "nullable": {"id": False, "amount": True},
        "primary_key": ["id"],
    }
    report = classify_schema_change(old, new)
    assert report["severity"] == "breaking"
    kinds = {c["kind"] for c in report["breaking"]}
    assert "drop" in kinds
    assert "narrow_type" in kinds


def test_classify_breaking_pk_change():
    old = {
        "columns": {"id": "INTEGER", "sku": "VARCHAR"},
        "nullable": {"id": False, "sku": False},
        "primary_key": ["id"],
    }
    new = {
        "columns": {"id": "INTEGER", "sku": "VARCHAR"},
        "nullable": {"id": False, "sku": False},
        "primary_key": ["sku"],
    }
    report = classify_schema_change(old, new)
    assert report["severity"] == "breaking"
    assert any(c["kind"] == "primary_key_change" for c in report["breaking"])


def test_classify_rename_as_breaking():
    old = {"columns": {"full_name": "VARCHAR"}, "nullable": {"full_name": True}, "primary_key": []}
    new = {"columns": {"name": "VARCHAR"}, "nullable": {"name": True}, "primary_key": []}
    report = classify_schema_change(old, new)
    assert report["severity"] == "breaking"
    assert any(c["kind"] == "rename" for c in report["breaking"])


def test_classify_multi_column_renames_not_as_drops_and_adds():
    old = {
        "columns": {"cust_id": "INTEGER", "full_name": "VARCHAR", "amt": "DECIMAL"},
        "nullable": {"cust_id": False, "full_name": True, "amt": True},
        "primary_key": ["cust_id"],
    }
    new = {
        "columns": {"customer_id": "INTEGER", "name": "VARCHAR", "amount": "DECIMAL"},
        "nullable": {"customer_id": False, "name": True, "amount": True},
        "primary_key": ["customer_id"],
    }
    report = classify_schema_change(old, new)
    kinds = [c["kind"] for c in report["breaking"]]
    assert kinds.count("rename") == 3
    assert "drop" not in kinds
    assert "add_column" not in {c["kind"] for c in report["additive"]}


def test_classify_add_not_null_is_breaking():
    old = {"columns": {"id": "INTEGER"}, "nullable": {"id": False}, "primary_key": ["id"]}
    new = {
        "columns": {"id": "INTEGER", "code": "VARCHAR"},
        "nullable": {"id": False, "code": False},
        "primary_key": ["id"],
    }
    report = classify_schema_change(old, new)
    assert report["severity"] == "breaking"
    assert any(c["kind"] == "add_not_null" for c in report["breaking"])


def test_classify_flat_schema_dicts():
    report = classify_schema_change(
        {"id": "INT", "name": "VARCHAR(50)"},
        {"id": "INT", "name": "VARCHAR(200)"},
    )
    assert report["severity"] == "additive"
    assert any(c["kind"] == "widen_type" for c in report["additive"])
