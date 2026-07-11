"""Schema drift detection tests."""

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) in sys.path:
    sys.path.remove(str(_API_ROOT))
sys.path.insert(0, str(_API_ROOT))

from services.schema_drift import detect_schema_drift
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
