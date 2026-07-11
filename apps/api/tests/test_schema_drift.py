"""Schema drift detection tests."""

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
