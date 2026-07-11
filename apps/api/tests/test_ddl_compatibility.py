"""DDL compatibility (G6) validation tests."""

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
