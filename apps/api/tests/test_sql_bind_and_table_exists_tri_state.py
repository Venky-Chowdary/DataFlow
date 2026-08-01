"""Shared SQL bind + table_exists tri-state honesty proofs."""

from __future__ import annotations


def test_sql_bind_bool_json_parity_mysql_postgres():
    from connectors.generic_sql import _to_sa_value
    from connectors.mysql_writer import _to_mysql_value
    from connectors.sql_bind import normalize_sql_bind_value

    assert _to_mysql_value("false", "BOOLEAN") == 0
    assert _to_mysql_value("true", "TINYINT") == 1
    assert normalize_sql_bind_value("false", "BOOLEAN", engine="postgresql") is False
    assert normalize_sql_bind_value("true", "BOOL", engine="postgresql") is True
    assert _to_sa_value("false", "boolean") is False
    assert _to_sa_value("true", "boolean") is True

    assert _to_mysql_value("", "JSON") is None
    assert normalize_sql_bind_value("", "JSONB", engine="postgresql") is None
    assert _to_sa_value("", "json") is None
    # JSONB binds as text: psycopg2 cannot adapt a native dict (wave 88).
    assert normalize_sql_bind_value('{"a":1}', "JSONB", engine="postgresql") == '{"a":1}'
    assert _to_mysql_value({"a": 1}, "JSON") == '{"a":1}'


def test_ddl_unknown_table_exists_does_not_claim_create_new():
    from services.ddl_compatibility import evaluate_ddl_compatibility

    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "id", "target": "id"}],
        source_schema={"id": "INTEGER"},
        target_schema={},
        table_exists=None,
        dest_connected=True,
        dest_db_type="postgresql",
        allow_create=True,
        sync_mode="full_refresh_append",
        destination_table="users",
    )
    assert any("unknown" in i.lower() for i in issues), issues
    # Must not only talk about proposed CREATE DDL as if table is missing
    assert not any("Proposed DDL" in i for i in issues)


def test_ddl_create_new_still_works_when_explicit_false():
    from services.ddl_compatibility import evaluate_ddl_compatibility

    ok, issues = evaluate_ddl_compatibility(
        mappings=[{"source": "id", "target": "id"}],
        source_schema={"id": "INTEGER"},
        target_schema={},
        table_exists=False,
        dest_connected=True,
        dest_db_type="postgresql",
        allow_create=True,
        sync_mode="full_refresh_append",
        destination_table="brand_new",
    )
    assert not any("unknown" in i.lower() for i in issues), issues


def test_analyze_coercion_postgres_json_and_bool_wire():
    from services.coercion_probe import analyze_coercion

    report = analyze_coercion(
        sample_rows=[{"flag": "false", "payload": '{"ok":true}'}],
        mappings=[
            {
                "source": "flag",
                "target": "flag",
                "target_type": "BOOLEAN",
                "transform": "none",
            },
            {
                "source": "payload",
                "target": "payload",
                "target_type": "JSONB",
                "transform": "json",
            },
        ],
        source_types={"flag": "BOOLEAN", "payload": "JSON"},
        dest_types={"flag": "BOOLEAN", "payload": "JSONB"},
        dest_db_type="postgresql",
    )
    assert report["sampled_rows"] == 1
    assert report["has_blocking_failures"] is False
