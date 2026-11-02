"""Type-stamp grammar gate: Map / declared type carriers cannot inject DDL."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from pydantic import ValidationError

from services.type_stamp_grammar import (
    UnsafeTypeStamp,
    is_safe_type_stamp,
    require_safe_type_stamp,
    validate_mapping_type_stamps,
)

INJECTIONS = [
    "VARCHAR(9)); DROP TABLE users; --",
    "NUMBER(1), ADD COLUMN loot VARIANT",
    "VARCHAR(1), DROP COLUMN x",
    "INT DEFAULT 1",
    "INT REFERENCES users(id)",
    "INT CHECK (1=1)",
    "TEXT COLLATE x DEFAULT y",
    "TEXT /* c */",
    "TEXT -- c",
    "INT # c",
    "VARCHAR(10",
    "VARCHAR(10))",
    "TEXT, b INT",
    "'TEXT'",
    "TEXT 'x'",
    "TEXT\x00",
    "INT $$",
    "enum('a\\'')",
    "TEXT; ATTACH DATABASE 'x' AS y",
    "VARCHAR((SELECT 1))",
    "INT) ; SELECT 1 --",
    "X" * 600,
]

LEGIT = [
    "VARCHAR(255)",
    "DECIMAL(10, 2)",
    "NUMBER(*,0)",
    "NUMBER(38,0)",
    "VARCHAR2(36 CHAR)",
    "INTERVAL DAY(2) TO SECOND(6)",
    "TIMESTAMP(6) WITH LOCAL TIME ZONE",
    "timestamp with time zone",
    "DOUBLE PRECISION",
    "character varying(255)",
    "INT[]",
    "int[3][]",
    "geometry(Point,4326)",
    "VECTOR(FLOAT, 768)",
    "ARRAY<STRUCT<a INT64, b STRING>>",
    "struct<a: int>",
    "map<string, int>",
    "DateTime64(6, 'UTC')",
    "Enum8('a' = 1, 'it''s' = -2)",
    "LowCardinality(Nullable(String))",
    "NVARCHAR(MAX)",
    '"public"."order_status"',
    "int(11) unsigned zerofill",
    "varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
    "enum('a','b')",
    "TEXT COLLATE 'en-ci'",
    "TIMESTAMP_NTZ(9)",
    "VARIANT",
]


@pytest.mark.parametrize("stamp", INJECTIONS)
def test_injection_payloads_are_refused(stamp):
    assert not is_safe_type_stamp(stamp)
    with pytest.raises(UnsafeTypeStamp):
        require_safe_type_stamp(stamp)


@pytest.mark.parametrize("stamp", LEGIT)
def test_physical_stamps_are_kept(stamp):
    assert require_safe_type_stamp(stamp) == stamp


@pytest.mark.parametrize("dest", ["snowflake", "sqlite", "postgresql", "mysql", "bigquery"])
@pytest.mark.parametrize("stamp", INJECTIONS[:6])
def test_materialize_dest_ddl_refuses_injection(dest, stamp):
    from services.type_system import materialize_dest_ddl

    with pytest.raises(UnsafeTypeStamp):
        materialize_dest_ddl(dest, stamp)


def test_materialize_dest_ddl_keeps_identity_and_typmod_stamps():
    from services.type_system import materialize_dest_ddl

    assert materialize_dest_ddl("snowflake", "NUMBER(10,2)") == "NUMBER(10,2)"
    assert materialize_dest_ddl("postgresql", "BIGINT GENERATED ALWAYS AS IDENTITY")


def test_mapping_ingest_refuses_injection():
    from src.routers.preflight_router import MappingItem
    from src.routers.transfer_router import ExecuteTransferRequest

    payload = "VARCHAR(9)); DROP TABLE users; --"
    with pytest.raises(ValidationError):
        MappingItem(source="a", target="a", target_type=payload)
    # Source-side types describe the source; they never reach DDL.
    assert MappingItem(source="a", target="a", source_type="timestamp[us, tz=Asia/Kolkata]")
    assert MappingItem(source="a", target="a", target_type="VARCHAR(20)").target_type
    endpoint = {"type": "sqlite", "config": {}}
    with pytest.raises(ValidationError):
        ExecuteTransferRequest(
            source=endpoint,
            destination=endpoint,
            mappings=[{"source": "a", "target": "a", "target_type": payload}],
        )
    validate_mapping_type_stamps(
        [{"source": "id", "target": "id", "target_type": "BIGINT IDENTITY(1,1)"}]
    )


def test_resolve_mapping_dest_types_refuses_injection():
    from connectors.writer_common import resolve_mapping_dest_types

    with pytest.raises(UnsafeTypeStamp):
        resolve_mapping_dest_types(
            ["a"],
            [{"source": "a", "target": "a", "target_type": "NUMBER(1), ADD COLUMN loot VARIANT"}],
            dest_db="snowflake",
        )


class _RecordingCursor:
    def __init__(self):
        self.sql: list[str] = []

    def execute(self, sql, *args, **kwargs):
        self.sql.append(sql)
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return None


def test_snowflake_merge_staging_refuses_injected_type():
    from connectors.snowflake_writer import _merge_batch_via_temp

    cur = _RecordingCursor()
    with pytest.raises(UnsafeTypeStamp):
        _merge_batch_via_temp(
            cur,
            "T",
            ["ID", "X"],
            ["NUMBER(38,0)", "NUMBER(1), ADD COLUMN loot VARIANT"],
            [(1, 2)],
            ["ID"],
            prefer_copy=False,
            conn=None,
        )
    assert not any("CREATE" in s.upper() for s in cur.sql)


def test_sqlite_writer_refuses_injected_target_type(tmp_path: Path):
    from connectors.sqlite_writer import write_mapped_rows

    db = tmp_path / "victim.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE users (id INTEGER)")
    conn.commit()
    conn.close()
    try:
        result = write_mapped_rows(
            host="",
            port=0,
            database=str(db),
            username="",
            password="",
            schema="main",
            connection_string="",
            ssl=False,
            table_name="t",
            headers=["a"],
            data_rows=[["1"]],
            mappings=[
                {
                    "source": "a",
                    "target": "a",
                    "target_type": "VARCHAR(9)); DROP TABLE users; --",
                }
            ],
            column_types={"a": "VARCHAR"},
            create_table=True,
        )
    except UnsafeTypeStamp:
        pass
    else:
        assert result.ok is False
    conn = sqlite3.connect(str(db))
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master")}
    finally:
        conn.close()
    assert "users" in tables
    assert "t" not in tables


def test_render_create_column_defs_refuses_injected_type():
    from services.schema_fidelity import render_create_column_defs

    with pytest.raises(UnsafeTypeStamp):
        render_create_column_defs(
            columns=["a"],
            types=["VARCHAR(9)); DROP TABLE users; --"],
            plan=None,
            dialect="sqlite",
        )


def test_pg_create_sql_refuses_injected_type():
    from services.copy_mysql_pg import _pg_create_sql

    with pytest.raises(UnsafeTypeStamp):
        _pg_create_sql("public", "t", [("a", "a")], ["VARCHAR(9)); DROP TABLE users; --"], [])


def test_sqlite_pg_copy_refuses_injected_dest_ddl_before_connecting(tmp_path: Path):
    from services.copy_sqlite_pg import copy_sqlite_to_postgres

    with pytest.raises(UnsafeTypeStamp):
        copy_sqlite_to_postgres(
            source_cfg={"type": "sqlite", "database": str(tmp_path / "s.db"), "table": "s"},
            source_table="s",
            dest_cfg={"host": "127.0.0.1", "port": 1, "database": "x"},
            dest_schema="public",
            dest_table="t",
            pairs=[("a", "a")],
            pg_ddls=["VARCHAR(9)); DROP TABLE users; --"],
            replace_destination=True,
        )



@pytest.mark.parametrize(
    "stamp",
    [
        "STRUCT<key STRING, value INT64>",
        "ARRAY<STRUCT<comment STRING, from_ts TIMESTAMP, table INT64>>",
        "ROW(key VARCHAR, value INT)",
        "Tuple(key String, default Int32)",
    ],
)
def test_struct_field_names_may_be_sql_keywords(stamp):
    assert is_safe_type_stamp(stamp)


def test_top_level_clause_keywords_still_refused_after_a_group():
    for bad in ("STRUCT<a INT64> DEFAULT 1", "VARCHAR(5) KEY", "INT(1) REFERENCES t(x)"):
        assert not is_safe_type_stamp(bad), bad
