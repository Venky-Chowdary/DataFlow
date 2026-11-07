"""ITEM 1 — Postgres CREATE path must invent BIGINT from bare logical integer.

Adversarial audit proved ``generic_sql._sa_type_for_logical`` can be green while
``postgresql_writer.pg_type`` / ``materialize_dest_ddl`` still emit the SQL
keyword ``integer`` (PostgreSQL INT32). Production Execute uses the string DDL
writer, not the SA helper.

This regression fails if CREATE invent narrows bare logical ``integer`` to INT32.
"""

from __future__ import annotations

import uuid

import pytest

from connectors.postgresql_writer import pg_type, write_mapped_rows
from services.decision_kernel import (
    InventContext,
    invent_dest_type,
    materialize_dest_ddl,
    stamp_additive_mapping_types,
)
from services.type_system import DDL_TYPES, LOGICAL_INTEGER, ddl_type, integer_bit_width
from tests.helpers.live_env import pg_creds, pg_up


def test_materialize_and_pg_type_bare_integer_are_bigint():
    """String CREATE authority must match ddl_type / DDL_TYPES (never keyword integer)."""
    expected = ddl_type("postgresql", LOGICAL_INTEGER)
    assert expected == DDL_TYPES["postgresql"][LOGICAL_INTEGER]
    assert expected == "BIGINT"
    assert materialize_dest_ddl("postgresql", "integer") == "BIGINT"
    assert pg_type("integer") == "BIGINT"
    # Ambiguous INTEGER invents 64; unambiguous INT4 stays width-preserving.
    assert materialize_dest_ddl("postgresql", "INTEGER") == "BIGINT"
    assert pg_type("INTEGER") == "BIGINT"
    assert integer_bit_width("INTEGER") is None
    assert materialize_dest_ddl("postgresql", "INT4") == "INTEGER"
    assert pg_type("INT4") == "INTEGER"
    assert integer_bit_width("INT4") == 32


def test_materialize_bare_integer_never_narrower_across_sql_dests():
    for dest in (
        "postgresql",
        "mysql",
        "sqlserver",
        "oracle",
        "snowflake",
        "bigquery",
        "redshift",
        "duckdb",
        "clickhouse",
        "databricks",
        "iceberg",
    ):
        wire = materialize_dest_ddl(dest, "integer")
        table = DDL_TYPES[dest][LOGICAL_INTEGER]
        assert wire == ddl_type(dest, "integer") == table, (dest, wire, table)
        # Must not emit the ambiguous INT32 keyword as invent wire.
        assert wire.strip().upper() not in {"INTEGER", "INT", "INT32", "SIGNED"}, (
            dest,
            wire,
        )


def test_stamp_additive_does_not_collapse_logical_integer_to_int32():
    """create_new + source INTEGER must not treat stamp 'integer' as identity bootstrap."""
    maps = [
        {
            "source": "v",
            "target": "v",
            "target_type": "integer",
            "create_new": True,
        }
    ]
    stamped, _ = stamp_additive_mapping_types(
        maps,
        dest_db="postgresql",
        live_dest_types={},
        source_types={"v": "INTEGER"},
    )
    got = str(stamped[0].get("target_type") or "")
    # Keep bare logical invent OR widen via invent_dest_type — never physical INT32.
    assert got in {
        "integer",
        invent_dest_type("integer", dest_db="postgresql", context=InventContext.CREATE_NEW),
        "BIGINT",
    }
    assert materialize_dest_ddl("postgresql", got) == "BIGINT"
    # Physical INT32 SQL carriers only (case-sensitive — logical ``integer`` is OK).
    assert got not in {"INTEGER", "INT", "INT32", "SIGNED"}


@pytest.mark.skipif(not pg_up("ITEM1"), reason="PostgreSQL not reachable for live CREATE")
def test_live_pg_writer_create_bare_integer_is_int8_and_holds_int64():
    """information_schema + bit-exact insert via postgresql_writer (production path)."""
    import psycopg2

    creds = pg_creds("ITEM1")
    host = creds["host"]
    port = creds["port"]
    database = creds["database"]
    username = creds["username"]
    password = creds["password"]
    table = f"item1_pg_{uuid.uuid4().hex[:10]}"

    result = write_mapped_rows(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        schema="public",
        connection_string="",
        ssl=False,
        table_name=table,
        headers=["id", "big_val"],
        data_rows=[
            ["1", "9223372036854775807"],
            ["2", "-9223372036854775808"],
            ["3", "2147483648"],
            ["4", "0"],
        ],
        mappings=[
            {
                "source": "id",
                "target": "id",
                "target_type": "integer",
                "approved": True,
            },
            {
                "source": "big_val",
                "target": "big_val",
                "target_type": "integer",
                "approved": True,
            },
        ],
        column_types={"id": "INTEGER", "big_val": "INTEGER"},
        create_table=True,
        type="postgresql",
        error_policy="fail",
    )
    assert result.ok, result.error
    assert result.rows_written == 4

    conn = psycopg2.connect(
        host=host, port=port, dbname=database, user=username, password=password
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, udt_name, numeric_precision
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
                ORDER BY ordinal_position
                """,
                (table,),
            )
            cols = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
            assert cols["id"][0] == "int8", cols
            assert cols["big_val"][0] == "int8", cols
            assert cols["id"][1] == 64, cols
            assert cols["big_val"][1] == 64, cols
            cur.execute(
                f'SELECT id, big_val FROM public."{table}" ORDER BY id'
            )
            rows = cur.fetchall()
            assert rows == [
                (1, 9223372036854775807),
                (2, -9223372036854775808),
                (3, 2147483648),
                (4, 0),
            ]
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.commit()
    finally:
        conn.close()
