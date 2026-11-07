"""ITEM 1 — SQLite INTEGER affinity must invent BIGINT on INT32 destinations.

SQLite ``INTEGER`` is a signed int64 storage class. Introspect used to emit
``INTEGER``, and create-new invent stamped PostgreSQL ``INTEGER`` (INT32).
Values like ``2147483648`` then fail or quarantine on auto-map transfers.

Regression: introspect + invent + materialize/pg_type must yield a 64-bit
wire; live PG CREATE must be ``int8`` and hold int64 values bit-exactly.
"""

from __future__ import annotations

import sqlite3
import tempfile
import uuid
from pathlib import Path

import pytest

from connectors.postgresql_writer import pg_type, write_mapped_rows
from services.decision_kernel import (
    InventContext,
    invent_dest_type,
    materialize_dest_ddl,
)
from services.schema_introspect import _introspect_sqlite
from services.type_system import integer_bit_width
from tests.helpers.live_env import pg_creds, pg_up


def test_sqlite_integer_affinity_introspects_as_bigint():
    path = Path(tempfile.gettempdir()) / f"item1_sqlite_{uuid.uuid4().hex[:8]}.db"
    try:
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, f REAL)")
        conn.execute(
            "INSERT INTO t VALUES (1, ?, ?)",
            (2147483648, 1.2345678901234567),
        )
        conn.commit()
        conn.close()
        result = _introspect_sqlite(database=str(path), table="t")
        assert result.get("ok"), result
        by = {c["name"]: c["inferred_type"] for c in result["columns"]}
        assert by["v"] == "BIGINT", by
        assert by["id"] == "BIGINT", by
        assert by["f"] in {"DOUBLE PRECISION", "DOUBLE", "FLOAT64"}, by
    finally:
        path.unlink(missing_ok=True)


def test_sqlite_integer_carrier_invents_pg_bigint_wire():
    stamp = invent_dest_type(
        "BIGINT", dest_db="postgresql", context=InventContext.CREATE_NEW
    )
    # After introspect fix the carrier is BIGINT; also prove INTEGER would still
    # be wrong if it leaked through — invent from BIGINT must be 64-bit.
    assert materialize_dest_ddl("postgresql", stamp) == "BIGINT"
    assert pg_type(stamp) == "BIGINT"
    assert integer_bit_width(stamp) == 64 or stamp.upper() in {"BIGINT", "INT64"}


def test_invent_from_sqlite_introspect_carrier_is_never_int32():
    """End-to-end carrier chain used by UTE auto-map column_types."""
    path = Path(tempfile.gettempdir()) / f"item1_sqlite2_{uuid.uuid4().hex[:8]}.db"
    try:
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t (v INTEGER)")
        conn.execute("INSERT INTO t VALUES (?)", (9223372036854775807,))
        conn.commit()
        conn.close()
        result = _introspect_sqlite(database=str(path), table="t")
        carrier = result["columns"][0]["inferred_type"]
        stamp = invent_dest_type(
            carrier, dest_db="postgresql", context=InventContext.CREATE_NEW
        )
        wire = materialize_dest_ddl("postgresql", stamp)
        assert wire.upper() not in {"INTEGER", "INT", "INT32"}, (carrier, stamp, wire)
        assert pg_type(wire) == "BIGINT" or wire == "BIGINT"
    finally:
        path.unlink(missing_ok=True)


@pytest.mark.skipif(not pg_up("ITEM1"), reason="PostgreSQL not reachable")
def test_live_pg_create_from_sqlite_affinity_carrier_holds_int64():
    path = Path(tempfile.gettempdir()) / f"item1_sqlite3_{uuid.uuid4().hex[:8]}.db"
    table = f"item1_sqlite_pg_{uuid.uuid4().hex[:10]}"
    try:
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        conn.execute(
            "INSERT INTO t VALUES (1, ?), (2, ?), (3, ?)",
            (9223372036854775807, -9223372036854775808, 2147483648),
        )
        conn.commit()
        conn.close()
        schema = _introspect_sqlite(database=str(path), table="t")
        types = {c["name"]: c["inferred_type"] for c in schema["columns"]}
        stamp_id = invent_dest_type(
            types["id"], dest_db="postgresql", context=InventContext.CREATE_NEW
        )
        stamp_v = invent_dest_type(
            types["v"], dest_db="postgresql", context=InventContext.CREATE_NEW
        )
        creds = pg_creds("ITEM1")
        host = creds["host"]
        port = creds["port"]
        database = creds["database"]
        username = creds["username"]
        password = creds["password"]
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
            headers=["id", "v"],
            data_rows=[
                ["1", "9223372036854775807"],
                ["2", "-9223372036854775808"],
                ["3", "2147483648"],
            ],
            mappings=[
                {
                    "source": "id",
                    "target": "id",
                    "target_type": stamp_id,
                    "approved": True,
                },
                {
                    "source": "v",
                    "target": "v",
                    "target_type": stamp_v,
                    "approved": True,
                },
            ],
            column_types=types,
            create_table=True,
            type="postgresql",
            error_policy="fail",
        )
        assert result.ok, result.error
        assert result.rows_written == 3
        import psycopg2

        pg = psycopg2.connect(
            host=host, port=port, dbname=database, user=username, password=password
        )
        try:
            with pg.cursor() as cur:
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
                assert cols["v"][0] == "int8", cols
                assert cols["v"][1] == 64, cols
                cur.execute(f'SELECT id, v FROM public."{table}" ORDER BY id')
                assert cur.fetchall() == [
                    (1, 9223372036854775807),
                    (2, -9223372036854775808),
                    (3, 2147483648),
                ]
                cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            pg.commit()
        finally:
            pg.close()
    finally:
        path.unlink(missing_ok=True)


def test_materialize_bare_logical_float_is_double_on_mysql():
    """Bare logical float must not pass through as MySQL FLOAT32 invent wire."""
    from services.type_system import DDL_TYPES, LOGICAL_FLOAT, ddl_type

    assert materialize_dest_ddl("mysql", "float") == ddl_type("mysql", LOGICAL_FLOAT)
    assert materialize_dest_ddl("mysql", "float") == DDL_TYPES["mysql"][LOGICAL_FLOAT]
    assert materialize_dest_ddl("mysql", "float").upper() == "DOUBLE"
    # The single-precision carrier is spelled FLOAT32 (MySQL introspect emits
    # it): a bare FLOAT stamp is ambiguous across engines — IEEE-64 on
    # PostgreSQL/SQL Server/Snowflake — so it must not narrow to MySQL FLOAT.
    assert materialize_dest_ddl("mysql", "FLOAT32") == "FLOAT"
    assert materialize_dest_ddl("mysql", "FLOAT").upper() == "DOUBLE"
