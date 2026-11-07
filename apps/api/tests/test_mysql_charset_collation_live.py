"""Live MySQL proof for the charset / collation DDL contract (D4, D5).

Generated DDL is only true if the engine accepts it and the column it creates
holds what the plan promised. Three claims are measured on a real server:

1. A representative SQL Server catalog and a representative Oracle catalog
   both render a ``CREATE TABLE`` MySQL executes — charset clauses on the
   character columns only, in front of ``NOT NULL``, one clause per column.
2. A SQL Server ``NVARCHAR`` source lands in a carrier that really stores a
   supplementary scalar: inserted, re-read on a second connection, byte-exact,
   with ``information_schema`` confirming the character set and the ``_bin``
   collation's equality polarity.
3. A MySQL-source ``NVARCHAR`` keeps its own utf8mb3 alias, and the engine
   refuses the astral scalar (1366). That refusal is the reason the capacity
   model grades the alias BMP-only instead of reading the name as national.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.decision_kernel.invent import invent_dest_type
from services.encoding_capacity import plan_encoding_carry
from services.schema_fidelity import (
    SourceSchemaCatalog,
    plan_create_new_fidelity,
    render_create_column_defs,
)
from services.source_engine_scope import bind_source_engine

ASTRAL = "\U0001f600ok"


def _mysql_up() -> bool:
    try:
        with socket.create_connection((_creds()["host"], _creds()["port"]), timeout=0.4):
            return True
    except OSError:
        return False


def _creds() -> dict:
    return {
        "host": os.environ.get("P2_MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("P2_MYSQL_PORT", "3306")),
        "database": os.environ.get("P2_MYSQL_DB", "dataflow"),
        "user": os.environ.get("P2_MYSQL_USER", "dataflow"),
        "password": os.environ.get("P2_MYSQL_PASSWORD", "dataflow"),
    }


def _connect():
    import pymysql

    c = _creds()
    return pymysql.connect(
        host=c["host"],
        port=c["port"],
        user=c["user"],
        password=c["password"],
        database=c["database"],
        charset="utf8mb4",
        autocommit=True,
    )


def _render(
    *,
    source_dialect: str,
    columns: dict[str, str],
    table: str,
    collations: dict[str, str] | None = None,
    primary_key: list[str] | None = None,
    unique_keys: list[list[str]] | None = None,
) -> tuple[str, list[str], object, SourceSchemaCatalog]:
    """CREATE TABLE text, invented types, plan and catalog for one route."""
    pk = primary_key if primary_key is not None else ["id"]
    with bind_source_engine(source_dialect):
        types = [
            str(invent_dest_type(t, dest_db="mysql", context="create_new"))
            for t in columns.values()
        ]
    catalog = SourceSchemaCatalog(
        dialect=source_dialect,
        columns=list(columns),
        column_types=dict(columns),
        nullable={c: c not in pk for c in columns},
        primary_key=pk,
        unique_keys=unique_keys or [],
        collations=collations or {},
        charsets={},
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="mysql",
        target_columns=list(columns),
        target_types=types,
        source_to_target={c: c for c in columns},
    )
    body = render_create_column_defs(
        columns=plan.dest_columns, types=types, plan=plan, dialect="mysql"
    )
    return f"CREATE TABLE `{table}` ({body})", types, plan, catalog


_SQLSERVER_COLUMNS = {
    "id": "INT",
    "code": "NVARCHAR(32)",
    "name": "NVARCHAR(64)",
    "descr": "NVARCHAR(MAX)",
    "amount": "DECIMAL(12,2)",
    "created": "DATETIME2(6)",
    "flag": "BIT",
    "payload": "VARBINARY(MAX)",
    "guid": "UNIQUEIDENTIFIER",
    "xmlcol": "XML",
}

_ORACLE_COLUMNS = {
    "id": "NUMBER(10,0)",
    "code": "VARCHAR2(32)",
    "name": "VARCHAR2(64)",
    "descr": "CLOB",
    "ncdescr": "NCLOB",
    "amount": "NUMBER(12,2)",
    "created": "TIMESTAMP(6)",
    "payload": "BLOB",
    "rawcol": "RAW(16)",
}


@pytest.mark.skipif(not _mysql_up(), reason="MySQL not listening")
@pytest.mark.parametrize(
    "source_dialect,columns",
    [("sqlserver", _SQLSERVER_COLUMNS), ("oracle", _ORACLE_COLUMNS)],
)
def test_generated_mysql_ddl_is_accepted_by_the_engine(source_dialect, columns) -> None:
    """The engine is the judge of syntax — a charset clause in the wrong place is 1064."""
    table = f"d4_{source_dialect}_{uuid.uuid4().hex[:8]}"
    stmt, types, _plan, _catalog = _render(
        source_dialect=source_dialect,
        columns=columns,
        table=table,
        collations={"code": "Latin1_General_BIN"} if source_dialect == "sqlserver" else {},
        unique_keys=[["code"]],
    )
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(stmt)
            cur.execute(
                "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_SET_NAME "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (_creds()["database"], table),
            )
            charsets = {str(r[0]): (r[1], r[2]) for r in cur.fetchall()}
            # A charset only exists on the character columns; the numeric,
            # temporal and binary carriers must have none.
            for col in ("amount", "created", "payload"):
                if col in columns:
                    assert charsets[col][1] is None, (col, charsets[col])
            for col in ("code", "name", "descr"):
                assert charsets[col][1] is not None, (col, charsets[col])
            cur.execute(f"DROP TABLE `{table}`")
    finally:
        conn.close()
    assert len(types) == len(columns)


@pytest.mark.skipif(not _mysql_up(), reason="MySQL not listening")
def test_national_source_column_stores_a_supplementary_scalar() -> None:
    """utf8mb3 would refuse the scalar with 1366; the plan states utf8mb4."""
    table = f"d4_astral_{uuid.uuid4().hex[:8]}"
    columns = {"id": "INT", "code": "NVARCHAR(32)", "descr": "NVARCHAR(MAX)"}
    stmt, types, _plan, catalog = _render(
        source_dialect="sqlserver",
        columns=columns,
        table=table,
        collations={"code": "Latin1_General_BIN"},
        unique_keys=[["code"]],
    )
    encoding = plan_encoding_carry(
        catalog=catalog,
        dest_dialect="mysql",
        dest_name_for_source=lambda c: c,
        dest_type_for_column=lambda c: types[list(columns).index(c)],
    )
    decisions = encoding if isinstance(encoding, list) else encoding.decisions
    by_col = {d.dest_column: d for d in decisions}
    assert by_col["code"].status == "carried", by_col["code"].reason
    assert by_col["descr"].status == "carried", by_col["descr"].reason

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(stmt)
            cur.execute(
                f"INSERT INTO `{table}` (id, code, descr) VALUES (%s, %s, %s)",
                (1, ASTRAL, ASTRAL),
            )
        # Independent connection: the destination answers for itself.
        verify = _connect()
        try:
            with verify.cursor() as cur:
                cur.execute(f"SELECT code, descr FROM `{table}` WHERE id = 1")
                assert cur.fetchone() == (ASTRAL, ASTRAL)
                cur.execute(
                    "SELECT COLUMN_NAME, CHARACTER_SET_NAME, COLLATION_NAME "
                    "FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                    "AND COLUMN_NAME IN ('code','descr')",
                    (_creds()["database"], table),
                )
                meta = {str(r[0]): (str(r[1]), str(r[2])) for r in cur.fetchall()}
                assert meta["code"][0] == "utf8mb4", meta
                assert meta["descr"][0] == "utf8mb4", meta
                # The source's BIN collation carried: equality stays exact.
                assert meta["code"][1].endswith("_bin"), meta
                cur.execute(
                    f"SELECT COUNT(*) FROM `{table}` WHERE code = %s", (ASTRAL,)
                )
                assert cur.fetchone()[0] == 1
                cur.execute(
                    f"SELECT COUNT(*) FROM `{table}` WHERE code = %s",
                    (ASTRAL.upper(),),
                )
                assert cur.fetchone()[0] == 0
        finally:
            verify.close()
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE `{table}`")
    finally:
        conn.close()


@pytest.mark.skipif(not _mysql_up(), reason="MySQL not listening")
def test_mysql_source_national_alias_really_is_bmp_only() -> None:
    """The measured refusal behind the capacity model, not a claim about a name."""
    table = f"d4_mb3_{uuid.uuid4().hex[:8]}"
    stmt, types, _plan, _catalog = _render(
        source_dialect="mysql",
        columns={"id": "INT", "code": "NVARCHAR(32)"},
        table=table,
    )
    assert any("NVARCHAR" in t.upper() for t in types), types
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(stmt)
            cur.execute(
                "SELECT CHARACTER_SET_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                "AND COLUMN_NAME = 'code'",
                (_creds()["database"], table),
            )
            assert str(cur.fetchone()[0]) == "utf8mb3"
            with pytest.raises(Exception) as excinfo:
                cur.execute(
                    f"INSERT INTO `{table}` (id, code) VALUES (%s, %s)",
                    (1, ASTRAL),
                )
            assert "1366" in str(excinfo.value) or "Incorrect string" in str(
                excinfo.value
            )
            cur.execute(f"DROP TABLE `{table}`")
    finally:
        conn.close()
