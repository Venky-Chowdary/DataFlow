"""QA MX2-07 — query-mode SQL Server source typed every column INTEGER.

pymssql reports PEP 249 type objects as small ints: STRING=1, BINARY=2,
NUMBER=3, DATETIME=4, DECIMAL=5. The cursor carrier tables are MySQL
FIELD_TYPE / PostgreSQL OIDs, so read dialect-blind those codes became
TINYINT/SMALLINT/INT/FLOAT/DOUBLE: VARCHAR ``sku``, FLOAT ``weight_kg`` and
DATETIMEOFFSET all arrived as INTEGER and preflight blocked
"sku INTEGER → INTEGER ... INVALID_NUMERIC".

The T16 fix that should have arbitrated (driver Python types) never ran in
production: ``_execute_live`` stringified the cells before
``peek_callable_schema`` looked at them, so every carrier read ``str``.
"""

from __future__ import annotations

import datetime as dt
import os
import socket
import sqlite3
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.decimal_observe import cursor_declared_carriers  # noqa: E402
from services.procedure_source import (  # noqa: E402
    _cell,
    _overlay_declared_numerics,
    peek_callable_schema,
)

# What pymssql 2.x returns for ``SELECT * FROM dbo.products`` (captured live).
_PYMSSQL_DESCRIPTION = (
    ("id", 3, None, None, None, None, None),
    ("sku", 1, None, None, None, None, None),
    ("weight_kg", 3, None, None, None, None, None),
    ("price", 5, None, None, None, None, None),
    ("created_at", 2, None, None, None, None, None),
    ("active", 3, None, None, None, None, None),
    ("made", 2, None, None, None, None, None),
    ("legacy_dt", 4, None, None, None, None, None),
)
_HEADERS = [c[0] for c in _PYMSSQL_DESCRIPTION]
_RAW_ROWS = [
    (
        1,
        "SKU-1",
        1.25,
        Decimal("9.99"),
        dt.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30))),
        True,
        dt.datetime(2024, 1, 1, 10, 0),
        dt.datetime(2024, 1, 1, 10, 0, 0, 3000),
    ),
    (
        2,
        "SKU-2",
        0.5,
        Decimal("19.50"),
        dt.datetime(2024, 2, 2, 3, 4, 5, tzinfo=dt.timezone(dt.timedelta(hours=-8))),
        False,
        dt.datetime(2024, 1, 2, 10, 0),
        dt.datetime(2024, 1, 2, 10, 0),
    ),
]


@pytest.mark.parametrize("dialect", ["sqlserver", "mssql", "sql_server", "azure_sql_database"])
def test_pymssql_type_codes_are_not_read_as_mysql_field_types(dialect):
    declared = cursor_declared_carriers(_HEADERS, _PYMSSQL_DESCRIPTION, dialect=dialect)
    assert "INTEGER" not in declared.values(), declared
    assert declared.get("sku") == "VARCHAR"
    assert declared.get("price") == "DECIMAL"
    assert declared.get("legacy_dt") == "DATETIME"
    # NUMBER (int/float/bit) and BINARY (datetimeoffset/datetime2/varbinary)
    # name no carrier: the driver's Python type decides.
    for ambiguous in ("id", "weight_kg", "created_at", "active", "made"):
        assert ambiguous not in declared, (ambiguous, declared)


def test_mysql_and_pg_codes_still_resolve():
    mysql = (("i", 3, None, None, 11, 0, True), ("f", 5, None, None, 22, 31, True))
    assert cursor_declared_carriers(["i", "f"], mysql, dialect="mysql") == {
        "i": "INTEGER",
        "f": "DOUBLE",
    }
    pg = (("t", 25, None, None, None, None, None), ("ts", 1184, None, None, None, None, None))
    assert cursor_declared_carriers(["t", "ts"], pg, dialect="postgresql") == {
        "t": "VARCHAR",
        "ts": "TIMESTAMPTZ",
    }


def test_untrusted_int_codes_never_invent_integer():
    # Snowflake also reports ints (2 = TEXT). No table is known: sample decides.
    desc = (("name", 2, None, None, None, None, None),)
    assert cursor_declared_carriers(["name"], desc, dialect="snowflake") == {}


def test_carriers_survive_wire_stringification():
    from services.procedure_source import python_carriers_of_rows

    wire = [[_cell(v) for v in row] for row in _RAW_ROWS]
    carriers = python_carriers_of_rows(_HEADERS, _RAW_ROWS)
    schema, _ = peek_callable_schema(_HEADERS, wire, carriers)
    merged = _overlay_declared_numerics(_HEADERS, _PYMSSQL_DESCRIPTION, schema, dialect="sqlserver")
    assert merged["id"] == "INTEGER"
    assert merged["sku"] == "VARCHAR"
    assert merged["weight_kg"] == "DOUBLE"
    assert merged["price"] == "DECIMAL"
    assert merged["created_at"] == "TIMESTAMPTZ"
    assert merged["active"] == "BOOLEAN"
    assert merged["made"] == "TIMESTAMP"


def test_mixed_python_types_give_no_carrier():
    from services.procedure_source import python_carriers_of_rows

    assert python_carriers_of_rows(["v"], [(1,), ("a",)]) == {}


# ---------------------------------------------------------------- live
_MSSQL = {
    "host": os.environ.get("DATAFLOW_TEST_MSSQL_HOST", "localhost"),
    "port": int(os.environ.get("DATAFLOW_TEST_MSSQL_PORT", "1433")),
    "username": os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa"),
    "password": os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", "DataFlow!Pass123"),
}


def _mssql_conn(database: str | None = None):
    import pymssql

    kwargs = dict(
        server=_MSSQL["host"],
        port=_MSSQL["port"],
        user=_MSSQL["username"],
        password=_MSSQL["password"],
        autocommit=True,
        login_timeout=3,
    )
    if database:
        kwargs["database"] = database
    return pymssql.connect(**kwargs)


def _mssql_ready() -> bool:
    try:
        with socket.create_connection((_MSSQL["host"], _MSSQL["port"]), timeout=1):
            pass
        _mssql_conn().close()
        return True
    except Exception:
        return False


live = pytest.mark.skipif(not _mssql_ready(), reason="SQL Server not reachable (pymssql)")


@pytest.fixture()
def products_db():
    conn = _mssql_conn()
    cur = conn.cursor()
    cur.execute("IF DB_ID('dataflow_mx207') IS NULL CREATE DATABASE dataflow_mx207")
    cur.execute(
        "USE dataflow_mx207; IF OBJECT_ID('dbo.products') IS NOT NULL DROP TABLE dbo.products; "
        "CREATE TABLE dbo.products (id INT PRIMARY KEY, sku VARCHAR(20), name NVARCHAR(100), "
        "category VARCHAR(30), weight_kg FLOAT, price DECIMAL(10,2), description NVARCHAR(MAX), "
        "created_at DATETIMEOFFSET(6), active BIT)"
    )
    cur.execute(
        "USE dataflow_mx207; INSERT INTO dbo.products VALUES "
        "(1,'SKU-1',N'Widget','tools',1.25,9.99,N'first','2024-01-02 03:04:05.123456 +05:30',1),"
        "(2,'SKU-2',N'Gadget','toys',0.5,19.50,N'second','2024-02-02 03:04:05 -08:00',0),"
        "(3,'00042',N'Thing','misc',2.0,0.01,N'third','2024-03-03 00:00:00 +00:00',1)"
    )
    conn.close()
    yield {**_MSSQL, "type": "sqlserver", "database": "dataflow_mx207", "connection_string": "", "ssl": False}


@live
def test_live_sqlserver_query_peek_types(products_db):
    from services.procedure_source import read_callable_batch

    cfg = {**products_db, "source_read_mode": "query", "source_query": "SELECT * FROM dbo.products"}
    batch = read_callable_batch(cfg, offset=0, limit=10, peek=True)
    types = batch.meta["native_types"]
    assert types == {
        "id": "INTEGER",
        "sku": "VARCHAR",
        "name": "VARCHAR",
        "category": "VARCHAR",
        "weight_kg": "DOUBLE",
        "price": "DECIMAL",
        "description": "VARCHAR",
        "created_at": "TIMESTAMPTZ",
        "active": "BOOLEAN",
    }, types


@live
def test_live_sqlserver_query_to_sqlite_runs_with_preflight(products_db, tmp_path: Path, monkeypatch):
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Fake(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            return True

    fake = _Fake()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    dst = tmp_path / "dst.sqlite"
    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlserver",
            host=products_db["host"],
            port=products_db["port"],
            database=products_db["database"],
            username=products_db["username"],
            password=products_db["password"],
            # The studio/Pilot names the stream from the SELECT before Execute.
            table="products_q",
            extra={"source_read_mode": "query", "source_query": "SELECT * FROM dbo.products"},
        ),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="ms_q"),
        sync_mode="full_refresh_overwrite",
    )
    job_id = "mx207" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    conn = sqlite3.connect(str(dst))
    try:
        rows = conn.execute(
            "SELECT sku, typeof(sku), weight_kg, typeof(weight_kg), name FROM ms_q ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    assert [r[0] for r in rows] == ["SKU-1", "SKU-2", "00042"]
    assert {r[1] for r in rows} == {"text"}
    assert [r[2] for r in rows] == [1.25, 0.5, 2.0]
    assert {r[3] for r in rows} == {"real"}
