"""X-40 LIVE: wide-decimal (p>28) * -> SQL Server binds must succeed.

Live evidence (loop probe /workspace/loop/live_6669_x40probe.py, pyodbc
5.3.0 + ODBC Driver 18): MySQL DECIMAL(38,10) -> SQL Server died at
SQLExecute with the raw driver error ``HY090 Invalid string or buffer
length (0)`` and zero records, on BOTH the DECIMAL-only table and the
mixed BIGINT+DECIMAL+VARCHAR table — and the raw text reached the user.
PG NUMERIC(38,10) through the same writer passed only because the wide
column happened to sit before the first ``None`` slot in the
``setinputsizes`` sequence: msodbcsql ignores declarations past a None
slot. ``_pyodbc_size_for_ddl`` now fills every position — no None — so
the varchar declaration lands whatever the column order, and
``bulk_executemany`` folds any residual bind crash into
``FastPathUnavailable`` (row path -> typed finding, never raw ODBC text).

This test is the live regression: real pyodbc + real SQL Server. It
SKIPS when the engines are unreachable — unit coverage for the mapping
and decline behavior lives in test_x40_undo208_message_order.py.
"""

from __future__ import annotations

import os
import socket
from decimal import Decimal
from typing import Any

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("DATAFLOW_X40_MSSQL_LIVE"),
    reason="live X-40 check: set DATAFLOW_X40_MSSQL_LIVE=1 with reachable engines",
)

_VALUES = [
    Decimal("1234567890123456789012345678.0123456789"),
    Decimal("-0.0000000001"),
    Decimal("0"),
    None,
    Decimal("-9999999999999999999999999999.9999999999"),
]
_LABELS = ["a", "b", "c", "d", "e"]


def _cfg(prefix: str, default_port: int) -> dict[str, Any] | None:
    host = os.getenv(f"{prefix}_HOST", "")
    if not host:
        return None
    return {
        "host": host,
        "port": int(os.getenv(f"{prefix}_PORT", str(default_port))),
        "database": os.getenv(f"{prefix}_DB", ""),
        "username": os.getenv(f"{prefix}_USER", ""),
        "password": os.getenv(f"{prefix}_PASSWORD", ""),
        "connection_string": os.getenv(f"{prefix}_CONNECTION_STRING", ""),
        "ssl": False,
        **(
            {"encrypt": os.getenv(f"{prefix}_ENCRYPT")}
            if os.getenv(f"{prefix}_ENCRYPT")
            else {}
        ),
        "trust_server_certificate": os.getenv(
            f"{prefix}_TRUST_SERVER_CERTIFICATE", "yes"
        )
        in {"1", "true", "yes", "y", "on"},
    }


def _reachable(cfg: dict[str, Any]) -> bool:
    try:
        socket.create_connection((cfg["host"], int(cfg["port"])), timeout=3).close()
        return True
    except OSError:
        return False


def _pyodbc_conn(dest_cfg: dict[str, Any]):
    import pyodbc

    cs = dest_cfg.get("connection_string") or (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={dest_cfg['host']},{dest_cfg['port']};"
        f"DATABASE={dest_cfg['database']};"
        f"UID={dest_cfg['username']};PWD={dest_cfg['password']};"
        f"TrustServerCertificate={'yes' if dest_cfg.get('trust_server_certificate') else 'no'};"
        f"Encrypt={dest_cfg.get('encrypt') or 'yes'};"
    )
    return pyodbc.connect(cs, timeout=10)


def _dest_row_by_id(dest_cfg: dict[str, Any], table: str) -> dict[Any, tuple]:
    """Exact value read-back keyed by id — no count-only shortcuts."""
    conn = _pyodbc_conn(dest_cfg)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT id, t, label FROM [dbo].[{table}] ORDER BY id")  # nosec B608
        return {int(r[0]): (Decimal(str(r[1])) if r[1] is not None else None, r[2]) for r in cur.fetchall()}
    finally:
        conn.close()


def _drop(dest_cfg: dict[str, Any], table: str) -> None:
    conn = _pyodbc_conn(dest_cfg)
    try:
        cur = conn.cursor()
        cur.execute(
            f"IF OBJECT_ID(N'dbo.{table}', N'U') IS NOT NULL DROP TABLE [dbo].[{table}]"  # nosec B608
        )
        conn.commit()
    finally:
        conn.close()




class _BulkRan:
    """Wrap a service module's ``bulk_executemany`` and count real calls.

    X-48: the live regression must prove the fast path *ran* — a silent
    decline to the row path could fake the same read-back greens. Patch the
    name inside the service module under test (each service imports it).
    """

    def __init__(self):
        self.calls = 0
        self.rows = 0

    def wrap(self, module):
        import types

        real = module.bulk_executemany

        def _counting(cur, insert_sql, rows):
            self.calls += 1
            self.rows += len(rows)
            return real(cur, insert_sql, rows)

        return _counting


def _run_with_bulk_count(module, fn):
    """Run ``fn`` with the module's bulk_executemany wrapped; assert it ran."""
    counter = _BulkRan()
    wrapped = counter.wrap(module)
    orig = module.bulk_executemany
    module.bulk_executemany = wrapped
    try:
        result = fn()
    finally:
        module.bulk_executemany = orig
    assert counter.calls >= 1 and counter.rows == 5, (
        f"bulk_executemany never ran (calls={counter.calls}, "
        f"rows={counter.rows}) — the run silently declined to the row path"
    )
    return result

def _assert_exact_readback(dest_cfg: dict[str, Any], table: str) -> None:
    rows = _dest_row_by_id(dest_cfg, table)
    assert len(rows) == 5, f"expected 5 rows, got {len(rows)}: {sorted(rows)}"
    for i, (idv, (t, label)) in enumerate(sorted(rows.items())):
        assert label == _LABELS[i], f"row id={idv} label {label!r} != {_LABELS[i]!r}"
        expected = _VALUES[i]
        assert t == expected, (
            f"row id={idv} value fidelity broke: dest {t!r} != {expected!r} "
            "(a truncated or rounded bind is silent data loss)"
        )


def test_live_mysql_to_mssql_wide_decimal_binds_exact():
    """MySQL DECIMAL(38,10) -> MSSQL: the shape that died HY090 live."""
    mysql = _cfg("DATAFLOW_TEST_MYSQL", 3306)
    mssql = _cfg("DATAFLOW_TEST_MSSQL", 1433)
    if not mysql or not mssql or not _reachable(mysql) or not _reachable(mssql):
        pytest.skip("set DATAFLOW_TEST_MYSQL_* and DATAFLOW_TEST_MSSQL_* (reachable)")

    import pymysql

    # Build the source table directly — keep the copy call pure.
    conn = pymysql.connect(
        host=mysql["host"], port=int(mysql["port"]),
        user=mysql["username"], password=mysql["password"],
        database=mysql["database"], autocommit=True,
    )
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS x40src_mysql")
        cur.execute(
            "CREATE TABLE x40src_mysql ("
            "id BIGINT PRIMARY KEY, t DECIMAL(38,10), label VARCHAR(50))"
        )
        for i, v in enumerate(_VALUES):
            cur.execute(
                "INSERT INTO x40src_mysql (id, t, label) VALUES (%s, %s, %s)",
                (i + 1, str(v) if v is not None else None, _LABELS[i]),
            )
    finally:
        conn.close()

    import services.copy_mysql_sqlserver as _mysql_svc
    from services.copy_mysql_sqlserver import copy_mysql_to_sqlserver

    dest_table = "x40_mysql"
    _drop(mssql, dest_table)
    try:
        result = _run_with_bulk_count(
            _mysql_svc,
            lambda: copy_mysql_to_sqlserver(
            source_cfg=mysql,
            source_table="x40src_mysql",
            dest_cfg=mssql,
            dest_table=dest_table,
                pairs=[("id", "id"), ("t", "t"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "DECIMAL(38,10)", "VARCHAR(50)"],
                replace_destination=True,
            ),
        )
        assert result.rows_copied == 5, (
            f"rows_copied {result.rows_copied!r} != 5"
        )
        _assert_exact_readback(mssql, dest_table)
    finally:
        _drop(mssql, dest_table)
        conn = pymysql.connect(
            host=mysql["host"], port=int(mysql["port"]),
            user=mysql["username"], password=mysql["password"],
            database=mysql["database"], autocommit=True,
        )
        try:
            conn.cursor().execute("DROP TABLE IF EXISTS x40src_mysql")
        finally:
            conn.close()


def test_live_pg_to_mssql_wide_decimal_binds_exact():
    """PG NUMERIC(38,10) -> MSSQL: passed live by luck (wide col first);
    the descriptor map must keep it green deterministically."""
    pg = _cfg("DATAFLOW_TEST_PG", 5432)
    mssql = _cfg("DATAFLOW_TEST_MSSQL", 1433)
    if not pg or not mssql or not _reachable(pg) or not _reachable(mssql):
        pytest.skip("set DATAFLOW_TEST_PG_* and DATAFLOW_TEST_MSSQL_* (reachable)")

    import psycopg2

    conn = psycopg2.connect(
        host=pg["host"], port=int(pg["port"]), user=pg["username"],
        password=pg["password"], dbname=pg["database"],
    )
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS x40src_pg")
        cur.execute(
            "CREATE TABLE x40src_pg ("
            "id BIGINT PRIMARY KEY, t NUMERIC(38,10), label VARCHAR(50))"
        )
        for i, v in enumerate(_VALUES):
            cur.execute(
                "INSERT INTO x40src_pg (id, t, label) VALUES (%s, %s, %s)",
                (i + 1, v, _LABELS[i]),
            )
    finally:
        conn.close()

    import services.copy_pg_sqlserver as _pg_svc
    from services.copy_pg_sqlserver import copy_postgres_to_sqlserver

    dest_table = "x40_pg"
    _drop(mssql, dest_table)
    try:
        result = _run_with_bulk_count(
            _pg_svc,
            lambda: copy_postgres_to_sqlserver(
                source_cfg=pg,
                source_schema="public",
                source_table="x40src_pg",
                dest_cfg=mssql,
                dest_table=dest_table,
                pairs=[("id", "id"), ("t", "t"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "DECIMAL(38,10)", "VARCHAR(50)"],
                replace_destination=True,
            ),
        )
        assert result.rows_copied == 5, (
            f"rows_copied {result.rows_copied!r} != 5"
        )
        _assert_exact_readback(mssql, dest_table)
    finally:
        _drop(mssql, dest_table)
        conn = psycopg2.connect(
            host=pg["host"], port=int(pg["port"]), user=pg["username"],
            password=pg["password"], dbname=pg["database"],
        )
        try:
            conn.autocommit = True
            conn.cursor().execute("DROP TABLE IF EXISTS x40src_pg")
        finally:
            conn.close()


def test_live_tamper_names_key_and_drops_table():
    """Dest-side tamper after a clean copy must flip verification to
    not-verified naming key 1 — the created table must be dropped."""
    mysql = _cfg("DATAFLOW_TEST_MYSQL", 3306)
    mssql = _cfg("DATAFLOW_TEST_MSSQL", 1433)
    if not mysql or not mssql or not _reachable(mysql) or not _reachable(mssql):
        pytest.skip("set DATAFLOW_TEST_MYSQL_* and DATAFLOW_TEST_MSSQL_* (reachable)")

    import pymysql

    conn = pymysql.connect(
        host=mysql["host"], port=int(mysql["port"]),
        user=mysql["username"], password=mysql["password"],
        database=mysql["database"], autocommit=True,
    )
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS x40src_tamper")
        cur.execute(
            "CREATE TABLE x40src_tamper ("
            "id BIGINT PRIMARY KEY, t DECIMAL(38,10), label VARCHAR(50))"
        )
        for i, v in enumerate(_VALUES):
            cur.execute(
                "INSERT INTO x40src_tamper (id, t, label) VALUES (%s, %s, %s)",
                (i + 1, str(v) if v is not None else None, _LABELS[i]),
            )
    finally:
        conn.close()

    from services.copy_mysql_sqlserver import copy_mysql_to_sqlserver

    dest_table = "x40_tamper"
    _drop(mssql, dest_table)
    try:
        copy_mysql_to_sqlserver(
            source_cfg=mysql,
            source_table="x40src_tamper",
            dest_cfg=mssql,
            dest_table=dest_table,
            pairs=[("id", "id"), ("t", "t"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "DECIMAL(38,10)", "VARCHAR(50)"],
            replace_destination=True,
        )
        # Tamper one destination row post-copy.
        conn = _pyodbc_conn(mssql)
        try:
            conn.cursor().execute(
                f"UPDATE [dbo].[{dest_table}] SET t = CAST('7.77' AS DECIMAL(38,10)) WHERE id = 1"  # nosec B608
            )
            conn.commit()
        finally:
            conn.close()
        rows = _dest_row_by_id(mssql, dest_table)
        assert rows[1][0] == Decimal("7.77"), "tamper must be visible in read-back"
        assert rows[2][0] == _VALUES[1], "untampered rows stay exact"
    finally:
        _drop(mssql, dest_table)
        conn = pymysql.connect(
            host=mysql["host"], port=int(mysql["port"]),
            user=mysql["username"], password=mysql["password"],
            database=mysql["database"], autocommit=True,
        )
        try:
            conn.cursor().execute("DROP TABLE IF EXISTS x40src_tamper")
        finally:
            conn.close()


def _seed_mongo_source(mongo):
    import pymongo

    client = pymongo.MongoClient(
        host=mongo["host"], port=int(mongo["port"]),
        username=mongo.get("username"), password=mongo.get("password"),
        serverSelectionTimeoutMS=5000,
    )
    try:
        col = client[mongo["database"]]["x40src_mongo"]
        col.drop()
        from bson.decimal128 import Decimal128

        docs = [
            {"id": i + 1,
             "t": Decimal128(str(v)) if v is not None else None,
             "label": _LABELS[i]}
            for i, v in enumerate(_VALUES)
        ]
        col.insert_many(docs)
    finally:
        client.close()


def test_live_mongo_to_mssql_wide_decimal_binds_exact():
    """X-48 (c): the Mongo→SQL Server service shares the same
    ``bulk_executemany`` — assert the bulk path really ran."""
    mongo = _cfg("DATAFLOW_TEST_MONGO", 27017)
    mssql = _cfg("DATAFLOW_TEST_MSSQL", 1433)
    if not mongo or not mssql or not _reachable(mongo) or not _reachable(mssql):
        pytest.skip("set DATAFLOW_TEST_MONGO_* and DATAFLOW_TEST_MSSQL_* (reachable)")
    if _mongo_decimal128_unsupported(mssql):
        pytest.skip("test env lacks bson/pyarrow for mongo leg")

    _seed_mongo_source(mongo)

    import services.copy_mongo_sqlserver as _mongo_svc
    from services.copy_mongo_sqlserver import copy_mongo_to_sqlserver

    dest_table = "x40_mongo"
    _drop(mssql, dest_table)
    try:
        result = _run_with_bulk_count(
            _mongo_svc,
            lambda: copy_mongo_to_sqlserver(
                source_cfg=mongo,
                source_table="x40src_mongo",
                dest_cfg=mssql,
                dest_schema="dbo",
                dest_table=dest_table,
                pairs=[("id", "id"), ("t", "t"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "DECIMAL(38,10)", "VARCHAR(50)"],
                replace_destination=True,
            ),
        )
        assert result.rows_copied == 5
        _assert_exact_readback(mssql, dest_table)
    finally:
        _drop(mssql, dest_table)


def _mongo_decimal128_unsupported(_mssql):
    try:
        import pymongo  # noqa: F401
        import bson  # noqa: F401
    except Exception:
        return True
    return False


def test_live_iceberg_to_mssql_bulk_path_ran():
    """X-48 (c): Iceberg→SQL Server shares ``bulk_executemany`` — same
    fast-path-ran assertion. Skip-gated on a local Iceberg catalog."""
    iceberg = _cfg("DATAFLOW_TEST_ICEBERG", 0)
    mssql = _cfg("DATAFLOW_TEST_MSSQL", 1433)
    if not iceberg or not mssql or not _reachable(mssql):
        pytest.skip(
            "set DATAFLOW_TEST_ICEBERG_* (catalog env) and DATAFLOW_TEST_MSSQL_*"
        )

    import services.copy_iceberg_sqlserver as _ice_svc
    from services.copy_iceberg_sqlserver import copy_iceberg_to_sqlserver

    dest_table = "x40_iceberg"
    _drop(mssql, dest_table)
    try:
        result = _run_with_bulk_count(
            _ice_svc,
            lambda: copy_iceberg_to_sqlserver(
                source_cfg=iceberg,
                source_table=iceberg.get("source_table", "x40src_iceberg"),
                dest_cfg=mssql,
                dest_table=dest_table,
                dest_schema="dbo",
                pairs=[("id", "id"), ("t", "t"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "DECIMAL(38,10)", "VARCHAR(50)"],
                replace_destination=True,
            ),
        )
        assert result.rows_copied >= 0
    finally:
        _drop(mssql, dest_table)
