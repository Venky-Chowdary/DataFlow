"""D157 / D66 / D59 — float carriers use exact IEEE-754 limits.

``_float_magnitude_bounds`` used rounded windows: ``1.79e308`` for SQL Server
FLOAT(53) refused the legal ``±DBL_MAX`` (1.7976931348623157e308) after a
green preflight, and ``3.40e38`` for PostgreSQL REAL refused legal float32
values up to FLT_MAX (3.4028234663852886e38). The refusal also claimed the
row was "quarantined" while strict validation aborts the load.
"""

from __future__ import annotations

import os
import socket
import struct
import sys
import uuid

import pytest

from connectors.sql_bind import coerce_float_wire

DBL_MAX = sys.float_info.max
FLT_MAX = struct.unpack("<f", struct.pack("<f", 3.4028234663852886e38))[0]


@pytest.mark.parametrize("value", [DBL_MAX, -DBL_MAX, sys.float_info.min, -sys.float_info.min])
def test_sqlserver_float53_accepts_the_double_limits(value):
    assert coerce_float_wire(value, ddl_type="FLOAT(53)", engine="sqlserver") == value
    assert coerce_float_wire(value, ddl_type="FLOAT", engine="mssql") == value


@pytest.mark.parametrize("engine", ["postgresql", "sqlserver"])
@pytest.mark.parametrize(
    "value",
    [FLT_MAX, -FLT_MAX, 3.4028235e38, 3.4e38, 2.0**-126],
)
def test_float32_carrier_accepts_every_value_that_stores_as_a_finite_real(engine, value):
    assert coerce_float_wire(value, ddl_type="REAL", engine=engine) == value


@pytest.mark.parametrize("value", [3.4028236e38, 1e39, 1e308])
def test_float32_carrier_still_refuses_overflow_with_an_honest_message(value):
    with pytest.raises(ValueError) as exc:
        coerce_float_wire(value, ddl_type="REAL", engine="postgresql")
    msg = str(exc.value)
    assert "refused" in msg
    assert "strict validation fails the load" in msg
    assert "— quarantined" not in msg


def test_sqlite_real_is_ieee64():
    assert coerce_float_wire(DBL_MAX, ddl_type="REAL", engine="sqlite") == DBL_MAX


# --------------------------------------------------------------------------
# Live: PG double precision / real -> new SQL Server table, preflight ON.
# --------------------------------------------------------------------------


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
        return True
    except OSError:
        return False


_PG = {
    "host": os.getenv("PGHOST", ""),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", ""),
    "username": os.getenv("PGUSER", ""),
    "password": os.getenv("PGPASSWORD", ""),
}
_MS = {
    "host": os.getenv("DATAFLOW_TEST_MSSQL_HOST", ""),
    "port": int(os.getenv("DATAFLOW_TEST_MSSQL_PORT", "1433")),
    "database": os.getenv("DATAFLOW_TEST_MSSQL_DB", "dataflow"),
    "username": os.getenv("DATAFLOW_TEST_MSSQL_USER", ""),
    "password": os.getenv("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
}


@pytest.mark.skipif(
    not (
        _PG["host"]
        and _MS["host"]
        and _reachable(_PG["host"], _PG["port"])
        and _reachable(_MS["host"], _MS["port"])
    ),
    reason="live PG + SQL Server: set PG* and DATAFLOW_TEST_MSSQL_* (reachable)",
)
def test_pg_extreme_floats_land_exactly_in_sqlserver(monkeypatch):
    import psycopg2
    import pyodbc

    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)

    table = "d157_" + uuid.uuid4().hex[:10]
    rows = [(1, DBL_MAX, FLT_MAX), (2, -DBL_MAX, -FLT_MAX), (3, 1.5, 0.5)]
    pg = psycopg2.connect(
        host=_PG["host"], port=_PG["port"], user=_PG["username"],
        password=_PG["password"], dbname=_PG["database"],
    )
    cur = pg.cursor()
    ms = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={_MS['host']},{_MS['port']};DATABASE={_MS['database']};"
        f"UID={_MS['username']};PWD={_MS['password']};TrustServerCertificate=yes",
        autocommit=True,
        timeout=10,
    )
    try:
        cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, d DOUBLE PRECISION, r REAL)")  # nosec B608
        cur.executemany(f"INSERT INTO {table} VALUES (%s, %s, %s)", rows)  # nosec B608
        pg.commit()
        job_id = "d157" + uuid.uuid4().hex[:12]
        fake.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="database", format="postgresql", table=table, **_PG),
                destination=EndpointConfig(
                    kind="database",
                    format="mssql",
                    table=table,
                    extra={"trust_server_certificate": True},
                    **_MS,
                ),
                sync_mode="overwrite",
                validation_mode="strict",
            ),
            job_id,
        )
        assert result.success, result.error
        got = ms.execute(f"SELECT id, d, r FROM [{table}] ORDER BY id").fetchall()  # nosec B608
        def as_real(v):
            return struct.unpack("<f", struct.pack("<f", v))[0]

        # DOUBLE is bit-exact; REAL is compared as the float32 it stores (PG
        # renders it as the shortest float32 text, 3.4028235e+38).
        assert [(int(i), d, as_real(r)) for i, d, r in got] == rows
    finally:
        ms.execute(f"DROP TABLE IF EXISTS [{table}]")  # nosec B608
        ms.close()
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.commit()
        pg.close()
