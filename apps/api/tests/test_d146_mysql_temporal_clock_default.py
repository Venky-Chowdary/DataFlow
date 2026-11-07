"""D146 — CREATE TABLE on MySQL/MariaDB failed "(1067) Invalid default value".

A source ``date DEFAULT now()`` (or ``DEFAULT CURRENT_TIMESTAMP``) was carried
as ``DATE DEFAULT CURRENT_TIMESTAMP``. MySQL accepts a timestamp clock only on
DATETIME/TIMESTAMP, so the CREATE died after a green preflight. The column's
own clock (``(CURRENT_DATE)`` / ``(CURRENT_TIME)``) is the same rule.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from services.schema_fidelity import _normalize_default_sql, mysql_default_is_legal


@pytest.mark.parametrize("dialect", ["mysql", "mariadb"])
@pytest.mark.parametrize(
    ("expr", "dest_type", "expected"),
    [
        ("now()", "DATE", "(CURRENT_DATE)"),
        ("CURRENT_TIMESTAMP", "DATE", "(CURRENT_DATE)"),
        ("now()", "TIME(6)", "(CURRENT_TIME)"),
        ("now()", "DATETIME(6)", "CURRENT_TIMESTAMP(6)"),
        ("CURRENT_DATE", "DATE", "(CURRENT_DATE)"),
    ],
)
def test_clock_default_matches_the_column_family(dialect, expr, dest_type, expected):
    assert _normalize_default_sql(expr, dialect, dest_type) == expected


def test_timestamp_clock_on_date_is_not_legal_mysql():
    assert not mysql_default_is_legal("CURRENT_TIMESTAMP", "DATE")
    assert not mysql_default_is_legal("now()", "TIME")
    assert mysql_default_is_legal("CURRENT_TIMESTAMP(6)", "DATETIME(6)")
    assert mysql_default_is_legal("(CURRENT_DATE)", "DATE")


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
_MY = {
    "host": os.getenv("DATAFLOW_TEST_MYSQL_HOST", ""),
    "port": int(os.getenv("DATAFLOW_TEST_MYSQL_PORT", "3306")),
    "database": os.getenv("DATAFLOW_TEST_MYSQL_DB", ""),
    "username": os.getenv("DATAFLOW_TEST_MYSQL_USER", ""),
    "password": os.getenv("DATAFLOW_TEST_MYSQL_PASSWORD", ""),
}


@pytest.mark.skipif(
    not (
        _PG["host"]
        and _MY["host"]
        and _reachable(_PG["host"], _PG["port"])
        and _reachable(_MY["host"], _MY["port"])
    ),
    reason="live PG + MySQL: set PG* and DATAFLOW_TEST_MYSQL_* (reachable)",
)
def test_pg_date_default_now_creates_on_mysql(monkeypatch):
    import psycopg2
    import pymysql

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

    table = "d146_" + uuid.uuid4().hex[:10]
    pg = psycopg2.connect(
        host=_PG["host"], port=_PG["port"], user=_PG["username"],
        password=_PG["password"], dbname=_PG["database"],
    )
    cur = pg.cursor()
    my = pymysql.connect(
        host=_MY["host"], port=_MY["port"], user=_MY["username"],
        password=_MY["password"], database=_MY["database"], autocommit=True,
    )
    try:
        cur.execute(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, d DATE DEFAULT now(), "  # nosec B608
            "d2 DATE DEFAULT CURRENT_TIMESTAMP, ts TIMESTAMP DEFAULT now(), plain DATE)"
        )
        cur.execute(
            f"INSERT INTO {table} (id, d, d2, ts, plain) VALUES "  # nosec B608
            "(1, '2024-01-02', '2024-01-03', '2024-01-02 03:04:05', NULL)"
        )
        pg.commit()
        job_id = "d146" + uuid.uuid4().hex[:12]
        fake.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="database", format="postgresql", table=table, **_PG),
                destination=EndpointConfig(kind="database", format="mysql", table=table, **_MY),
                sync_mode="overwrite",
                validation_mode="strict",
            ),
            job_id,
        )
        assert result.success, result.error
        c = my.cursor()
        c.execute(
            "SELECT column_name, column_default FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = %s AND column_name IN ('d', 'd2')",
            (table,),
        )
        assert {str(n): str(v).lower() for n, v in c.fetchall()} == {
            "d": "curdate()",
            "d2": "curdate()",
        }
        c.execute(f"SELECT id, d, d2, plain FROM `{table}`")  # nosec B608
        row = c.fetchone()
        assert (row[0], str(row[1]), str(row[2]), row[3]) == (1, "2024-01-02", "2024-01-03", None)
    finally:
        my.cursor().execute(f"DROP TABLE IF EXISTS `{table}`")  # nosec B608
        my.close()
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.commit()
        pg.close()
