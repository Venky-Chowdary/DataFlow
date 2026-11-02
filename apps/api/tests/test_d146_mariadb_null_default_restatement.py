"""D146 — MariaDB CREATE for DATE / naive timestamp columns failed
``(1067) Invalid default value``.

The CREATE itself is fine. The failure is the follow-up ``MODIFY COLUMN`` that
restates a column's attributes: MariaDB's ``information_schema.column_default``
reports the *text* ``NULL`` for a nullable column with no default (MySQL reports
SQL NULL), and that text was quoted into ``DATETIME(6) DEFAULT 'NULL'``.
MariaDB also lists clock defaults as bare functions and string literals already
quoted, which the restatement must not re-quote.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from connectors.schema_drift import _mysql_preserved_column_clause


class _Cursor:
    def __init__(self, row):
        self._row = row

    def execute(self, *_a, **_k):
        return None

    def fetchall(self):
        return [self._row]


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (("YES", "NULL", ""), ""),
        (("YES", None, ""), ""),
        (("NO", "current_timestamp(6)", ""), " NOT NULL DEFAULT current_timestamp(6)"),
        (("YES", "curdate()", ""), " DEFAULT curdate()"),
        (("YES", "'abc'", ""), " DEFAULT 'abc'"),
        (("YES", "it's", ""), " DEFAULT 'it''s'"),
        (("YES", "5", ""), " DEFAULT '5'"),
        (("YES", "CURRENT_TIMESTAMP", "DEFAULT_GENERATED"), " DEFAULT CURRENT_TIMESTAMP"),
    ],
)
def test_restated_attributes_never_turn_the_text_null_into_a_default(row, expected):
    assert _mysql_preserved_column_clause(_Cursor(row), "", "t", "c") == expected


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
_MA = {
    "host": os.getenv("DATAFLOW_TEST_MARIADB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DATAFLOW_TEST_MARIADB_PORT", "3307")),
    "database": os.getenv("DATAFLOW_TEST_MARIADB_DB", "dataflow"),
    "username": os.getenv("DATAFLOW_TEST_MARIADB_USER", "dataflow"),
    "password": os.getenv("DATAFLOW_TEST_MARIADB_PASSWORD", "dataflow"),
}


@pytest.mark.skipif(
    not (
        _PG["host"]
        and _reachable(_PG["host"], _PG["port"])
        and _reachable(_MA["host"], _MA["port"])
    ),
    reason="live PG + MariaDB: set PG* and DATAFLOW_TEST_MARIADB_* (default 127.0.0.1:3307)",
)
def test_pg_date_and_naive_timestamps_create_on_mariadb(monkeypatch):
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

    table = "d146m_" + uuid.uuid4().hex[:10]
    pg = psycopg2.connect(
        host=_PG["host"], port=_PG["port"], user=_PG["username"],
        password=_PG["password"], dbname=_PG["database"],
    )
    cur = pg.cursor()
    my = pymysql.connect(
        host=_MA["host"], port=_MA["port"], user=_MA["username"],
        password=_MA["password"], database=_MA["database"], autocommit=True,
    )
    try:
        cur.execute(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, d DATE, ts TIMESTAMP, "  # nosec B608
            "tstz TIMESTAMPTZ, d2 DATE NOT NULL DEFAULT CURRENT_DATE, "
            "ts2 TIMESTAMP NOT NULL DEFAULT now(), t1 TIME)"
        )
        cur.execute(
            f"INSERT INTO {table} (id, d, ts, tstz, t1) VALUES "  # nosec B608
            "(1, '2024-01-02', '2024-01-02 03:04:05', '2024-01-02 03:04:05+00', '10:00:00'), "
            "(2, NULL, NULL, NULL, NULL)"
        )
        pg.commit()
        job_id = "d146m" + uuid.uuid4().hex[:12]
        fake.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(kind="database", format="postgresql", table=table, **_PG),
                destination=EndpointConfig(kind="database", format="mysql", table=table, **_MA),
                sync_mode="overwrite",
                validation_mode="strict",
            ),
            job_id,
        )
        assert result.success, result.error
        c = my.cursor()
        c.execute(f"SELECT id, d, ts FROM `{table}` ORDER BY id")  # nosec B608
        rows = c.fetchall()
        assert [r[0] for r in rows] == [1, 2]
        assert str(rows[0][1]) == "2024-01-02" and rows[1][1] is None
    finally:
        my.cursor().execute(f"DROP TABLE IF EXISTS `{table}`")  # nosec B608
        my.close()
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.commit()
        pg.close()
