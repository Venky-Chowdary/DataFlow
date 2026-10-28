"""D149 / D137 — run 2 into a MySQL table DataFlow created was blocked.

SQL Server ``CI_AS`` (case-insensitive, accent-sensitive) landed on a MySQL
table created with the server default ``utf8mb4_0900_ai_ci``. Run 2 then saw a
source/destination accent-polarity change that DataFlow itself introduced and
refused its own table. The CREATE side must pick a collation with the source's
case and accent polarity: MySQL 8 has ``utf8mb4_0900_as_ci`` for CI_AS.
MariaDB has no such collation, so it keeps the honest refusal.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

from services.collation_carry import apply_dest_native_collation


@pytest.mark.parametrize(
    ("src_dialect", "src_type", "ddl", "expected"),
    [
        # SQL Server CI_AS -> accent-sensitive CI
        ("sqlserver", "VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS", "VARCHAR(20)",
         "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_ci"),
        ("sqlserver", "NVARCHAR(50) COLLATE Latin1_General_CI_AS", "VARCHAR(50) CHARACTER SET utf8mb4",
         "VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_ci"),
        # SQL Server binary / case-sensitive -> utf8mb4_bin
        ("sqlserver", "VARCHAR(20) COLLATE Latin1_General_BIN", "VARCHAR(20)",
         "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"),
        ("sqlserver", "VARCHAR(20) COLLATE Latin1_General_CS_AS", "VARCHAR(20)",
         "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"),
        # PostgreSQL / Oracle / SQLite default equality is case-sensitive
        ("postgresql", "TEXT", "LONGTEXT", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"),
        # CI_AI stays on the accent-insensitive unicode collation
        ("sqlserver", "VARCHAR(20) COLLATE Latin1_General_CI_AI", "VARCHAR(20)",
         "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"),
    ],
)
def test_mysql_create_collation_follows_source_polarity(src_dialect, src_type, ddl, expected):
    assert (
        apply_dest_native_collation(
            ddl, dest_dialect="mysql", source_dialect=src_dialect, source_type=src_type
        )
        == expected
    )


def test_unknown_source_collation_keeps_the_server_default():
    assert (
        apply_dest_native_collation(
            "VARCHAR(20)", dest_dialect="mysql", source_dialect="", source_type="VARCHAR(20)"
        )
        == "VARCHAR(20)"
    )


def test_mariadb_keeps_the_honest_refusal_for_accent_sensitive_ci():
    assert (
        apply_dest_native_collation(
            "VARCHAR(20)",
            dest_dialect="mariadb",
            source_dialect="sqlserver",
            source_type="VARCHAR(20) COLLATE Latin1_General_CI_AS",
        )
        == "VARCHAR(20)"
    )


def _reachable(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, int(port)), timeout=3).close()
        return True
    except OSError:
        return False


_MS = {
    "host": os.getenv("DATAFLOW_TEST_MSSQL_HOST", ""),
    "port": int(os.getenv("DATAFLOW_TEST_MSSQL_PORT", "1433")),
    "database": os.getenv("DATAFLOW_TEST_MSSQL_DB", "dataflow"),
    "username": os.getenv("DATAFLOW_TEST_MSSQL_USER", ""),
    "password": os.getenv("DATAFLOW_TEST_MSSQL_PASSWORD", ""),
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
        _MS["host"]
        and _MY["host"]
        and _reachable(_MS["host"], _MS["port"])
        and _reachable(_MY["host"], _MY["port"])
    ),
    reason="live SQL Server + MySQL: set DATAFLOW_TEST_MSSQL_* / DATAFLOW_TEST_MYSQL_*",
)
def test_sqlserver_to_mysql_full_refresh_append_run_two_verifies(monkeypatch):
    import pymysql
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

    table = "d149_" + uuid.uuid4().hex[:10]
    ms = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={_MS['host']},{_MS['port']};DATABASE={_MS['database']};"
        f"UID={_MS['username']};PWD={_MS['password']};TrustServerCertificate=yes",
        autocommit=True,
        timeout=10,
    )
    my = pymysql.connect(
        host=_MY["host"], port=_MY["port"], user=_MY["username"],
        password=_MY["password"], database=_MY["database"], autocommit=True,
    )

    def run():
        job_id = "d149" + uuid.uuid4().hex[:12]
        fake.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=EndpointConfig(
                    kind="database", format="mssql", table=table,
                    extra={"trust_server_certificate": True}, **_MS,
                ),
                destination=EndpointConfig(kind="database", format="mysql", table=table, **_MY),
                sync_mode="full_refresh_append",
                validation_mode="strict",
            ),
            job_id,
        )

    try:
        ms.execute(
            f"CREATE TABLE [{table}] (id BIGINT, name NVARCHAR(50), code VARCHAR(20), "  # nosec B608
            "note NVARCHAR(MAX), fixed NCHAR(4))"
        )
        for i in range(1, 4):
            ms.execute(
                f"INSERT INTO [{table}] VALUES (?, ?, ?, ?, ?)",  # nosec B608
                i, f"n{i} 中", f"c{i}", f"note {i}", "ab",
            )
        first = run()
        assert first.success, first.error
        cur = my.cursor()
        cur.execute(
            "SELECT column_name, collation_name FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = %s "
            "AND collation_name IS NOT NULL",
            (table,),
        )
        assert {c: n for c, n in cur.fetchall()} == {
            "name": "utf8mb4_0900_as_ci",
            "code": "utf8mb4_0900_as_ci",
            "note": "utf8mb4_0900_as_ci",
            "fixed": "utf8mb4_0900_as_ci",
        }
        second = run()
        assert second.success, second.error
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")  # nosec B608
        assert cur.fetchone()[0] == 6
    finally:
        my.cursor().execute(f"DROP TABLE IF EXISTS `{table}`")  # nosec B608
        my.close()
        ms.execute(f"DROP TABLE IF EXISTS [{table}]")  # nosec B608
        ms.close()
