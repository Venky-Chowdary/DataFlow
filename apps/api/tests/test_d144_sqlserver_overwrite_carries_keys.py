"""D144 — a SQL Server overwrite create dropped the source PRIMARY KEY / UNIQUE.

An overwrite rebuilds the table from one snapshot, so the source key holds on
the copy; the PostgreSQL, MySQL and SQLite writers carry it via
``carry_source_keys``. The SQL Server / Oracle writers are reached through the
registry dispatch, which never forwarded that flag, and the generic CREATE
also ignored it. An append create keeps withholding the key by design.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest


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

pytestmark = pytest.mark.skipif(
    not (_MS["host"] and _reachable(_MS["host"], _MS["port"])),
    reason="live SQL Server: set DATAFLOW_TEST_MSSQL_* (reachable)",
)


def _connect():
    import pyodbc

    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={_MS['host']},{_MS['port']};DATABASE={_MS['database']};"
        f"UID={_MS['username']};PWD={_MS['password']};TrustServerCertificate=yes",
        autocommit=True,
        timeout=10,
    )


def _keys(conn, table: str) -> set[tuple[str, str]]:
    rows = conn.execute(
        "SELECT kc.type, c.name FROM sys.key_constraints kc "
        "JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id "
        "AND ic.index_id = kc.unique_index_id "
        "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "WHERE kc.parent_object_id = OBJECT_ID(?)",
        table,
    ).fetchall()
    return {(str(t).strip(), str(n)) for t, n in rows}


@pytest.mark.parametrize(
    ("sync_mode", "expected"),
    [
        ("overwrite", {("PK", "id"), ("UQ", "code")}),
        ("full_refresh_append", set()),
    ],
)
def test_sqlserver_create_carries_source_keys_only_on_overwrite(monkeypatch, sync_mode, expected):
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

    src, dst = "d144s_" + uuid.uuid4().hex[:8], "d144d_" + uuid.uuid4().hex[:8]
    conn = _connect()

    def endpoint(table):
        return EndpointConfig(
            kind="database", format="mssql", table=table,
            extra={"trust_server_certificate": True}, **_MS,
        )

    try:
        conn.execute(
            f"CREATE TABLE [{src}] (id BIGINT NOT NULL PRIMARY KEY, "  # nosec B608
            "code VARCHAR(10) NOT NULL UNIQUE, n DECIMAL(12,2))"
        )
        conn.execute(f"INSERT INTO [{src}] VALUES (1, 'a', 1.50), (2, 'b', NULL)")  # nosec B608
        job_id = "d144" + uuid.uuid4().hex[:12]
        fake.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=endpoint(src),
                destination=endpoint(dst),
                sync_mode=sync_mode,
                validation_mode="strict",
            ),
            job_id,
        )
        assert result.success, result.error
        assert _keys(conn, dst) == expected
    finally:
        conn.execute(f"DROP TABLE IF EXISTS [{src}]")  # nosec B608
        conn.execute(f"DROP TABLE IF EXISTS [{dst}]")  # nosec B608
        conn.close()
