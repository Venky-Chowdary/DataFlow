"""D141 — DataFlow picks a carrier for its own new table, then blocks on it.

Two create-new stamps were graded "fidelity collapse" against themselves:

* A Kafka ``VARCHAR`` lands on the ``NVARCHAR(MAX)`` SQL Server create-new
  stamps. Kafka was missing from the Unicode-only sources, so the national
  charset check called the promotion an invented repertoire (lossy_cast, 0.70).
* An offset-pinned ``TIMESTAMP WITH TIME ZONE`` / ``DATETIMEOFFSET`` lands on
  SQLite ``TIMESTAMPTZ`` — the only aware carrier SQLite has, written as the
  UTC instant. The tz -> ltz rule treated it as an offset rewrite with an
  alternative carrier, which SQLite does not offer, so every such load into a
  new SQLite table was blocked at Validate.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from services.type_coercion import is_lossy_coercion


def test_kafka_text_onto_created_nvarchar_max_is_not_lossy():
    assert not is_lossy_coercion(
        "VARCHAR",
        "NVARCHAR(MAX) COLLATE Latin1_General_BIN",
        dest_db="sqlserver",
        source_db="kafka",
    )


def test_kafka_to_sqlserver_create_new_plan_has_no_lossy_column():
    from services.mapping_pipeline import run_mapping_pipeline

    out = run_mapping_pipeline(
        ["name", "doc"],
        [],
        source_schemas=[
            {"name": "name", "inferred_type": "VARCHAR", "samples": ["Ada", "中文"]},
            {"name": "doc", "inferred_type": "JSON", "samples": ['{"a": 1}', '{"b": 2}']},
        ],
        destination_db_type="sqlserver",
        destination_table_exists=False,
        use_llm=False,
        source_db_type="kafka",
    )
    proof = {r["source"]: r for r in out["mapping_proof"]["mappings"]}
    assert proof["name"]["fidelity"] != "lossy_cast", proof["name"]
    assert float(next(m for m in out["mappings"] if m["source"] == "name")["confidence"]) >= 0.85


# DATETIMEOFFSET(7) keeps its honest 100ns -> microsecond narrowing verdict.
@pytest.mark.parametrize("src", ["TIMESTAMP WITH TIME ZONE", "TIMESTAMP_TZ(6)", "DATETIMEOFFSET(6)"])
def test_offset_pinned_instant_onto_sqlite_timestamptz_is_not_lossy(src):
    assert not is_lossy_coercion(src, "TIMESTAMPTZ", dest_db="sqlite")


def test_naive_wall_clock_onto_sqlite_timestamptz_still_invents_utc():
    assert is_lossy_coercion("TIMESTAMP", "TIMESTAMPTZ", dest_db="sqlite")


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, source, dest_path: Path, table: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    job_id = "d141" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=source,
            destination=EndpointConfig(
                kind="database", format="sqlite", database=str(dest_path), table=table
            ),
            sync_mode="overwrite",
            validation_mode="strict",
        ),
        job_id,
    )


def _instants(path: Path, table: str) -> dict[int, datetime | None]:
    conn = sqlite3.connect(str(path))
    try:
        return {
            int(i): (datetime.fromisoformat(ts) if ts is not None else None)
            for i, ts in conn.execute(f"SELECT id, ts FROM {table} ORDER BY id")  # nosec B608
        }
    finally:
        conn.close()


_EXPECTED = {
    1: datetime.fromisoformat("2024-01-01T10:00:00.123456+05:30"),
    2: datetime.fromisoformat("2024-06-01T08:00:00-04:00"),
    3: None,
}


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


@pytest.mark.skipif(
    not (_MS["host"] and _reachable(_MS["host"], _MS["port"])),
    reason="live SQL Server: set DATAFLOW_TEST_MSSQL_* (reachable)",
)
def test_sqlserver_datetimeoffset_into_new_sqlite_table_loads_and_verifies(tmp_path, isolated):
    import pyodbc

    from src.transfer.models import EndpointConfig

    table = "d141_" + uuid.uuid4().hex[:10]
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={_MS['host']},{_MS['port']};DATABASE={_MS['database']};"
        f"UID={_MS['username']};PWD={_MS['password']};TrustServerCertificate=yes",
        autocommit=True,
        timeout=10,
    )
    try:
        conn.execute(f"CREATE TABLE [{table}] (id BIGINT PRIMARY KEY, ts DATETIMEOFFSET(6))")  # nosec B608
        conn.execute(
            f"INSERT INTO [{table}] VALUES "  # nosec B608
            "(1, '2024-01-01 10:00:00.123456 +05:30'), "
            "(2, '2024-06-01 08:00:00 -04:00'), (3, NULL)"
        )
        dest = tmp_path / "d141.sqlite"
        result = _run(
            isolated,
            EndpointConfig(
                kind="database",
                format="mssql",
                table=table,
                extra={"trust_server_certificate": True},
                **_MS,
            ),
            dest,
            table,
        )
        assert result.success, result.error
        assert _instants(dest, table) == _EXPECTED
    finally:
        conn.execute(f"DROP TABLE IF EXISTS [{table}]")  # nosec B608
        conn.close()
