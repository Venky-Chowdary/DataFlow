"""D158 — PostgreSQL -> SQL Server run 2 into the table run 1 created.

Run 1 lands PostgreSQL ``VARCHAR(50)`` on ``NVARCHAR(50)`` (a PostgreSQL text
column holds every code point; a code-page ``VARCHAR`` would rewrite ``中`` to
``?``). Run 2 then maps onto that existing ``NVARCHAR(50) COLLATE ...`` column
from the plan / Map / job-detail surfaces, which run outside a transfer and so
have no source engine bound. ``is_lossy_coercion`` asked the national charset
check with an empty engine, which answers "the source might be a code page" and
graded the promotion an invented Unicode repertoire: "fidelity collapse" on
DataFlow's own column.

Unit legs pin each surface with no engine bound; the live leg runs PG ->
SQL Server incremental_upsert twice with preflight ON.
"""

from __future__ import annotations

import json
import os
import socket
import uuid
from pathlib import Path

import pytest

from services.source_engine_scope import active_source_engine
from services.type_coercion import is_lossy_coercion

_COLLATE = " COLLATE SQL_Latin1_General_CP1_CI_AS"


def test_varchar_to_nvarchar_is_not_lossy_for_a_unicode_source_with_no_engine_bound():
    assert active_source_engine() == ""
    assert not is_lossy_coercion(
        "varchar(50)", "nvarchar(50)", source_db="postgresql", dest_db="sqlserver"
    )
    assert not is_lossy_coercion(
        "character varying(50)",
        "NVARCHAR(50)" + _COLLATE,
        source_db="postgresql",
        dest_db="sqlserver",
        dest_table_exists=True,
    )
    # The binding is scoped to the call.
    assert active_source_engine() == ""


def test_code_page_source_still_reports_the_invented_national_charset():
    # SQL Server VARCHAR really is a code page: NVARCHAR invents a repertoire.
    assert is_lossy_coercion(
        "varchar(50)", "nvarchar(50)", source_db="mssql", dest_db="sqlserver"
    )


_SRC = [
    {"name": "id", "inferred_type": "bigint"},
    {"name": "name", "inferred_type": "character varying(50)", "samples": ["n1", "n2"]},
    {"name": "note", "inferred_type": "text", "samples": ["a", "b"]},
    {"name": "updated_at", "inferred_type": "bigint"},
]
_RUN1_TABLE = [
    {"name": "id", "inferred_type": "BIGINT"},
    {"name": "name", "inferred_type": "NVARCHAR(50)" + _COLLATE},
    {"name": "note", "inferred_type": "NVARCHAR(MAX)" + _COLLATE},
    {"name": "updated_at", "inferred_type": "BIGINT"},
]


def test_mapper_scores_varchar_onto_its_own_nvarchar_as_a_clean_exact_match():
    from services.semantic_mapper import map_columns

    out = map_columns(
        [c["name"] for c in _SRC],
        [c["name"] for c in _RUN1_TABLE],
        source_schemas=_SRC,
        target_schemas=_RUN1_TABLE,
        destination_db_type="sqlserver",
        destination_table_exists=True,
        source_db_type="postgresql",
    )
    by = {m["source"]: m for m in out}
    name = by["name"]
    assert name["target"] == "name"
    assert "lossy" not in name["reasoning"], name
    assert float(name["confidence"]) >= 0.95, name
    assert not name["requires_review"], name


def test_mapping_proof_built_outside_a_transfer_grades_varchar_to_nvarchar_preserve():
    from services.mapping_proof import mapping_proof_or_build

    proof = mapping_proof_or_build(
        [
            {
                "source": "name",
                "target": "name",
                "source_type": "character varying(50)",
                "target_type": "NVARCHAR(50)" + _COLLATE,
                "transform": "none",
                "confidence": 0.99,
            }
        ],
        target_columns=["name"],
        destination_db_type="sqlserver",
        source_kind="database",
        dest_kind="database",
        destination_table_exists=True,
        source_db_type="postgresql",
    )
    row = proof["mappings"][0]
    assert row["fidelity"] != "lossy_cast", row
    assert not row["type_narrowing"], row


# --------------------------------------------------------------------------
# Live: PG table (varchar / text / jsonb) -> new SQL Server table, run 1 then
# run 2 after a source update, preflight ON.
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

_live = pytest.mark.skipif(
    not (
        _PG["host"]
        and _MS["host"]
        and _reachable(_PG["host"], _PG["port"])
        and _reachable(_MS["host"], _MS["port"])
    ),
    reason="live PG + SQL Server: set PG* and DATAFLOW_TEST_MSSQL_* (reachable)",
)


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


def _mssql():
    import pyodbc

    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={_MS['host']},{_MS['port']};DATABASE={_MS['database']};"
        f"UID={_MS['username']};PWD={_MS['password']};TrustServerCertificate=yes",
        timeout=10,
    )


def _request(table: str):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table=table, **_PG),
        destination=EndpointConfig(
            kind="database",
            format="mssql",
            table=table,
            extra={"trust_server_certificate": True},
            **_MS,
        ),
        sync_mode="incremental_upsert",
        stream_contracts=[
            {
                "name": table,
                "cursor_field": "updated_at",
                "cursor_semantics": "monotonic_sequence",
                "sync_mode": "incremental_upsert",
                "primary_key": ["id"],
                "selected": True,
            }
        ],
        validation_mode="strict",
    )


def _run(fake, table: str):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "d158" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(table), job_id)


def _dest_columns(table: str) -> dict[str, tuple[str, int | None]]:
    conn = _mssql()
    try:
        rows = conn.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH "
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?",
            table,
        ).fetchall()
        return {str(r[0]): (str(r[1]), r[2]) for r in rows}
    finally:
        conn.close()


@_live
def test_pg_to_sqlserver_incremental_run_two_into_own_table_verifies(isolated):
    import psycopg2

    table = "d158_" + uuid.uuid4().hex[:10]
    pg = psycopg2.connect(
        host=_PG["host"], port=_PG["port"], user=_PG["username"],
        password=_PG["password"], dbname=_PG["database"],
    )
    cur = pg.cursor()
    try:
        cur.execute(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name VARCHAR(50), "  # nosec B608
            "note TEXT, payload JSONB, updated_at BIGINT)"
        )
        for i in range(1, 6):
            cur.execute(
                f"INSERT INTO {table} VALUES (%s, %s, %s, %s, %s)",  # nosec B608
                (i, f"n{i} 中", f"note {i}", json.dumps({"k": i}), i),
            )
        pg.commit()

        first = _run(isolated, table)
        assert first.success, first.error
        created = _dest_columns(table)

        cur.execute(
            f"UPDATE {table} SET name = 'changed 中', payload = %s, "  # nosec B608
            "updated_at = 100 WHERE id = 2",
            (json.dumps({"k": 99}),),
        )
        pg.commit()

        second = _run(isolated, table)
        assert second.success, second.error
        assert not any("ADD" in str(d).upper() for d in second.ddl_executed), (
            second.ddl_executed
        )
        # Run 2 adds no column: every source lands on the column run 1 made.
        assert _dest_columns(table) == created

        conn = _mssql()
        try:
            rows = conn.execute(
                f"SELECT id, name, payload, updated_at FROM [{table}] ORDER BY id"  # nosec B608
            ).fetchall()
        finally:
            conn.close()
        assert len(rows) == 5
        assert rows[1][1] == "changed 中"
        assert json.loads(rows[1][2]) == {"k": 99}
        assert int(rows[1][3]) == 100
    finally:
        try:
            conn = _mssql()
            conn.execute(f"DROP TABLE IF EXISTS [{table}]")  # nosec B608
            conn.commit()
            conn.close()
        finally:
            cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            pg.commit()
            pg.close()
