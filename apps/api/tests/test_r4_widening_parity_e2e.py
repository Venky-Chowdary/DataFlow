"""Type widening and numeric sizing: Validate and Execute decide alike (QA round 4).

Every case runs the route QA drives: MCP ``start_transfer`` -> ``/copilot/confirm``
-> job, on real engines (PostgreSQL, MySQL, SQL Server, DuckDB).

* D193 / D107: PG -> existing PG table, ``incremental_upsert``. The source
  column ``n`` widens INT -> BIGINT and gains values above 2^31. With the
  approval given once (``schema_drift_acknowledged`` and/or
  ``propagate_columns``) the plan was green and the job then failed
  "1 mapping(s) require review before Execute: n -> n": Execute reviewed the
  request's mappings before applying the evolution Validate had applied.
* D200: VARCHAR / NUMERIC widening into the table DataFlow created, same split
  ("2 mapping(s) require review"), on PG -> PG, PG -> MySQL, PG -> SQL Server,
  MySQL -> PG and PG -> DuckDB. On SQL Server the approved widen also has to
  keep NVARCHAR (not ALTER it down to code-page VARCHAR); on DuckDB the plan and
  the job disagreed on whether overwrite keeps the table.
* Blocked case: without approval the plan refuses, names the change
  ("Source type widening needs approval") and the parameter that approves it,
  and Execute given the same mappings refuses too. The destination is untouched.
* D68: a bare PG NUMERIC was sized from run 1's rows (DECIMAL(9,6)); run 2's
  larger value then failed. The carrier is now sized by the declaration.
* D202: PG TIMESTAMP -> new DuckDB table was blocked "TIMESTAMP_NTZ -> TIMESTAMP".
* D191: the refusal headline names the gate that failed (g9 NULL/empty key,
  g1 source read error, g2 unreachable destination), not "fidelity collapse".

Each engine is skipped when it is not reachable. Credentials come from the
environment (``PG*``, ``MYSQL_*``, ``MSSQL_SA_PASSWORD`` / ``DATAFLOW_TEST_MSSQL_PASSWORD``).
"""

from __future__ import annotations

import os
import socket
import uuid
from decimal import Decimal
from typing import Any

import pytest

from tests.test_d190_d194_d197_declared_string_types_e2e import (  # noqa: F401
    _wait_job,
    transfer_client,
)

pytest.importorskip("psycopg2")

PG = {
    "host": os.environ.get("PGHOST") or "127.0.0.1",
    "port": int(os.environ.get("PGPORT") or 5432),
    "user": os.environ.get("PGUSER") or "dataflow",
    "password": os.environ.get("PGPASSWORD") or "dataflow",
    "dbname": os.environ.get("PGDATABASE") or "dataflow",
}
MYSQL = {
    "host": os.environ.get("MYSQL_HOST") or "127.0.0.1",
    "port": int(os.environ.get("MYSQL_PORT") or 3306),
    "user": os.environ.get("MYSQL_USER") or "dataflow",
    "password": os.environ.get("MYSQL_PASSWORD") or "dataflow",
    "database": os.environ.get("MYSQL_DATABASE") or "dataflow",
}
MSSQL_PASSWORD = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD") or os.environ.get(
    "MSSQL_SA_PASSWORD", ""
)
BIG = 2**63 - 1
OVER_INT = 2**31


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


def _require(kind: str) -> None:
    if kind == "postgresql":
        if not _up(PG["port"]):
            pytest.skip("PostgreSQL not reachable")
        import psycopg2

        try:
            psycopg2.connect(**PG).close()
        except Exception as exc:  # environmental: rejected credentials
            pytest.skip(f"PostgreSQL login failed: {exc}")
    elif kind == "mysql":
        pytest.importorskip("pymysql")
        if not _up(MYSQL["port"]):
            pytest.skip("MySQL not reachable")
    elif kind == "sqlserver":
        pytest.importorskip("pymssql")
        if not MSSQL_PASSWORD or not _up(1433):
            pytest.skip("SQL Server not configured")
    elif kind == "duckdb":
        pytest.importorskip("duckdb")


def _connector(kind: str, tmp_path, **override: Any) -> str:
    from services.connector_store import create_connector

    rec: dict[str, Any] = {
        "name": f"r4w-{kind}-{uuid.uuid4().hex[:6]}",
        "type": kind,
        "ssl": False,
        "workspace_id": "",
    }
    if kind == "postgresql":
        rec.update(host=PG["host"], port=PG["port"], database=PG["dbname"],
                   username=PG["user"], password=PG["password"], schema="public")
    elif kind == "mysql":
        rec.update(host=MYSQL["host"], port=MYSQL["port"], database=MYSQL["database"],
                   username=MYSQL["user"], password=MYSQL["password"])
    elif kind == "sqlserver":
        rec.update(host="127.0.0.1", port=1433, database="dataflow", username="sa",
                   password=MSSQL_PASSWORD, schema="dbo",
                   extra={"trust_server_certificate": True})
    elif kind == "duckdb":
        rec.update(host="localhost", port=0, database=str(tmp_path / "r4w.duckdb"))
    rec.update(override)
    return str(create_connector(rec).id)


def _sql(kind: str, tmp_path, sql: str, params: Any = None, fetch: bool = False):
    if kind == "postgresql":
        import psycopg2

        conn = psycopg2.connect(**PG)
        conn.autocommit = True
    elif kind == "mysql":
        import pymysql

        conn = pymysql.connect(**MYSQL, autocommit=True)
    elif kind == "sqlserver":
        import pymssql

        conn = pymssql.connect(server="127.0.0.1", port=1433, user="sa",
                               password=MSSQL_PASSWORD, database="dataflow",
                               autocommit=True)
    else:
        import duckdb

        conn = duckdb.connect(str(tmp_path / "r4w.duckdb"))
    try:
        cur = conn.cursor()
        if kind == "sqlserver":
            cur.execute("SET LOCK_TIMEOUT 10000")
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return [tuple(r) for r in cur.fetchall()] if fetch else None
    finally:
        conn.close()


def _columns(kind: str, tmp_path, table: str) -> dict[str, tuple]:
    """column -> (data_type, char length, precision, scale, collation)."""
    if kind == "duckdb":
        rows = _sql(kind, tmp_path,
                    "SELECT column_name, data_type, character_maximum_length, "
                    "numeric_precision, numeric_scale, NULL FROM information_schema.columns "
                    f"WHERE table_name = '{table}'", fetch=True)
    else:
        rows = _sql(kind, tmp_path,
                    "SELECT column_name, data_type, character_maximum_length, "
                    "numeric_precision, numeric_scale, collation_name "
                    f"FROM information_schema.columns WHERE table_name = '{table}'",
                    fetch=True)
    return {str(r[0]).lower(): tuple(str(x).lower() if isinstance(x, str) else x for x in r[1:])
            for r in rows or []}


def _drop(kind: str, tmp_path, *tables: str) -> None:
    for table in tables:
        try:
            _sql(kind, tmp_path, f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass


def _stage(**kwargs: Any):
    from src.ai.copilot.transfer_tools import start_transfer

    staged = start_transfer(validation_mode="strict", **kwargs)
    return staged


def _run(client, **kwargs: Any) -> dict:
    """``start_transfer`` -> confirm -> finished job; the plan must be green."""
    staged = _stage(**kwargs)
    assert staged.success, staged.error
    plan_pf = ((staged.output or {}).get("plan") or {}).get("preflight") or {}
    assert plan_pf.get("passed") is True, plan_pf.get("blockers")
    confirmed = client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": staged.output["ack_id"], "reason": "r4 widening parity"},
    )
    assert confirmed.status_code == 200, confirmed.text
    body = confirmed.json()
    assert body.get("ok") is True, body
    job = _wait_job(str(body.get("job_id") or ""))
    # Validate passed, so Execute must too: the same decision on the same inputs.
    assert str(job.get("status") or "") == "completed", {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
    }
    return job


def _execute_without_approval(staged, source_id: str, dest_id: str, src_table: str,
                              dst_table: str, sync_mode: str, **extra: Any):
    """Execute with the plan's own mappings and no approval: must refuse too."""
    from services.connector_store import get_connector
    from src.ai.copilot.schema_tools import _endpoint_from_connector
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    mappings = list((staged.output or {}).get("engine_mappings") or [])
    assert mappings, "the refused plan must still report the mappings it graded"

    def endpoint(cid: str, table: str):
        conn = get_connector(cid)
        rec = conn.to_dict()
        return _endpoint_from_connector(rec, table=table)

    request = TransferRequest(
        source=endpoint(source_id, src_table),
        destination=endpoint(dest_id, dst_table),
        sync_mode=sync_mode,
        mappings=mappings,
        validation_mode="strict",
        **extra,
    )
    job_id = "r4w" + uuid.uuid4().hex[:16]
    return UniversalTransferEngine().execute_tracked(request, job_id)


# --------------------------------------------------------------------------- #
# D193 / D107: INT -> BIGINT into an existing PG table, incremental_upsert
# --------------------------------------------------------------------------- #

APPROVALS = {
    "acknowledged": {"schema_drift_acknowledged": True},
    "propagate": {"schema_policy": "propagate_columns"},
    "qa_both": {"schema_drift_acknowledged": True, "schema_policy": "propagate_columns"},
}


def _d193_setup(tmp_path):
    _require("postgresql")
    table = f"r4w193_{uuid.uuid4().hex[:6]}"
    _sql("postgresql", tmp_path, f"CREATE TABLE {table} (id INT PRIMARY KEY, n INT)")
    _sql("postgresql", tmp_path, f"CREATE TABLE {table}_d (id INT PRIMARY KEY, n INT)")
    _sql("postgresql", tmp_path, f"INSERT INTO {table} VALUES (1, 5), (2, NULL), (3, -2147483648)")
    return table


def _d193_args(tmp_path, table: str) -> dict:
    return dict(
        source_connector_id=_connector("postgresql", tmp_path),
        source_table=table,
        dest_connector_id=_connector("postgresql", tmp_path),
        dest_table=f"{table}_d",
        sync_mode="incremental_upsert",
        upsert_key="id",
        cursor_column="id",
        cursor_semantics="monotonic_sequence",
    )


def _widen_n(tmp_path, table: str) -> None:
    _sql("postgresql", tmp_path, f"ALTER TABLE {table} ALTER COLUMN n TYPE BIGINT")
    _sql("postgresql", tmp_path,
         f"INSERT INTO {table} VALUES (4, {OVER_INT}), (5, {BIG}), (6, NULL)")


@pytest.mark.parametrize("approval", sorted(APPROVALS))
def test_d193_int_to_bigint_widens_existing_pg_table_when_approved(
    transfer_client, tmp_path, approval
):
    table = _d193_setup(tmp_path)
    try:
        args = _d193_args(tmp_path, table)
        _run(transfer_client, **args)
        _widen_n(tmp_path, table)

        _run(transfer_client, **args, **APPROVALS[approval])

        assert _columns("postgresql", tmp_path, f"{table}_d")["n"][0] == "bigint"
        rows = _sql("postgresql", tmp_path, f"SELECT id, n FROM {table}_d ORDER BY id", fetch=True)
        assert rows == [(1, 5), (2, None), (3, -2147483648), (4, OVER_INT), (5, BIG), (6, None)]
    finally:
        _drop("postgresql", tmp_path, table, f"{table}_d")


def test_d193_unapproved_widen_blocks_plan_and_job_alike(transfer_client, tmp_path):
    table = _d193_setup(tmp_path)
    try:
        args = _d193_args(tmp_path, table)
        _run(transfer_client, **args)
        _widen_n(tmp_path, table)

        staged = _stage(**args)

        assert not staged.success
        # The headline names the class of change and the parameter that approves it.
        assert "Source type widening needs approval" in str(staged.error)
        assert "n INT4 → BIGINT" in str(staged.error)
        assert "schema_drift_acknowledged=true" in str(staged.error)
        assert "fidelity collapse" not in str(staged.error).lower()
        result = _execute_without_approval(
            staged, args["source_connector_id"], args["dest_connector_id"],
            table, f"{table}_d", "incremental_upsert",
        )
        assert not result.success
        assert _columns("postgresql", tmp_path, f"{table}_d")["n"][0] == "integer"
        assert _sql("postgresql", tmp_path, f"SELECT count(*) FROM {table}_d", fetch=True) == [(3,)]
    finally:
        _drop("postgresql", tmp_path, table, f"{table}_d")


# --------------------------------------------------------------------------- #
# D200: VARCHAR / NUMERIC / INT widening into the table DataFlow created
# --------------------------------------------------------------------------- #

ROUTES = [
    ("postgresql", "postgresql", "incremental_upsert"),
    ("postgresql", "mysql", "full_refresh_overwrite"),
    ("postgresql", "sqlserver", "full_refresh_overwrite"),
    ("mysql", "postgresql", "full_refresh_overwrite"),
    ("postgresql", "duckdb", "full_refresh_overwrite"),
]


def _route_id(route) -> str:
    return f"{route[0]}-{route[1]}-{route[2]}"


def _widen_source(kind: str, tmp_path, table: str) -> None:
    if kind == "postgresql":
        _sql(kind, tmp_path,
             f"ALTER TABLE {table} ALTER COLUMN qty TYPE BIGINT, "
             "ALTER COLUMN amt TYPE NUMERIC(18,4), ALTER COLUMN label TYPE VARCHAR(500)")
    else:
        _sql(kind, tmp_path,
             f"ALTER TABLE {table} MODIFY qty BIGINT, MODIFY amt DECIMAL(18,4), "
             "MODIFY label VARCHAR(500)")
    _sql(kind, tmp_path,
         f"INSERT INTO {table} VALUES (3, {OVER_INT}, 12345678901234.5678, %s), "
         f"(4, {BIG}, -99999999999999.9999, ''), (5, NULL, NULL, NULL)",
         ("é" + "x" * 499,))


EXPECTED_ROWS = [
    (1, 2147483647, Decimal("12345678.12"), "a"),
    (2, None, None, None),
    (3, OVER_INT, Decimal("12345678901234.5678"), "é" + "x" * 499),
    (4, BIG, Decimal("-99999999999999.9999"), ""),
    (5, None, None, None),
]


@pytest.mark.parametrize("route", ROUTES, ids=_route_id)
def test_d200_widened_source_lands_in_dataflow_created_table(transfer_client, tmp_path, route):
    src_kind, dst_kind, mode = route
    _require(src_kind)
    _require(dst_kind)
    table = f"r4w200_{uuid.uuid4().hex[:6]}"
    _sql(src_kind, tmp_path,
         f"CREATE TABLE {table} (id INT PRIMARY KEY, qty INT, amt DECIMAL(10,2), label VARCHAR(50))")
    _sql(src_kind, tmp_path,
         f"INSERT INTO {table} VALUES (1, 2147483647, 12345678.12, 'a'), (2, NULL, NULL, NULL)")
    args = dict(
        source_connector_id=_connector(src_kind, tmp_path),
        source_table=table,
        dest_connector_id=_connector(dst_kind, tmp_path),
        dest_table=f"{table}_d",
        sync_mode=mode,
    )
    if mode == "incremental_upsert":
        args.update(upsert_key="id", cursor_column="id", cursor_semantics="monotonic_sequence")
    try:
        _run(transfer_client, **args)  # run 1 creates the destination
        created = _columns(dst_kind, tmp_path, f"{table}_d")
        _widen_source(src_kind, tmp_path, table)

        # Blocked: the plan names the widening and Execute refuses the same plan.
        staged = _stage(**args)
        assert not staged.success
        assert "Source type widening needs approval" in str(staged.error)
        assert "fidelity collapse" not in str(staged.error).lower()
        assert "does not exist yet" not in str(staged.error)
        blocked = _execute_without_approval(
            staged, args["source_connector_id"], args["dest_connector_id"],
            table, f"{table}_d", mode,
        )
        assert not blocked.success
        assert _columns(dst_kind, tmp_path, f"{table}_d") == created

        # Approved once: plan green, job completes, destination widened.
        _run(transfer_client, **args, schema_drift_acknowledged=True)

        cols = _columns(dst_kind, tmp_path, f"{table}_d")
        assert (cols["amt"][2], cols["amt"][3]) == (18, 4), cols["amt"]
        assert cols["qty"][0] in {"bigint", "int8"}, cols["qty"]
        if dst_kind != "duckdb":  # DuckDB VARCHAR is unbounded from run 1
            assert cols["label"][1] == 500, cols["label"]
        if dst_kind == "sqlserver":
            # The widen keeps the destination's Unicode carrier and collation.
            assert cols["label"][0] == "nvarchar", cols["label"]
            assert cols["label"][4] == created["label"][4], (cols["label"], created["label"])
        if dst_kind == "mysql":
            assert cols["label"][4] == created["label"][4], (cols["label"], created["label"])
        rows = _sql(dst_kind, tmp_path,
                    f"SELECT id, qty, amt, label FROM {table}_d ORDER BY id", fetch=True)
        assert [
            (r[0], r[1], None if r[2] is None else Decimal(r[2]).normalize(), r[3])
            for r in rows
        ] == [
            (e[0], e[1], None if e[2] is None else e[2].normalize(), e[3])
            for e in EXPECTED_ROWS
        ]
    finally:
        _drop(dst_kind, tmp_path, f"{table}_d")
        _drop(src_kind, tmp_path, table)


# --------------------------------------------------------------------------- #
# D68 (bare NUMERIC sized by declaration) and D202 (PG TIMESTAMP -> DuckDB)
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("dst_kind", ["postgresql", "mysql", "sqlserver", "duckdb"])
def test_d68_d202_bare_numeric_and_naive_timestamp_survive_run_two(
    transfer_client, tmp_path, dst_kind
):
    _require("postgresql")
    _require(dst_kind)
    table = f"r4w68_{uuid.uuid4().hex[:6]}"
    _sql("postgresql", tmp_path, f"CREATE TABLE {table} (id INT PRIMARY KEY, v NUMERIC, ts TIMESTAMP)")
    _sql("postgresql", tmp_path,
         f"INSERT INTO {table} VALUES (1, 1.5, '2024-01-02 03:04:05.123456'), "
         "(2, NULL, NULL), (3, 123.456789, '1999-12-31 23:59:59')")
    args = dict(
        source_connector_id=_connector("postgresql", tmp_path),
        source_table=table,
        dest_connector_id=_connector(dst_kind, tmp_path),
        dest_table=f"{table}_d",
        sync_mode="full_refresh_overwrite",
    )
    try:
        _run(transfer_client, **args)  # D202: not blocked as TIMESTAMP_NTZ -> TIMESTAMP
        v = _columns(dst_kind, tmp_path, f"{table}_d")["v"]
        # Sized by the declaration, never by run 1's rows (DECIMAL(9,6)).
        assert v[2] is None or v[2] >= 38, v
        _sql("postgresql", tmp_path,
             f"INSERT INTO {table} VALUES (4, 12345678901234567890.123456, '2024-06-01 00:00:00')")

        _run(transfer_client, **args)

        rows = _sql(dst_kind, tmp_path, f"SELECT id, v, ts FROM {table}_d ORDER BY id", fetch=True)
        assert [r[0] for r in rows] == [1, 2, 3, 4]
        assert [None if r[1] is None else Decimal(r[1]).normalize() for r in rows] == [
            Decimal("1.5"), None, Decimal("123.456789"),
            Decimal("12345678901234567890.123456"),
        ]
        assert [None if r[2] is None else str(r[2])[:26] for r in rows] == [
            "2024-01-02 03:04:05.123456", None, "1999-12-31 23:59:59", "2024-06-01 00:00:00",
        ]
    finally:
        _drop(dst_kind, tmp_path, f"{table}_d")
        _drop("postgresql", tmp_path, table)


# --------------------------------------------------------------------------- #
# D191: the refusal headline names the gate that failed
# --------------------------------------------------------------------------- #

def _headline(error: str) -> str:
    return str(error).split(" — ", 1)[-1].split(":", 1)[0]


def test_d191_null_or_empty_key_names_g9(transfer_client, tmp_path):
    _require("postgresql")
    table = f"r4w191_{uuid.uuid4().hex[:6]}"
    _sql("postgresql", tmp_path, f"CREATE TABLE {table} (code TEXT, v INT)")
    _sql("postgresql", tmp_path, f"INSERT INTO {table} VALUES ('a', 1), ('', 2), (NULL, 3)")
    try:
        staged = _stage(
            source_connector_id=_connector("postgresql", tmp_path), source_table=table,
            dest_connector_id=_connector("postgresql", tmp_path), dest_table=f"{table}_d",
            sync_mode="incremental_upsert", upsert_key="code", cursor_column="v",
            cursor_semantics="monotonic_sequence",
        )
        assert not staged.success
        assert _headline(staged.error) == "g9_data_integrity", staged.error
        assert "null/empty" in str(staged.error)
        assert "fidelity collapse" not in str(staged.error).lower()
    finally:
        _drop("postgresql", tmp_path, table, f"{table}_d")


def test_d191_unreachable_destination_and_source_read_error_lead(transfer_client, tmp_path):
    _require("postgresql")
    _require("mysql")
    table = f"r4w191b_{uuid.uuid4().hex[:6]}"
    _sql("postgresql", tmp_path, f"CREATE TABLE {table} (id INT PRIMARY KEY, v INT)")
    _sql("postgresql", tmp_path, f"INSERT INTO {table} VALUES (1, 1)")
    _sql("postgresql", tmp_path, f"CREATE VIEW {table}_v AS SELECT id, 1 / (v - 1) AS r FROM {table}")
    src = _connector("postgresql", tmp_path)
    try:
        unreachable = _stage(
            source_connector_id=src, source_table=table,
            dest_connector_id=_connector("mysql", tmp_path, port=1),
            dest_table=table, sync_mode="full_refresh_overwrite",
        )
        assert not unreachable.success
        assert _headline(unreachable.error) == "g2_destination", unreachable.error
        assert "fidelity collapse" not in str(unreachable.error).lower()

        unreadable = _stage(
            source_connector_id=src, source_table=f"{table}_v",
            dest_connector_id=_connector("mysql", tmp_path),
            dest_table=f"{table}_v", sync_mode="full_refresh_overwrite",
        )
        assert not unreadable.success
        assert _headline(unreadable.error) == "g1_source", unreadable.error
        assert "fidelity collapse" not in str(unreadable.error).lower()
    finally:
        _sql("postgresql", tmp_path, f"DROP VIEW IF EXISTS {table}_v")
        _drop("postgresql", tmp_path, table)
        _drop("mysql", tmp_path, f"{table}_v", table)
