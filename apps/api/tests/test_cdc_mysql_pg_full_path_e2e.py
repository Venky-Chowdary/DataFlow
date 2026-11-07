"""MySQL ``cdc_incremental`` -> new PostgreSQL table, the whole engine path.

QA's flow: saved connectors, ``start_transfer`` -> Confirm, ``sync_mode=
cdc_incremental``, ``validation_mode=strict``, ``source_timezone=UTC``,
preflight on. The earlier D174/D177/D178 tests called the CDC executor
directly; this drives the same route a user does (the MCP tool + the confirm
route, and ``UniversalTransferEngine().execute_tracked``) over a table with
every type QA used, then changes the source (UPDATE / INSERT / DELETE),
re-runs, and compares PostgreSQL to MySQL cell by cell.

Live: MySQL (ROW binlog, replication grants) on 3306 and PostgreSQL on 5432;
skipped when either is down.
"""

from __future__ import annotations

import json
import socket
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

MYSQL = {
    "type": "mysql", "host": "127.0.0.1", "port": 3306, "database": "dataflow",
    "username": "dataflow", "password": "dataflow", "connection_string": "", "ssl": False,
}
PG = {
    "type": "postgresql", "host": "127.0.0.1", "port": 5432, "database": "dataflow",
    "username": "dataflow", "password": "dataflow", "schema": "public",
}

NESTED_JSON = '{"a": [1, [2, {"k": "v\\"q"}], "x"], "s": "he said \\"hi\\"", "n": null, "b": true}'
LITERAL = "__DF_SQL_NULL__"


def _mysql():
    import pymysql

    return pymysql.connect(
        host=MYSQL["host"], port=MYSQL["port"], user=MYSQL["username"],
        password=MYSQL["password"], database=MYSQL["database"], autocommit=True,
        connect_timeout=3,
    )


def _pg():
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"], port=PG["port"], dbname=PG["database"], user=PG["username"],
        password=PG["password"], connect_timeout=3,
    )
    conn.autocommit = True
    return conn


def _live_ready() -> bool:
    try:
        import pymysqlreplication  # noqa: F401

        for port in (3306, 5432):
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                pass
        conn = _mysql()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW VARIABLES LIKE 'binlog_format'")
                row = cur.fetchone()
                if not (row and str(row[1]).upper() == "ROW"):
                    return False
        finally:
            conn.close()
        _pg().close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _live_ready(), reason="MySQL ROW binlog or PostgreSQL not reachable"
)


@pytest.fixture(autouse=True)
def _engine_env(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.delenv("DATAFLOW_ENGINE_DIGEST", raising=False)


def _mysql_exec(*statements) -> None:
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                if isinstance(stmt, tuple):
                    cur.execute(*stmt)
                else:
                    cur.execute(stmt)
    finally:
        conn.close()


def _create_source(table: str) -> None:
    _mysql_exec(
        f"DROP TABLE IF EXISTS `{table}`",
        f"""CREATE TABLE `{table}` (
            id INT PRIMARY KEY, amount DECIMAL(12,2), qty INT, flag TINYINT(1),
            d DATE, dt DATETIME, ts TIMESTAMP NULL, dt6 DATETIME(6), doc JSON,
            note VARCHAR(100)) CHARACTER SET utf8mb4""",
        (
            f"INSERT INTO `{table}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (1, "10.50", 5, 1, "2024-01-02", "2024-01-02 03:04:05", "2024-01-02 03:04:05",
             "2024-01-02 03:04:05.123456", '{"k": 1}', "plain"),
        ),
        (
            f"INSERT INTO `{table}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (2, None, None, None, None, None, None, None, None, None),
        ),
        (
            f"INSERT INTO `{table}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (3, "-0.01", 0, 0, "1999-12-31", "1999-12-31 23:59:59", "2001-09-09 01:46:40",
             "1999-12-31 23:59:59.000001", NESTED_JSON, "café über\t中文 \U0001f600"),
        ),
        (
            f"INSERT INTO `{table}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (4, "7.00", 7, 1, "2020-02-29", "2020-02-29 12:00:00", "2020-02-29 12:00:00",
             "2020-02-29 12:00:00.5", "[1, [2, [3]], \"q\"]", LITERAL),
        ),
    )


def _mysql_cells(table: str) -> dict:
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, amount, qty, flag, d, dt, ts, dt6, CAST(doc AS CHAR), note "
                f"FROM `{table}` ORDER BY id"
            )
            return {r[0]: r for r in cur.fetchall()}
    finally:
        conn.close()


def _pg_cells(table: str) -> dict:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            cur.execute(f'SELECT * FROM public."{table}" ORDER BY id')
            names = [d[0] for d in cur.description]
            return {r[0]: dict(zip(names, r)) for r in cur.fetchall()}
    finally:
        conn.close()


def _utc(v):
    if v is None:
        return None
    return v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo else v


def _json_equal(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    pa = a if not isinstance(a, (str, bytes)) else json.loads(a)
    pb = b if not isinstance(b, (str, bytes)) else json.loads(b)
    return pa == pb


def assert_cells_equal(table: str, dest_table: str) -> None:
    src, dst = _mysql_cells(table), _pg_cells(dest_table)
    assert set(src) == set(dst), (sorted(src), sorted(dst))
    for pk, s in src.items():
        d = dst[pk]
        got = (
            d["amount"], d["qty"], d["flag"], d["d"], _utc(d["dt"]), _utc(d["ts"]),
            _utc(d["dt6"]), d["note"],
        )
        want_flag = None if s[3] is None else s[3]
        want = (
            s[1] if s[1] is None else Decimal(s[1]), s[2],
            want_flag if not isinstance(d["flag"], bool) else (None if s[3] is None else bool(s[3])),
            s[4], s[5], s[6], s[7], s[9],
        )
        assert got == want, (pk, got, want)
        assert _json_equal(s[8], d["doc"]), (pk, s[8], d["doc"])


# ---------------------------------------------------------------------------
# Saved connectors + the MCP route
# ---------------------------------------------------------------------------


@pytest.fixture
def connectors():
    from services.connector_store import create_connector, delete_connector

    sfx = uuid.uuid4().hex[:6]
    made = []
    out = {}
    for key, cfg in (("mysql", MYSQL), ("pg", PG)):
        saved = create_connector({**cfg, "name": f"CDCE2E{key}{sfx}"})
        made.append(saved.id)
        out[key] = f"CDCE2E{key}{sfx}"
    try:
        yield out
    finally:
        for cid in made:
            delete_connector(cid)


def _wait_job(job_id: str, timeout: float = 180.0) -> dict:
    from src.ai.copilot.tools import DataPilotTools

    deadline = time.time() + timeout
    job: dict = {}
    while time.time() < deadline:
        res = DataPilotTools().execute("get_job", {"job_id": job_id})
        job = dict(res.output or {})
        status = str(job.get("status") or (job.get("job") or {}).get("status") or "")
        if status and status not in {"pending", "queued", "running", "starting"}:
            return job
        time.sleep(1.0)
    return job


def _start(connectors, src_table, dst_table) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute("start_transfer", {
        "source_connector_name": connectors["mysql"],
        "source_table": src_table,
        "dest_connector_name": connectors["pg"],
        "dest_table": dst_table,
        "sync_mode": "cdc_incremental",
        "validation_mode": "strict",
        "source_timezone": "UTC",
    })
    assert staged.success, staged.error
    confirmed = spend_pilot_ack(staged.output["ack_id"], "cdc-e2e")
    assert confirmed["ok"] is True, confirmed
    return _wait_job(confirmed["job_id"])


def _mutate(table: str) -> None:
    """UPDATE / INSERT / DELETE after the snapshot: the streamed leg."""
    _mysql_exec(
        f"UPDATE `{table}` SET amount=99.99, note='upd', doc='{{\"z\": [1, 2]}}' WHERE id=1",
        f"UPDATE `{table}` SET note=NULL, doc=NULL WHERE id=3",
        (f"UPDATE `{table}` SET note=%s, doc=%s WHERE id=4", (LITERAL, NESTED_JSON)),
        (
            f"INSERT INTO `{table}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (5, "1.10", 1, 0, "2021-01-01", "2021-01-01 00:00:01", "2021-01-01 00:00:01",
             "2021-01-01 00:00:01.000002", NESTED_JSON, LITERAL),
        ),
        (
            f"INSERT INTO `{table}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (6, None, None, None, None, None, None, None, None, None),
        ),
        f"DELETE FROM `{table}` WHERE id=2",
    )


def test_mcp_cdc_snapshot_then_stream_every_cell_equal(connectors):
    """start_transfer -> Confirm, twice, as QA ran it."""
    table = "cdce2e_" + uuid.uuid4().hex[:8]
    dst = table + "_d"
    _create_source(table)
    conn = _pg()
    try:
        conn.cursor().execute(f'DROP TABLE IF EXISTS public."{dst}"')
        job = _start(connectors, table, dst)
        assert job.get("status") == "completed", job
        assert_cells_equal(table, dst)
        _mutate(table)
        job2 = _start(connectors, table, dst)
        assert job2.get("status") == "completed", job2
        assert_cells_equal(table, dst)
    finally:
        _mysql_exec(f"DROP TABLE IF EXISTS `{table}`")
        conn.cursor().execute(f'DROP TABLE IF EXISTS public."{dst}"')
        conn.close()


# ---------------------------------------------------------------------------
# UniversalTransferEngine().execute_tracked
# ---------------------------------------------------------------------------


@pytest.fixture()
def fake_store(monkeypatch):
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


def _run_engine(fake, table: str, dst: str, delivery: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    src_cfg = {k: v for k, v in MYSQL.items() if k != "type"}
    dst_cfg = {k: v for k, v in PG.items() if k != "type"}
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="mysql", table=table, **src_cfg),
        destination=EndpointConfig(kind="database", format="postgresql", table=dst, **dst_cfg),
        sync_mode="cdc_incremental",
        validation_mode="strict",
        source_timezone="UTC",
        delivery_guarantee=delivery,
        stream_contracts=[{
            "name": table, "selected": True, "primary_key": ["id"],
            "cursor_semantics": "cdc_position", "sync_mode": "cdc",
            "snapshot_mode": "initial",
        }],
    )
    job_id = "cdce2e" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


@pytest.mark.parametrize("delivery", ["auto", "at_least_once"])
def test_execute_tracked_cdc_snapshot_then_stream_every_cell_equal(fake_store, delivery):
    table = "cdce2e_" + uuid.uuid4().hex[:8]
    dst = table + "_d"
    _create_source(table)
    conn = _pg()
    try:
        conn.cursor().execute(f'DROP TABLE IF EXISTS public."{dst}"')
        first = _run_engine(fake_store, table, dst, delivery)
        assert first.success, first.error
        delivered = (first.destination_summary or {}).get("cdc_delivery")
        assert delivered == ("exactly_once" if delivery == "auto" else "at-least-once"), delivered
        assert_cells_equal(table, dst)
        _mutate(table)
        second = _run_engine(fake_store, table, dst, delivery)
        assert second.success, second.error
        streamed = (second.destination_summary or {}).get("cdc") or {}
        assert (streamed.get("inserts"), streamed.get("updates"), streamed.get("deletes")) == (
            2, 3, 1,
        ), streamed  # the binlog leg, not a second snapshot
        assert_cells_equal(table, dst)
        third = _run_engine(fake_store, table, dst, delivery)  # quiet poll
        assert third.success, third.error
        assert_cells_equal(table, dst)
    finally:
        _mysql_exec(f"DROP TABLE IF EXISTS `{table}`")
        conn.cursor().execute(f'DROP TABLE IF EXISTS public."{dst}"')
        conn.close()
