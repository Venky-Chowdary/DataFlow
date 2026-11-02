"""D205 - a quarantine append into a table that already holds rows.

QA on 557f556c: an append under ``QUARANTINE_ROW`` into a populated table lands
every good row and quarantines the bad ones, yet the job ends ``failed``; the
quarantine replay is then refused because its parent "did not commit". Seen
PG -> PG, MySQL -> PG and from a Timescale source.

What went wrong: Gate-8 itself proved the batch on the written keys
(``checksum_scope = written_batch_keys``). The verification ladder that runs
after it compared the source batch against the *whole* destination table.
Without quarantined rows the ladder reused Gate-8's keyed digests, so a clean
append passed; with quarantined rows it threw those digests away (they still
hashed the held-out rows) and recomputed both sides - the batch minus the
quarantined keys against every destination row, including the rows earlier
runs wrote. L3 could never match, L4/L5 "localized" the first pre-existing row,
and the run failed; the doctrine for a failed append into a populated table is
to keep the batch, so the good rows stayed while the job said ``failed``.

Every test drives the real route: MCP ``start_transfer`` -> confirm -> job, then
the MCP ``replay_quarantine`` -> confirm, against live PostgreSQL (and MySQL).
No TimescaleDB extension is installed locally, so the Timescale leg uses a
``timescaledb`` connector over a range-partitioned PostgreSQL table with a
composite ``(id, ts)`` key - the shape a hypertable forces on its unique keys.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import pytest

from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up

pytestmark = pytest.mark.skipif(not pg_up(), reason="PostgreSQL not reachable")

# The bad cells are 3.7 and 5.5 into an INTEGER column. The text column carries
# NULL, '', whitespace and unicode, on good and on quarantined rows alike.
_SOURCE = [
    (1, 1.0, None),
    (2, 2.0, ""),
    (3, 3.7, "  "),
    (4, 4.0, "é中\U0001f600"),
    (5, 5.5, None),
    (6, 6.0, " lead"),
]
_PRIOR = [(100, 100, "prior"), (101, 101, None), (102, 102, "")]


def _pg_conn():
    import psycopg2

    cfg = pg_creds()
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
        user=cfg["username"], password=cfg["password"],
    )
    conn.autocommit = True
    return conn


def _pg_connector(name: str, kind: str = "postgresql") -> Any:
    from services.connector_store import create_connector

    cfg = pg_creds()
    return create_connector({
        "name": name, "type": kind, "host": cfg["host"], "port": cfg["port"],
        "database": cfg["database"], "username": cfg["username"],
        "password": cfg["password"], "schema": "public", "ssl": False,
    })


def _status(job_id: str) -> dict:
    from src.ai.copilot.tools import DataPilotTools

    tools = DataPilotTools()
    deadline = time.time() + 180
    job: dict = {}
    while time.time() < deadline:
        job = dict(tools.execute("get_job", {"job_id": job_id}).output or {})
        if job.get("status") not in {None, "", "pending", "queued", "running", "starting"}:
            break
        time.sleep(1.0)
    return job


def _append(src_name: str, src_table: str, dst_name: str, dst_table: str,
            *, quarantine: bool = True) -> tuple[str, dict]:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    args: dict[str, Any] = {
        "source_connector_name": src_name, "source_table": src_table,
        "dest_connector_name": dst_name, "dest_table": dst_table,
        "sync_mode": "full_refresh_append", "validation_mode": "lenient",
    }
    if quarantine:
        args["risk_acceptance"] = {
            "execution_policy": "QUARANTINE_ROW", "approved_by": "qa", "reason": "accepted",
        }
    staged = DataPilotTools().execute("start_transfer", args)
    assert staged.success, staged.error
    job_id = spend_pilot_ack(staged.output["ack_id"], "d205-test")["job_id"]
    return job_id, _status(job_id)


def _replay(job_id: str) -> dict:
    from conftest import spend_pilot_ack
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute("replay_quarantine", {"job_id": job_id})
    assert staged.success, staged.error
    return spend_pilot_ack(staged.output["ack_id"], "d205-test")


def _job_text(job: dict) -> str:
    return str(job.get("error") or job.get("message") or job)[:600]


def _assert_quarantined_append(job_id: str, job: dict) -> None:
    assert job.get("status") == "completed_with_quarantine", _job_text(job)


def _assert_replay_closes(job_id: str) -> None:
    from services.mongodb_service import get_mongodb_service
    from services.quarantine_dlq import job_quarantine_closure

    out = _replay(job_id)
    assert out["rows_written"] == 2 and out["rejected"] == 0, out
    assert out["status"] == "completed", (out.get("reconciliation") or {}).get("message")
    assert out["quarantine_closure"]["verdict"] == "closed", out["quarantine_closure"]
    assert out["quarantine_closure"]["open_count"] == 0
    parent = get_mongodb_service().get_job(job_id)
    assert (job_quarantine_closure(parent) or {}).get("verdict") == "closed"


def _dest_rows(conn, table: str, cols: str = "id, flt, note") -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(f'SELECT {cols} FROM "{table}" ORDER BY id')
        return cur.fetchall()


def _drop(conn, *tables: str) -> None:
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
            cur.execute(f'DROP TABLE IF EXISTS "{t}_df_quarantine"')


@pytest.fixture
def pg_route():
    from services.connector_store import delete_connector

    sfx = uuid.uuid4().hex[:6]
    src, dst = f"d205_src_{sfx}", f"d205_dst_{sfx}"
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src} (id INT PRIMARY KEY, flt DOUBLE PRECISION, note TEXT)")
        cur.executemany(f"INSERT INTO {src} VALUES (%s,%s,%s)", _SOURCE)
        cur.execute(f"CREATE TABLE {dst} (id INT PRIMARY KEY, flt INTEGER, note TEXT)")
        cur.executemany(f"INSERT INTO {dst} VALUES (%s,%s,%s)", _PRIOR)
    saved = _pg_connector(f"D205pg{sfx}")
    try:
        yield {"name": saved.name, "src": src, "dst": dst, "conn": conn}
    finally:
        _drop(conn, src, dst)
        conn.close()
        delete_connector(saved.id)


def _widen(conn, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE "{table}" ALTER COLUMN flt TYPE DOUBLE PRECISION')


def test_pg_to_pg_quarantine_append_into_populated_completes_and_replays(pg_route):
    job_id, job = _append(pg_route["name"], pg_route["src"], pg_route["name"], pg_route["dst"])
    _assert_quarantined_append(job_id, job)
    conn = pg_route["conn"]
    assert _dest_rows(conn, pg_route["dst"]) == [
        (1, 1, None), (2, 2, ""), (4, 4, "é中\U0001f600"), (6, 6, " lead"), *_PRIOR,
    ]

    _widen(conn, pg_route["dst"])
    _assert_replay_closes(job_id)
    assert _dest_rows(conn, pg_route["dst"]) == [
        (1, 1.0, None), (2, 2.0, ""), (3, 3.7, "  "), (4, 4.0, "é中\U0001f600"),
        (5, 5.5, None), (6, 6.0, " lead"),
        *[(i, float(f), n) for i, f, n in _PRIOR],
    ]


def test_quarantine_append_into_the_table_dataflow_created(pg_route):
    """Run 1 creates the table; run 2 appends a batch with bad cells into it."""
    conn = pg_route["conn"]
    sfx = uuid.uuid4().hex[:6]
    first_src, created = f"d205_first_{sfx}", f"d205_created_{sfx}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {first_src} (id INT PRIMARY KEY, flt INTEGER, note TEXT)")
        cur.executemany(f"INSERT INTO {first_src} VALUES (%s,%s,%s)", _PRIOR)
    try:
        _, first = _append(pg_route["name"], first_src, pg_route["name"], created, quarantine=False)
        assert first.get("status") == "completed", _job_text(first)
        job_id, job = _append(pg_route["name"], pg_route["src"], pg_route["name"], created)
        _assert_quarantined_append(job_id, job)
        assert [r[0] for r in _dest_rows(conn, created)] == [1, 2, 4, 6, 100, 101, 102]

        _widen(conn, created)
        _assert_replay_closes(job_id)
        assert [r[:2] for r in _dest_rows(conn, created)] == [
            (1, 1.0), (2, 2.0), (3, 3.7), (4, 4.0), (5, 5.5), (6, 6.0),
            (100, 100.0), (101, 101.0), (102, 102.0),
        ]
    finally:
        _drop(conn, first_src, created)


@pytest.mark.parametrize("tamper", ["drop_row", "rewrite_cell"])
def test_a_real_loss_in_the_written_batch_still_fails(pg_route, tamper):
    """The window is the written batch: a good row lost or changed there fails."""
    conn = pg_route["conn"]
    fn = f"{pg_route['dst']}_tamper"
    body = (
        "IF NEW.id = 4 THEN RETURN NULL; END IF; RETURN NEW;"
        if tamper == "drop_row"
        else "IF NEW.id = 6 THEN NEW.note := 'tampered'; END IF; RETURN NEW;"
    )
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE FUNCTION {fn}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {body} END $$"
        )
        cur.execute(
            f"CREATE TRIGGER {fn} BEFORE INSERT ON {pg_route['dst']} "
            f"FOR EACH ROW EXECUTE FUNCTION {fn}()"
        )
    try:
        _, job = _append(pg_route["name"], pg_route["src"], pg_route["name"], pg_route["dst"])
        assert job.get("status") == "failed", _job_text(job)
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {pg_route['dst']} CASCADE")
            cur.execute(f"DROP FUNCTION IF EXISTS {fn}() CASCADE")


@pytest.mark.parametrize("tamper", [None, "drop_row"])
def test_keyless_destination_proves_the_append_by_its_delta(pg_route, tamper):
    """No enforced key: the written batch is proved by the dest-before delta."""
    conn = pg_route["conn"]
    dst = pg_route["dst"]
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE {dst}")
        cur.execute(f"CREATE TABLE {dst} (id INT, flt INTEGER, note TEXT)")
        cur.executemany(f"INSERT INTO {dst} VALUES (%s,%s,%s)", _PRIOR)
        if tamper:
            cur.execute(
                f"CREATE FUNCTION {dst}_t() RETURNS trigger LANGUAGE plpgsql AS "
                "$$ BEGIN IF NEW.id = 4 THEN RETURN NULL; END IF; RETURN NEW; END $$"
            )
            cur.execute(
                f"CREATE TRIGGER {dst}_t BEFORE INSERT ON {dst} "
                f"FOR EACH ROW EXECUTE FUNCTION {dst}_t()"
            )
    try:
        job_id, job = _append(pg_route["name"], pg_route["src"], pg_route["name"], dst)
        if tamper:
            assert job.get("status") == "failed", _job_text(job)
        else:
            _assert_quarantined_append(job_id, job)
            assert [r[0] for r in _dest_rows(conn, dst)] == [1, 2, 4, 6, 100, 101, 102]
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {dst} CASCADE")
            cur.execute(f"DROP FUNCTION IF EXISTS {dst}_t() CASCADE")


@pytest.mark.skipif(not mysql_up(), reason="MySQL not reachable")
def test_mysql_to_pg_quarantine_append_into_populated_completes_and_replays(pg_route):
    import pymysql

    from services.connector_store import create_connector, delete_connector

    my = mysql_creds()
    sfx = uuid.uuid4().hex[:6]
    src = f"d205_my_{sfx}"
    mconn = pymysql.connect(
        host=my["host"], port=my["port"], user=my["username"], password=my["password"],
        database=my["database"], autocommit=True, charset="utf8mb4",
    )
    with mconn.cursor() as cur:
        cur.execute(
            f"CREATE TABLE {src} (id INT PRIMARY KEY, flt DOUBLE, note VARCHAR(40)) "
            "CHARACTER SET utf8mb4"
        )
        cur.executemany(f"INSERT INTO {src} VALUES (%s,%s,%s)", _SOURCE)
    saved = create_connector({
        "name": f"D205my{sfx}", "type": "mysql", "host": my["host"], "port": my["port"],
        "database": my["database"], "username": my["username"], "password": my["password"],
    })
    try:
        job_id, job = _append(saved.name, src, pg_route["name"], pg_route["dst"])
        _assert_quarantined_append(job_id, job)
        conn = pg_route["conn"]
        assert [r[0] for r in _dest_rows(conn, pg_route["dst"])] == [1, 2, 4, 6, 100, 101, 102]

        _widen(conn, pg_route["dst"])
        _assert_replay_closes(job_id)
        assert _dest_rows(conn, pg_route["dst"])[:6] == [
            (1, 1.0, None), (2, 2.0, ""), (3, 3.7, "  "), (4, 4.0, "é中\U0001f600"),
            (5, 5.5, None), (6, 6.0, " lead"),
        ]
    finally:
        with mconn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {src}")
        mconn.close()
        delete_connector(saved.id)


@pytest.fixture
def hypertable_route():
    """A ``timescaledb`` source over a range-partitioned table (hypertable stand-in)."""
    from services.connector_store import delete_connector

    sfx = uuid.uuid4().hex[:6]
    src, dst = f"d205_ts_src_{sfx}", f"d205_ts_dst_{sfx}"
    conn = _pg_conn()
    with conn.cursor() as cur:
        for table, flt in ((src, "DOUBLE PRECISION"), (dst, "INTEGER")):
            cur.execute(
                f"CREATE TABLE {table} (id INT NOT NULL, ts TIMESTAMPTZ NOT NULL, "
                f"flt {flt}, note TEXT, PRIMARY KEY (id, ts)) PARTITION BY RANGE (ts)"
            )
            cur.execute(
                f"CREATE TABLE {table}_p1 PARTITION OF {table} "
                "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
            )
            cur.execute(
                f"CREATE TABLE {table}_p2 PARTITION OF {table} "
                "FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')"
            )
        cur.executemany(
            f"INSERT INTO {src} VALUES (%s, %s, %s, %s)",
            [(i, f"2026-0{1 + i % 2}-0{i}T00:00:00Z", f, n) for i, f, n in _SOURCE],
        )
        cur.executemany(
            f"INSERT INTO {dst} VALUES (%s, %s, %s, %s)",
            [(i, "2026-01-15T00:00:00Z", f, n) for i, f, n in _PRIOR],
        )
    ts_conn = _pg_connector(f"D205ts{sfx}", kind="timescaledb")
    pg_conn = _pg_connector(f"D205tsd{sfx}")
    try:
        yield {"src_name": ts_conn.name, "dst_name": pg_conn.name, "src": src, "dst": dst,
               "conn": conn}
    finally:
        _drop(conn, src, dst)
        conn.close()
        delete_connector(ts_conn.id)
        delete_connector(pg_conn.id)


def test_timescale_like_source_quarantine_append_into_populated(hypertable_route):
    r = hypertable_route
    job_id, job = _append(r["src_name"], r["src"], r["dst_name"], r["dst"])
    _assert_quarantined_append(job_id, job)
    conn = r["conn"]
    assert [row[0] for row in _dest_rows(conn, r["dst"], "id")] == [1, 2, 4, 6, 100, 101, 102]

    _widen(conn, r["dst"])
    _assert_replay_closes(job_id)
    assert _dest_rows(conn, r["dst"], "id, flt, note")[:6] == [
        (1, 1.0, None), (2, 2.0, ""), (3, 3.7, "  "), (4, 4.0, "é中\U0001f600"),
        (5, 5.5, None), (6, 6.0, " lead"),
    ]
