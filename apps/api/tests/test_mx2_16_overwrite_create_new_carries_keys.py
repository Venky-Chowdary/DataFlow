"""MX2-16 — full_refresh_overwrite into a new table recreates the source keys.

QA: PostgreSQL -> PostgreSQL ``full_refresh_overwrite`` into a new table left
``pk_assert: {"src": [["PRIMARY KEY", "id"]], "dst": []}``. f125e5df withheld
PK/UNIQUE for *append* create-new by testing ``write_mode != "insert"``, but
overwrite is also ``insert`` — so every overwrite create-new lost its keys.
"""

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_PG = dict(host="127.0.0.1", port=5432, user="dataflow", password="dataflow", dbname="dataflow")
_MYSQL_PORT = int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT") or 3307)


def _pg():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        conn = psycopg2.connect(**_PG)
    except Exception as exc:  # noqa: BLE001 — skip reason, not swallowed
        pytest.skip(f"PostgreSQL dataflow/dataflow not reachable on :5432: {exc}")
    conn.autocommit = True
    return conn


@pytest.fixture
def pg_source():
    conn = _pg()
    cur = conn.cursor()
    tag = uuid.uuid4().hex[:8]
    src = f"qa_e2e_rt_pk_src_{tag}"
    src2 = f"qa_e2e_rt_pk2_src_{tag}"
    cur.execute(
        f"CREATE TABLE {src} (id INTEGER PRIMARY KEY, code VARCHAR(20) NOT NULL UNIQUE, note TEXT)"
    )
    cur.execute(f"INSERT INTO {src} SELECT g, 'c' || g, NULL FROM generate_series(1, 50) g")
    # pk2: composite key, no UNIQUE (QA's second table).
    cur.execute(
        f"CREATE TABLE {src2} (region VARCHAR(8) NOT NULL, id INTEGER NOT NULL, "
        "amount NUMERIC(12,2), PRIMARY KEY (region, id))"
    )
    cur.execute(
        f"INSERT INTO {src2} SELECT CASE WHEN g % 2 = 0 THEN 'east' ELSE 'west' END, g, g * 1.5 "
        "FROM generate_series(1, 40) g"
    )
    made: list[str] = [src, src2]
    try:
        yield cur, src, src2, made
    finally:
        for t in made:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        conn.close()


def _engine(monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    import src.transfer.engine as engine_mod
    from src.transfer.engine import UniversalTransferEngine
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Jobs(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            return True

    jobs = _Jobs()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: jobs)

    def run(req):
        job_id = f"mx216-{uuid.uuid4().hex[:8]}"
        jobs.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(req, job_id)

    return run


def _pg_ep(table):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database", format="postgresql", host="127.0.0.1", port=5432,
        database="dataflow", username="dataflow", password="dataflow",
        table=table, schema="public", ssl=False,
    )


def _req(src_ep, dst_ep, cols, mode="full_refresh_overwrite"):
    from src.transfer.models import TransferRequest

    return TransferRequest(
        mappings=[{"source": c, "target": c, "confidence": 1.0} for c in cols],
        source=src_ep, destination=dst_ep, sync_mode=mode, validation_mode="balanced",
    )


def _pg_constraints(cur, table):
    cur.execute(
        "SELECT tc.constraint_type, kcu.column_name FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu USING (constraint_schema, constraint_name) "
        "WHERE tc.table_name = %s ORDER BY 1, kcu.ordinal_position",
        (table,),
    )
    return [tuple(r) for r in cur.fetchall()]


def _pg_not_null(cur, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s "
        "AND is_nullable = 'NO' ORDER BY ordinal_position",
        (table,),
    )
    return [r[0] for r in cur.fetchall()]


def test_live_pg_to_pg_overwrite_new_table_recreates_pk_and_unique(pg_source, monkeypatch):
    cur, src, _src2, made = pg_source
    dst = src.replace("_src_", "_dst_")
    made.append(dst)
    run = _engine(monkeypatch)
    for attempt in (1, 2):  # second run drops + recreates: keys must survive both
        result = run(_req(_pg_ep(src), _pg_ep(dst), ["id", "code", "note"]))
        assert result.success, (attempt, result.error)
        assert _pg_constraints(cur, dst) == [("PRIMARY KEY", "id"), ("UNIQUE", "code")], attempt
        assert _pg_not_null(cur, dst) == ["id", "code"]
        cur.execute(f"SELECT count(*) FROM {dst}")
        assert cur.fetchone()[0] == 50


def test_live_pg_to_pg_overwrite_composite_pk(pg_source, monkeypatch):
    cur, _src, src2, made = pg_source
    dst = src2.replace("_src_", "_dst_")
    made.append(dst)
    result = _engine(monkeypatch)(_req(_pg_ep(src2), _pg_ep(dst), ["region", "id", "amount"]))
    assert result.success, result.error
    assert _pg_constraints(cur, dst) == [("PRIMARY KEY", "region"), ("PRIMARY KEY", "id")]
    assert _pg_not_null(cur, dst) == ["region", "id"]


def test_live_pg_to_sqlite_overwrite_new_table_recreates_pk(pg_source, monkeypatch, tmp_path):
    from src.transfer.models import EndpointConfig

    _cur, src, _src2, _made = pg_source
    db = tmp_path / "d.sqlite"
    dst_ep = EndpointConfig(kind="database", format="sqlite", database=str(db), table="pk_dst")
    result = _engine(monkeypatch)(_req(_pg_ep(src), dst_ep, ["id", "code", "note"]))
    assert result.success, result.error
    with sqlite3.connect(db) as c:
        info = {r[1]: (r[3], r[5]) for r in c.execute('PRAGMA table_info("pk_dst")')}
        uniq = [
            [r[2] for r in c.execute(f'PRAGMA index_info("{ix[1]}")')]
            for ix in c.execute('PRAGMA index_list("pk_dst")')
            if ix[2] and ix[3] == "u"
        ]
    assert info["id"][1] == 1  # PRIMARY KEY
    assert info["code"][0] == 1  # NOT NULL
    assert ["code"] in uniq


def test_live_pg_to_mysql_overwrite_new_table_recreates_pk(pg_source, monkeypatch):
    pymysql = pytest.importorskip("pymysql")
    from src.transfer.models import EndpointConfig

    try:
        my = pymysql.connect(host="127.0.0.1", port=_MYSQL_PORT, user="root",
                             password="dataflow", database="dataflow", autocommit=True)
    except Exception as exc:  # noqa: BLE001 — skip reason, not swallowed
        pytest.skip(f"MySQL not reachable on :{_MYSQL_PORT}: {exc}")
    _cur, src, _src2, _made = pg_source
    dst = f"mx216_{uuid.uuid4().hex[:8]}"
    dst_ep = EndpointConfig(kind="database", format="mysql", host="127.0.0.1", port=_MYSQL_PORT,
                            database="dataflow", username="root", password="dataflow",
                            table=dst, ssl=False)
    try:
        result = _engine(monkeypatch)(_req(_pg_ep(src), dst_ep, ["id", "code", "note"]))
        assert result.success, result.error
        mc = my.cursor()
        mc.execute(
            "SELECT tc.constraint_type, kcu.column_name FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tc.constraint_name "
            "AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name "
            "WHERE tc.table_schema = 'dataflow' AND tc.table_name = %s ORDER BY 1, 2",
            (dst,),
        )
        assert [tuple(r) for r in mc.fetchall()] == [("PRIMARY KEY", "id"), ("UNIQUE", "code")]
    finally:
        my.cursor().execute(f"DROP TABLE IF EXISTS {dst}")
        my.close()


def test_live_append_create_new_still_withholds_keys(pg_source, monkeypatch):
    """f125e5df's contract: append create-new declares no key (a re-load is legal)."""
    cur, src, _src2, made = pg_source
    dst = src.replace("_src_", "_app_")
    made.append(dst)
    run = _engine(monkeypatch)
    first = run(_req(_pg_ep(src), _pg_ep(dst), ["id", "code", "note"], mode="full_refresh_append"))
    assert first.success, first.error
    assert _pg_constraints(cur, dst) == []
