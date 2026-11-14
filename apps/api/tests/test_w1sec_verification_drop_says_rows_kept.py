"""REL-4: a connection lost during verification says how many rows were kept, and where.

A PG -> PG run whose destination connection was cut during reconcile failed
with the raw driver text (host and port included) and never said that the
1.5M rows it had committed stayed in the table.
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from tests.test_r13_d284_pg_readback_failure_is_named_and_retried import (  # noqa: E402
    _fail_full_readback,
    _landed,
    _Mongo,
    _run,
)


@pytest.fixture()
def tables(monkeypatch, tmp_path):
    try:
        import psycopg2

        conn = psycopg2.connect(host="localhost", port=5432, database="dataflow", user="dataflow",
                                password="dataflow", connect_timeout=3)
        conn.autocommit = True
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    for prefix in ("DATAFLOW_", "DATAWRAP_"):
        monkeypatch.setenv(f"{prefix}RETRY_BASE_DELAY_SECONDS", "0.01")
        monkeypatch.setenv(f"{prefix}RETRY_JITTER", "false")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _Mongo())
    src = str(tmp_path / "src.db")
    lite = sqlite3.connect(src)
    lite.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER, label TEXT)")
    lite.executemany("INSERT INTO t VALUES (?,?,?)", [(i, i, f"r{i}") for i in range(1, 6)])
    lite.commit()
    lite.close()
    dst = f"w1sec_rel4_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{dst}" (id int PRIMARY KEY, n int, label text)')
    yield conn, src, dst
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{dst}"')
    conn.close()


def test_a_lost_connection_during_verification_names_the_rows_kept(tables, monkeypatch):
    conn, src, dst = tables
    import psycopg2

    _fail_full_readback(monkeypatch, times=99, error=psycopg2.OperationalError(
        'connection to server at "10.20.30.40", port 18938 failed: server closed the connection unexpectedly'))
    result = _run(src, dst)
    err = result.error or ""
    assert result.success is False
    assert len(_landed(conn, dst)) == 5
    assert "5 row(s) were written" in err and dst in err and "kept" in err, err
    assert "10.20.30.40" not in err and "18938" not in err and "OperationalError" not in err, err
