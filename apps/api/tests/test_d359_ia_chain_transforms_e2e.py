"""D359 — a transformed incremental_append chain is green, exact, and never re-appends.

QA r42 / r62-r75: incremental_append into a new keyless table, cursor
``updated_at``, ``propagate_columns``, transforms on ``name`` and ``note``. The
transform proof failed on the boundary key's two copies (r67: "Culprit key(s)
— changed: 31"), or the run stayed green over them.

Root cause: the inclusive seed re-reads the watermark's tie group and the
keyless boundary dedupe compared those raw source rows with the destination,
which holds the recipe's output. ``"  Name30 "`` never equals the stored
``"name30"``, so the watermark row was appended again on every run, and the
post-write duplicate check counted the same raw rows, so it never fired.
The peers are now shaped by the run's recipe before they are compared
(``stream_boundary.shape_boundary_peers``).

Legs: SQLite and PostgreSQL here; SQL Server in ``test_d335_boundary_sqlserver_live``
(cdc-sqlserver), Oracle in ``test_d359_ia_chain_oracle_live`` (oracle-live).
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402
from tests.helpers.d359_ia_chain_scenario import request, rows_by_name, run_chain, ts  # noqa: E402

pytestmark = pytest.mark.timeout(900)


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _sqlite_exec(path: Path):
    def _exec(sql: str, params: tuple = ()) -> None:
        conn = sqlite3.connect(path)
        try:
            conn.execute(sql.replace("%s", "?"), params)
            conn.commit()
        finally:
            conn.close()

    return _exec


def _sqlite_endpoint(path: Path):
    return lambda table: EndpointConfig(kind="database", format="sqlite", database=str(path), table=table)


def _sqlite_rows(path: Path):
    def _rows(table: str) -> list[tuple]:
        conn = sqlite3.connect(path)
        try:
            cur = conn.execute(f'SELECT * FROM "{table}"')  # nosec B608 - test-owned name
            return rows_by_name(cur.description, cur.fetchall())
        except sqlite3.OperationalError:
            return []
        finally:
            conn.close()

    return _rows


def _sqlite_drop(path: Path):
    return lambda table: _sqlite_exec(path)(f'DROP TABLE IF EXISTS "{table}"')


def _proof_ran(n: int, result) -> None:
    message = str((result.reconciliation or {}).get("message") or "")
    assert "transform output verified" in message, f"run {n}: transform proof did not run: {message}"


def test_sqlite_three_run_transformed_append_chain_is_exact(tmp_path, store):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    run_chain(
        store,
        src_exec=_sqlite_exec(src),
        src_endpoint=_sqlite_endpoint(src),
        dest_endpoint=_sqlite_endpoint(dst),
        dest_rows=_sqlite_rows(dst),
        drop_dest=_sqlite_drop(dst),
        cursor_type="TEXT",
        on_run=_proof_ran,
    )


def test_pg_to_pg_three_run_transformed_append_chain_is_exact(store):
    from tests.c3_live_helpers import pg_endpoint, pg_sql
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not available")

    def _rows(table: str) -> list[tuple]:
        import psycopg2

        from tests.helpers.live_env import pg_creds

        c = pg_creds()
        conn = psycopg2.connect(
            host=c["host"], port=c["port"], dbname=c["database"], user=c["username"],
            password=c["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
                if cur.fetchone()[0] is None:
                    return []
                cur.execute(f'SELECT * FROM public."{table}"')  # nosec B608 - test-owned name
                return rows_by_name(cur.description, cur.fetchall())
        finally:
            conn.close()

    run_chain(
        store,
        src_exec=pg_sql,
        src_endpoint=pg_endpoint,
        dest_endpoint=pg_endpoint,
        dest_rows=_rows,
        drop_dest=lambda t: pg_sql(f'DROP TABLE IF EXISTS public."{t}"'),
        cursor_type="TIMESTAMP",
        on_run=_proof_ran,
    )


def test_wrong_write_on_run_two_still_fails_and_names_the_key(tmp_path, store):
    """AC-4: a destination that does not hold the recipe output fails the run.

    A trigger on the destination rewrites ``name`` on every row run 2 appends;
    the proof must not be scoped away from it."""
    from src.transfer.engine import UniversalTransferEngine

    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    run = _sqlite_exec(src)
    run("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(40), note VARCHAR(40), updated_at TEXT NOT NULL)")
    for i in range(1, 6):
        run("INSERT INTO t VALUES (?,?,?,?)", (i, f" Name{i} ", f"note {i}", ts(i)))
    endpoint = _sqlite_endpoint(src)
    dest = _sqlite_endpoint(dst)

    def _go(n: int):
        job_id = f"d359w{n}" + uuid.uuid4().hex[:12]
        store.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(request(endpoint("t"), dest("t"), "t"), job_id)

    assert _go(1).success
    conn = sqlite3.connect(dst)
    conn.execute(
        "CREATE TRIGGER wrong_name AFTER INSERT ON t BEGIN "
        "UPDATE t SET name = 'WRONG' WHERE rowid = NEW.rowid; END"
    )
    conn.commit()
    conn.close()
    run("UPDATE t SET note = 'changed', updated_at = ? WHERE id = 4", (ts(4, 11),))

    second = _go(2)

    assert not second.success, "a destination that does not hold the recipe output passed"
    text = f"{second.error} {(second.reconciliation or {}).get('message') or ''}"
    assert "4" in text and "name" in text.lower(), text


def test_pg_to_mysql_three_run_transformed_append_chain_is_exact(store):
    from tests.c3_live_helpers import mysql_endpoint, mysql_sql, pg_endpoint, pg_sql
    from tests.helpers.live_env import mysql_up, pg_up

    if not (pg_up() and mysql_up()):
        pytest.skip("live PostgreSQL and MySQL not available")

    def _rows(table: str) -> list[tuple]:
        import pymysql

        from tests.helpers.live_env import mysql_creds

        c = mysql_creds()
        conn = pymysql.connect(
            host=c["host"], port=int(c["port"]), user=c["username"], password=c["password"],
            database=c["database"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES LIKE %s", (table,))
                if not cur.fetchall():
                    return []
                cur.execute(f"SELECT * FROM `{table}`")  # nosec B608 - test-owned name
                return rows_by_name(cur.description, cur.fetchall())
        finally:
            conn.close()

    run_chain(
        store,
        src_exec=pg_sql,
        src_endpoint=pg_endpoint,
        dest_endpoint=mysql_endpoint,
        dest_rows=_rows,
        drop_dest=lambda t: mysql_sql(f"DROP TABLE IF EXISTS `{t}`"),
        cursor_type="TIMESTAMP",
        on_run=_proof_ran,
    )
