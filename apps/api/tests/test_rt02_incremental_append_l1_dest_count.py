"""QA RT-02 / ACC-02 — run 2+ of incremental_append failed post-write L1.

PG→PG ``incremental_append`` takes the identity COPY fast path, which hands
Gate-8 both engine digests. ``_writer_supplied_engine_digests`` returns the
writer's ``rows_written`` (the batch) as ``target_rows``, and Gate-8 used it as
the destination COUNT(*). On an occupied destination L1 then graded
``target - target_rows_before == expected`` as ``5 - 30 == 5`` and a correct
run 2 was marked FAILED ("Destination already held 30 row(s)"); the quiet run 3
graded ``0 - 35 == 0``, missed the no-op branch and failed too. Earlier fix
db459d06 only excluded ``pk_join_count`` / ``dest_count`` tokens; a real
digest pair still carried the batch size.

Live PostgreSQL (dataflow/dataflow on 127.0.0.1:5432), single and composite
identity (cursor ties broken by the key), three runs each.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _pg_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
    except OSError:
        return False
    try:
        import psycopg2

        psycopg2.connect(
            host="127.0.0.1", port=5432, dbname="dataflow",
            user="dataflow", password="dataflow", connect_timeout=2,
        ).close()
    except Exception:  # noqa: BLE001 — any connect failure means "not available"
        return False
    return True


pytestmark = pytest.mark.skipif(
    not _pg_up(), reason="PostgreSQL dataflow/dataflow not on 127.0.0.1:5432"
)


def _pg():
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow"
    )
    conn.autocommit = True
    return conn


def _endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        schema="public",
        username="dataflow",
        password="dataflow",
        table=table,
    )


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Mongo(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields or {})

    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, src: str, dst: str, pk: list[str]):
    job_id = "rt02" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=_endpoint(src),
        destination=_endpoint(dst),
        sync_mode="incremental_append",
        stream_contracts=[{
            "name": src,
            "cursor_field": "updated_at",
            "primary_key": pk,
            "sync_mode": "incremental_append",
            "selected": True,
        }],
        skip_preflight=True,
        validation_mode="strict",
    )
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _count(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        return int(cur.fetchone()[0])


@pytest.mark.parametrize("composite", [False, True], ids=["single_key", "composite_key"])
def test_pg_pg_incremental_append_run2_and_quiet_run3_pass_l1(isolated, composite):
    suffix = uuid.uuid4().hex[:8]
    src, dst = f"rt02_s_{suffix}", f"rt02_d_{suffix}"
    conn = _pg()
    try:
        with conn.cursor() as cur:
            if composite:
                cur.execute(
                    f'CREATE TABLE "{src}" (region text NOT NULL, id int NOT NULL, '
                    "amount numeric(10,2), updated_at timestamp NOT NULL, "
                    "PRIMARY KEY (region, id))"
                )
                # Every row of a region ties on updated_at with its peers.
                cur.execute(
                    f'INSERT INTO "{src}" SELECT r, i, i * 1.5, '
                    "timestamp '2026-01-01' + (i % 3) * interval '1 minute' "
                    "FROM unnest(array['eu','us','ap']) r, generate_series(1, 10) i"
                )
            else:
                cur.execute(
                    f'CREATE TABLE "{src}" (id int PRIMARY KEY, amount numeric(10,2), '
                    "updated_at timestamp NOT NULL)"
                )
                cur.execute(
                    f'INSERT INTO "{src}" SELECT i, i * 1.5, '
                    "timestamp '2026-01-01' + i * interval '1 minute' "
                    "FROM generate_series(1, 30) i"
                )
        pk = ["region", "id"] if composite else ["id"]

        first = _run(isolated, src, dst, pk)
        assert first.success, first.error
        assert _count(conn, dst) == 30

        with conn.cursor() as cur:
            if composite:
                cur.execute(
                    f'INSERT INTO "{src}" SELECT r, i, i * 1.5, timestamp \'2026-02-01\' '
                    "FROM unnest(array['eu','us']) r, generate_series(11, 12) i"
                )
            else:
                cur.execute(
                    f'INSERT INTO "{src}" SELECT i, i * 1.5, '
                    "timestamp '2026-02-01' + i * interval '1 minute' "
                    "FROM generate_series(31, 34) i"
                )
        second = _run(isolated, src, dst, pk)
        assert second.success, second.error  # RT-02: "L1 cardinality failed"
        assert _count(conn, dst) == 34

        third = _run(isolated, src, dst, pk)  # quiet poll
        assert third.success, third.error
        assert _count(conn, dst) == 34
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}", "{dst}"')
        conn.close()


def test_sqlite_destination_count_reads_a_connection_string(tmp_path):
    """A UI-saved SQLite connector carries ``connection_string`` only.

    ``destination_row_count`` read ``database`` alone and returned ``None``, so
    PG→SQLite quiet polls failed "pre-write destination count was not measured".
    """
    import sqlite3

    from services.dest_precount import destination_row_count, precount_table

    db = tmp_path / "dst.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE "QA_T_d" (id INTEGER PRIMARY KEY)')
        conn.executemany('INSERT INTO "QA_T_d" VALUES (?)', [(i,) for i in range(5)])
    cfg = {"connection_string": f"sqlite:///{db}", "database": ""}
    assert destination_row_count("sqlite", cfg, schema="", table_name="QA_T_d") == 5
    assert precount_table("sqlite", cfg, "QA_T_d") == 5
    assert destination_row_count("sqlite", cfg, schema="", table_name="absent") == 0
    memory = {"connection_string": "sqlite://:memory:"}
    assert destination_row_count("sqlite", memory, schema="", table_name="QA_T_d") is None


def test_pg_sqlite_incremental_upsert_quiet_run3_is_a_proven_no_op(isolated, tmp_path):
    suffix = uuid.uuid4().hex[:8]
    src, dst = f"rt02_q_{suffix}", f"rt02_q_{suffix}_d"
    db = tmp_path / "dst.sqlite"
    dest = EndpointConfig(
        kind="database", format="sqlite", connection_string=f"sqlite:///{db}", table=dst
    )
    conn = _pg()

    def run():
        job_id = "rt02q" + uuid.uuid4().hex[:16]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        request = TransferRequest(
            source=_endpoint(src),
            destination=dest,
            sync_mode="incremental_upsert",
            stream_contracts=[{
                "name": src, "cursor_field": "updated_at", "primary_key": ["id"],
                "sync_mode": "incremental_upsert", "selected": True,
            }],
            skip_preflight=True,
            validation_mode="strict",
        )
        return UniversalTransferEngine().execute_tracked(request, job_id)

    try:
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{src}" (id int PRIMARY KEY, amount numeric(10,2), '
                "updated_at timestamp NOT NULL)"
            )
            cur.execute(
                f'INSERT INTO "{src}" SELECT i, i * 1.5, '
                "timestamp '2026-01-01' + i * interval '1 minute' "
                "FROM generate_series(1, 30) i"
            )
        first = run()
        assert first.success, first.error
        with conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO "{src}" SELECT i, i * 1.5, '
                "timestamp '2026-02-01' + i * interval '1 minute' "
                "FROM generate_series(31, 34) i"
            )
        second = run()
        assert second.success, second.error
        third = run()  # quiet poll
        assert third.success, third.error  # RT-02: "count was not measured"
        import sqlite3

        with sqlite3.connect(db) as sq:
            assert sq.execute(f'SELECT COUNT(*) FROM "{dst}"').fetchone()[0] == 34
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{src}"')
        conn.close()
