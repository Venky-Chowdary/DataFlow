"""PostgreSQL full-refresh overwrite is one transaction (R4-D06).

A second session must keep seeing the previous rows until the load commits,
and a rollback must put those rows back. The overwrite state is a
PgOverwriteTransaction, not attributes hung on the driver connection.
"""

from __future__ import annotations

import inspect
import socket
import uuid

import pytest

PG = dict(
    host="127.0.0.1",
    port=5432,
    database="dataflow",
    user="dataflow",
    password="dataflow",
)


def _pg_or_skip():
    try:
        with socket.create_connection((PG["host"], PG["port"]), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")


def test_overwrite_transaction_is_explicit_and_not_stored_on_the_connection():
    from connectors import postgresql_writer
    from connectors.write_resilience import (
        PgOverwriteTransaction,
        bind_overwrite_transaction,
        finish_held_overwrite,
        overwrite_transaction_for,
    )

    assert "_df_overwrite" not in inspect.getsource(postgresql_writer)

    class Conn:
        def __init__(self) -> None:
            self.commits = 0
            self.rollbacks = 0

        def commit(self) -> None:
            self.commits += 1

        def rollback(self) -> None:
            self.rollbacks += 1

    conn = Conn()
    txn = PgOverwriteTransaction()
    bind_overwrite_transaction(conn, txn)
    txn.note_truncated()
    assert txn.active is True
    assert not hasattr(conn, "_df_overwrite_txn")
    finish_held_overwrite(conn, success=True)
    assert conn.commits == 1
    assert overwrite_transaction_for(conn) is None

    txn = PgOverwriteTransaction()
    bind_overwrite_transaction(conn, txn)
    txn.note_truncated()
    finish_held_overwrite(conn, success=False)
    assert conn.rollbacks == 1
    assert txn.active is False
    assert overwrite_transaction_for(conn) is None


def test_live_postgres_overwrite_hides_the_new_rows_until_commit():
    _pg_or_skip()
    psycopg2 = pytest.importorskip("psycopg2")
    from connectors.postgresql_writer import write_mapped_rows
    from connectors.write_resilience import finish_held_overwrite

    table = f"r4d06_atomic_{uuid.uuid4().hex[:10]}"
    writer = psycopg2.connect(**PG)
    writer.autocommit = False
    reader = psycopg2.connect(**PG)
    reader.autocommit = True
    try:
        with writer.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, label TEXT)"
            )
            cur.execute(
                f"INSERT INTO {table} (id, label) VALUES (1, 'old-a'), (2, 'old-b'), (3, 'old-c'), (4, 'old-d')"
            )
        writer.commit()

        def count_labels() -> list[str]:
            with reader.cursor() as cur:
                cur.execute("SET lock_timeout = '200ms'")
                cur.execute(f"SELECT label FROM {table} ORDER BY id")
                return [row[0] for row in cur.fetchall()]

        def reader_blocked() -> bool:
            try:
                count_labels()
            except psycopg2.errors.LockNotAvailable:
                reader.rollback()
                return True
            return False

        assert count_labels() == ["old-a", "old-b", "old-c", "old-d"]

        result = write_mapped_rows(
            host=PG["host"],
            port=PG["port"],
            database=PG["database"],
            username=PG["user"],
            password=PG["password"],
            schema="public",
            connection_string="",
            ssl=False,
            table_name=table,
            headers=["id", "label"],
            data_rows=[["1", "new-a"], ["2", "new-b"]],
            mappings=[
                {"source": "id", "target": "id", "source_type": "integer", "target_type": "integer"},
                {"source": "label", "target": "label", "source_type": "string", "target_type": "string"},
            ],
            column_types={"id": "integer", "label": "string"},
            create_table=False,
            write_mode="insert",
            connection=writer,
            close_connection=False,
            pg_overwrite_in_transaction=True,
        )
        assert result.ok, result.error
        # TRUNCATE holds an exclusive lock until the overwrite commits, so a
        # reader cannot observe an empty or partial table.
        assert reader_blocked()

        finish_held_overwrite(writer, success=False)
        assert count_labels() == ["old-a", "old-b", "old-c", "old-d"]

        result = write_mapped_rows(
            host=PG["host"],
            port=PG["port"],
            database=PG["database"],
            username=PG["user"],
            password=PG["password"],
            schema="public",
            connection_string="",
            ssl=False,
            table_name=table,
            headers=["id", "label"],
            data_rows=[["1", "new-a"], ["2", "new-b"]],
            mappings=[
                {"source": "id", "target": "id", "source_type": "integer", "target_type": "integer"},
                {"source": "label", "target": "label", "source_type": "string", "target_type": "string"},
            ],
            column_types={"id": "integer", "label": "string"},
            create_table=False,
            write_mode="insert",
            connection=writer,
            close_connection=False,
            pg_overwrite_in_transaction=True,
        )
        assert result.ok, result.error
        assert reader_blocked()
        finish_held_overwrite(writer, success=True)
        assert count_labels() == ["new-a", "new-b"]
        assert not hasattr(writer, "_df_overwrite_truncated")
    finally:
        try:
            writer.rollback()
        except Exception:
            pass
        writer.close()
        with reader.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        reader.close()


def test_live_postgres_new_table_overwrite_commits_once():
    """A full refresh into a table that does not exist yet must not publish a
    prefix. The first batch used to commit 20,000 rows, and the next batch
    died setting ``_df_overwrite_truncated`` on the psycopg2 connection.
    """
    _pg_or_skip()
    psycopg2 = pytest.importorskip("psycopg2")
    from connectors.postgresql_writer import write_mapped_rows
    from connectors.write_resilience import finish_held_overwrite

    table = f"r5_new_{uuid.uuid4().hex[:10]}"
    writer = psycopg2.connect(**PG)
    writer.autocommit = False
    reader = psycopg2.connect(**PG)
    reader.autocommit = True

    def visible() -> list[tuple]:
        with reader.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table,),
            )
            if cur.fetchone() is None:
                return []
            cur.execute(f"SELECT id, label FROM {table} ORDER BY id")
            return list(cur.fetchall())

    mappings = [
        {"source": "id", "target": "id", "source_type": "integer", "target_type": "integer"},
        {"source": "label", "target": "label", "source_type": "string", "target_type": "string"},
    ]
    common = dict(
        host=PG["host"],
        port=PG["port"],
        database=PG["database"],
        username=PG["user"],
        password=PG["password"],
        schema="public",
        connection_string="",
        ssl=False,
        table_name=table,
        headers=["id", "label"],
        mappings=mappings,
        column_types={"id": "integer", "label": "string"},
        create_table=True,
        write_mode="insert",
        connection=writer,
        close_connection=False,
        pg_overwrite_in_transaction=True,
    )
    try:
        first = write_mapped_rows(
            **common,
            data_rows=[["1", "a"], ["2", "b"]],
        )
        assert first.ok, first.error
        assert visible() == []
        assert not hasattr(writer, "_df_overwrite_truncated")

        second = write_mapped_rows(
            **common,
            data_rows=[["3", "c"]],
        )
        assert second.ok, second.error
        assert visible() == []

        finish_held_overwrite(writer, success=False)
        assert visible() == []

        first = write_mapped_rows(**common, data_rows=[["1", "a"], ["2", "b"]])
        assert first.ok, first.error
        second = write_mapped_rows(**common, data_rows=[["3", "c"]])
        assert second.ok, second.error
        finish_held_overwrite(writer, success=True)
        assert visible() == [(1, "a"), (2, "b"), (3, "c")]
    finally:
        try:
            finish_held_overwrite(writer, success=False)
        except Exception:
            pass
        try:
            writer.rollback()
        except Exception:
            pass
        writer.close()
        with reader.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        reader.close()


def test_parallel_workers_keep_every_overwrite_batch(monkeypatch):
    """Wide rows used to be one row per batch on a worker pool. Each worker
    truncated and committed, so a 3-row overwrite left the last row.
    """
    _pg_or_skip()
    monkeypatch.setenv("DATAFLOW_PARALLEL_WORKERS", "4")
    import src.transfer.stream as stream_mod

    monkeypatch.setattr(stream_mod, "adaptive_chunk_size", lambda *args, **kwargs: 1)

    psycopg2 = pytest.importorskip("psycopg2")
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    suffix = uuid.uuid4().hex[:8]
    src, dst = f"ow_src_{suffix}", f"ow_dst_{suffix}"
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE "{src}" (id bigint, label text)')
        cur.executemany(
            f'INSERT INTO "{src}" (id, label) VALUES (%s, %s)',
            [(1, "a"), (2, "b"), (3, "c")],
        )

    def endpoint(table: str) -> EndpointConfig:
        return EndpointConfig(
            kind="database",
            format="postgresql",
            host=PG["host"],
            port=PG["port"],
            database=PG["database"],
            username=PG["user"],
            password=PG["password"],
            schema="public",
            table=table,
        )

    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=endpoint(src),
                destination=endpoint(dst),
                mappings=[
                    {"source": "id", "target": "id", "confidence": 0.99},
                    {"source": "label", "target": "label", "confidence": 0.99},
                ],
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
                skip_preflight=True,
                stream_contracts=[
                    {
                        "name": src,
                        "primary_key": "id",
                        "selected": True,
                        "sync_mode": "full_refresh_overwrite",
                    }
                ],
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success, result.error
        with conn.cursor() as cur:
            cur.execute(f'SELECT id, label FROM "{dst}" ORDER BY id')
            assert cur.fetchall() == [(1, "a"), (2, "b"), (3, "c")]
    finally:
        with conn.cursor() as cur:
            for name in (src, dst):
                cur.execute(f'DROP TABLE IF EXISTS "{name}"')
        conn.close()
