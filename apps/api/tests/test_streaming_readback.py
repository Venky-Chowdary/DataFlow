"""Whole-table read-back must stream, and must still see its columns.

The Gate-8 destination proof was the largest allocation of a migration: a
measured 2.0 GB RSS to verify 10M rows, because psycopg2/PyMySQL buffer the
entire result set at ``execute`` time and ``fetchmany`` only slices what is
already in client memory. Server-side cursors fix that, but they also report
no ``description`` until the first block is fetched — reading column names too
early fingerprints zero columns and returns the digest of an empty table, i.e.
a proof that quietly proves nothing.
"""

import pytest

from services.reconciliation import (
    canonical_checksum_from_iter,
    dbapi_streaming_rows,
    streaming_readback_cursor,
)


class _LateDescriptionCursor:
    """Server-side cursor semantics: description appears after the first fetch."""

    def __init__(self, rows, names):
        self._rows = list(rows)
        self._names = names
        self._pos = 0
        self.description = None
        self.closed = False

    def fetchmany(self, size):
        if self._pos == 0:
            self.description = [(n,) for n in self._names]
        batch = self._rows[self._pos : self._pos + size]
        self._pos += len(batch)
        return batch

    def close(self):
        self.closed = True


def test_column_names_read_after_priming_not_before():
    cur = _LateDescriptionCursor([(1, "a"), (2, "b")], ["id", "name"])
    names, rows = dbapi_streaming_rows(cur, batch_size=1)

    assert names == ["id", "name"]
    assert list(rows) == [(1, "a"), (2, "b")]


def test_empty_table_keeps_its_columns_and_yields_no_rows():
    """An empty destination still has a shape; priming must not lose it."""
    cur = _LateDescriptionCursor([], ["id"])
    names, rows = dbapi_streaming_rows(cur)

    assert names == ["id"]
    assert list(rows) == []


def test_streamed_digest_equals_buffered_digest():
    """Streaming must change memory, never the verdict."""
    rows = [(i, f"n{i}") for i in range(2500)]
    columns = ["id", "name"]

    cur = _LateDescriptionCursor(rows, columns)
    names, streamed = dbapi_streaming_rows(cur, batch_size=100)

    assert canonical_checksum_from_iter(
        streamed, names, dest_db_type="postgresql"
    ) == canonical_checksum_from_iter(iter(rows), columns, dest_db_type="postgresql")


def _pg_conn():
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        return psycopg2.connect(
            host="localhost",
            port=5433,
            dbname="dataflow",
            user="postgres",
            password="postgres",  # nosec B106 - local fixture
            connect_timeout=3,
        )
    except Exception as exc:  # noqa: BLE001 - env dependent, any failure means skip
        pytest.skip(f"local PostgreSQL unavailable: {exc}")


def test_postgres_server_side_cursor_reads_full_table():
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS df_stream_readback")
            cur.execute("CREATE TABLE df_stream_readback (id int, name text)")
            cur.executemany(
                "INSERT INTO df_stream_readback VALUES (%s, %s)",
                [(i, f"n{i}") for i in range(1000)],
            )
        conn.commit()

        with streaming_readback_cursor(conn, engine="postgresql") as cur:
            assert cur.name, "expected a named (server-side) cursor"
            cur.execute("SELECT * FROM df_stream_readback")
            names, rows = dbapi_streaming_rows(cur)
            materialized = list(rows)

        assert names == ["id", "name"]
        assert len(materialized) == 1000
        assert materialized[0] == (0, "n0")
    finally:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS df_stream_readback")
        conn.commit()
        conn.close()


def test_autocommit_connection_falls_back_to_client_cursor():
    """Named cursors need a transaction; autocommit must not silently break."""
    conn = _pg_conn()
    try:
        conn.autocommit = True
        with streaming_readback_cursor(conn, engine="postgresql") as cur:
            assert not cur.name
            cur.execute("SELECT 1 AS one")
            names, rows = dbapi_streaming_rows(cur)
            assert names == ["one"]
            assert list(rows) == [(1,)]
    finally:
        conn.close()
