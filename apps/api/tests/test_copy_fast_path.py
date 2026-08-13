"""The COPY fast path moves rows without Python seeing them — so prove it.

Skipping the per-row path also skips per-row fingerprints, which means this code
carries its own proof or the run has none. Two claims need holding down:

* every value survives, including the types a text rendering would damage; and
* the digest describes the rows that were actually copied, which is why the
  source digest is taken inside the transaction that feeds the COPY rather than
  afterwards on a fresh connection.

The second is tested by writing to the source *while the copy runs*. Under
REPEATABLE READ those rows are outside the snapshot, so a correct
implementation both excludes them and still verifies. An implementation that
digested the source afterwards would report a mismatch on a transfer that was
right — the bug that kept the engine-side digest gated off.
"""

from __future__ import annotations

import socket
import threading
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from services.copy_fast_path import (
    FastPathUnavailable,
    copy_between_postgres,
)

CFG = {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
}


def _pg_reachable() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_reachable(), reason="PostgreSQL not reachable on 127.0.0.1:5432"
)


@pytest.fixture()
def pg():
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


class _Tables:
    def __init__(self, conn, ddl: str):
        self.conn = conn
        suffix = uuid.uuid4().hex[:8]
        self.src = f"fp_src_{suffix}"
        self.dst = f"fp_dst_{suffix}"
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE "{self.src}" ({ddl})')

    def insert(self, sql: str, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql.format(src=f'"{self.src}"'), params)

    def rows(self, table: str, order: str = "1"):
        with self.conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{table}" ORDER BY {order}')
            return cur.fetchall()

    def count(self, table: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM "{table}"')
            return cur.fetchone()[0]

    def drop(self):
        with self.conn.cursor() as cur:
            for name in (self.src, self.dst):
                cur.execute(f'DROP TABLE IF EXISTS "{name}"')


def _copy(tables: _Tables, pairs, **kwargs):
    return copy_between_postgres(
        source_cfg=CFG,
        source_schema="public",
        source_table=tables.src,
        dest_cfg=CFG,
        dest_schema="public",
        dest_table=tables.dst,
        pairs=pairs,
        **kwargs,
    )


def test_every_scalar_type_survives_the_round_trip(pg):
    """Binary COPY preserves what a text rendering would round or reformat."""
    tables = _Tables(
        pg,
        "id bigint, amount numeric(18,6), ratio double precision, flag boolean, "
        "created_at timestamp(6), seen_at timestamptz, ident uuid, "
        "payload jsonb, raw bytea, tags text[], note text",
    )
    ist = timezone(timedelta(hours=5, minutes=30))
    try:
        tables.insert(
            "INSERT INTO {src} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                9223372036854775807,
                Decimal("12345678901.234567"),
                1.7976931348623157e308,
                True,
                datetime(2026, 8, 13, 16, 42, 32, 677645),
                datetime(2026, 8, 13, 16, 42, 32, 677645, tzinfo=ist),
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                '{"b": 2, "a": [1, null, "x"]}',
                b"\x00\x01\xfe\xff",
                ["a", "b,c", 'd"e'],
                "unicode ünïcodé — 中文 \n newline \t tab 'quote' \"double\"",
            ),
        )
        columns = [
            "id",
            "amount",
            "ratio",
            "flag",
            "created_at",
            "seen_at",
            "ident",
            "payload",
            "raw",
            "tags",
            "note",
        ]
        result = _copy(tables, [(c, c) for c in columns])
        assert result.verified
        assert result.rows_copied == 1
        assert tables.rows(tables.src) == tables.rows(tables.dst)
    finally:
        tables.drop()


def test_nulls_are_not_confused_with_empty_or_zero(pg):
    tables = _Tables(pg, "id bigint, note text, amount numeric(12,2), flag boolean")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, NULL, NULL, NULL)")
        tables.insert("INSERT INTO {src} VALUES (2, '', 0.00, false)")
        columns = ["id", "note", "amount", "flag"]
        result = _copy(tables, [(c, c) for c in columns])
        assert result.verified
        assert tables.rows(tables.src) == tables.rows(tables.dst)
    finally:
        tables.drop()


def test_empty_source_produces_an_empty_verified_destination(pg):
    tables = _Tables(pg, "id bigint, note text")
    try:
        result = _copy(tables, [("id", "id"), ("note", "note")])
        assert result.verified
        assert result.rows_copied == 0
        assert tables.count(tables.dst) == 0
    finally:
        tables.drop()


def test_renamed_columns_pair_positionally(pg):
    tables = _Tables(pg, "id bigint, amount numeric(12,2)")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 10.50), (2, 20.25)")
        result = _copy(tables, [("id", "pk"), ("amount", "total")])
        assert result.verified
        with pg.cursor() as cur:
            cur.execute(f'SELECT pk, total FROM "{tables.dst}" ORDER BY pk')
            assert cur.fetchall() == [(1, Decimal("10.50")), (2, Decimal("20.25"))]
    finally:
        tables.drop()


def test_rows_written_during_the_copy_are_outside_the_snapshot(pg):
    """The claim that makes an in-transaction digest correct.

    A digest taken after the copy on a fresh connection would see these rows and
    report a mismatch against a destination that never should have held them.
    """
    psycopg2 = pytest.importorskip("psycopg2")
    tables = _Tables(pg, "id bigint, note text")
    try:
        with pg.cursor() as cur:
            cur.execute(
                f'INSERT INTO "{tables.src}" '
                "SELECT g, 'row_'||g FROM generate_series(1, 60000) g"
            )

        stop = threading.Event()
        inserted: list[int] = []

        def _write_during_copy() -> None:
            writer = psycopg2.connect(
                host="127.0.0.1",
                port=5432,
                dbname="dataflow",
                user="dataflow",
                password="dataflow",
            )
            writer.autocommit = True
            try:
                nxt = 1_000_000
                while not stop.is_set():
                    with writer.cursor() as cur:
                        cur.execute(
                            f'INSERT INTO "{tables.src}" VALUES (%s, %s)',
                            (nxt, f"concurrent_{nxt}"),
                        )
                    inserted.append(nxt)
                    nxt += 1
            finally:
                writer.close()

        noise = threading.Thread(target=_write_during_copy, daemon=True)
        noise.start()
        try:
            result = _copy(tables, [("id", "id"), ("note", "note")])
        finally:
            stop.set()
            noise.join(timeout=30)

        # Concurrent rows really did land in the source while the copy ran.
        assert inserted, "the concurrency this test depends on did not happen"
        assert tables.count(tables.src) > result.rows_copied

        # The guarantee is that the digest describes the population that was
        # copied. Which concurrent rows fall inside the snapshot depends on when
        # each insert commits relative to the snapshot, and asserting "none of
        # them" would be asserting the race rather than the invariant.
        assert result.verified
        assert result.source_rows == result.rows_copied
        assert result.target_rows == result.rows_copied

        # Nothing was invented: every destination row exists in the source.
        with pg.cursor() as cur:
            cur.execute(
                f'SELECT count(*) FROM '  # nosec B608
                f'(SELECT id, note FROM "{tables.dst}" '
                f'EXCEPT SELECT id, note FROM "{tables.src}") q'
            )
            assert cur.fetchone()[0] == 0
        # And the source kept growing past the snapshot, which is the condition
        # that made a post-hoc digest report a mismatch on a correct transfer.
        assert tables.count(tables.src) > result.target_rows
    finally:
        tables.drop()


def test_a_failed_copy_leaves_the_destination_as_it_was(pg):
    """A half-replaced table is worse than a refused one."""
    tables = _Tables(pg, "id bigint, note text")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'a'), (2, 'b')")
        assert _copy(tables, [("id", "id"), ("note", "note")]).verified
        assert tables.count(tables.dst) == 2

        # A column the source does not have: the run must abort before touching
        # the destination it already populated.
        with pytest.raises(Exception):
            _copy(tables, [("id", "id"), ("nope", "nope")])
        assert tables.count(tables.dst) == 2
        assert tables.rows(tables.dst) == [(1, "a"), (2, "b")]
    finally:
        tables.drop()


def test_replace_destination_false_appends(pg):
    tables = _Tables(pg, "id bigint, note text")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'a')")
        assert _copy(tables, [("id", "id"), ("note", "note")]).verified
        # Appending means the destination no longer matches the source snapshot,
        # so the verdict must say so rather than report a clean load.
        with pytest.raises(ValueError, match="does not match the source snapshot"):
            _copy(
                tables, [("id", "id"), ("note", "note")], replace_destination=False
            )
    finally:
        tables.drop()


def test_no_columns_is_declined_not_attempted(pg):
    tables = _Tables(pg, "id bigint")
    try:
        with pytest.raises(FastPathUnavailable):
            _copy(tables, [])
    finally:
        tables.drop()


def test_repeated_runs_replace_rather_than_accumulate(pg):
    tables = _Tables(pg, "id bigint, note text")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'a'), (2, 'b')")
        for _ in range(3):
            assert _copy(tables, [("id", "id"), ("note", "note")]).verified
        assert tables.count(tables.dst) == 2
    finally:
        tables.drop()
