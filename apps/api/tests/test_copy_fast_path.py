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
        # Occupied dest is not a second COPY — that would duplicate then fail
        # the digest. Decline so the row path (quarantine) owns the case.
        with pytest.raises(FastPathUnavailable, match="non-empty"):
            _copy(
                tables, [("id", "id"), ("note", "note")], replace_destination=False
            )
        assert tables.count(tables.dst) == 1
    finally:
        tables.drop()


def test_append_creates_missing_dest_and_counts(pg):
    tables = _Tables(pg, "id bigint PRIMARY KEY, note text")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'a')")
        tables.insert("INSERT INTO {src} VALUES (2, 'b')")
        result = _copy(
            tables, [("id", "id"), ("note", "note")], replace_destination=False
        )
        assert result.verified
        assert result.target_rows == 2
        assert tables.count(tables.dst) == 2
        assert result.source_snapshot.get("copy_split") == "binary"
    finally:
        tables.drop()


def test_refuses_copy_onto_the_same_table(pg):
    from services.copy_fast_path import postgres_same_relation

    assert postgres_same_relation(
        CFG, CFG, "public", "t", "public", "t"
    ) is True
    assert postgres_same_relation(
        CFG, {**CFG, "port": 5433}, "public", "t", "public", "t"
    ) is False
    tables = _Tables(pg, "id bigint")
    try:
        tables.insert("INSERT INTO {src} VALUES (1)")
        with pytest.raises(FastPathUnavailable, match="same PostgreSQL table"):
            copy_between_postgres(
                source_cfg=CFG,
                source_schema="public",
                source_table=tables.src,
                dest_cfg=CFG,
                dest_schema="public",
                dest_table=tables.src,
                pairs=[("id", "id")],
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


def test_primary_key_not_null_and_defaults_travel_with_the_values(pg):
    """Values without the structure that governs them is a different table."""
    tables = _Tables(
        pg,
        "id bigint PRIMARY KEY, code text NOT NULL, "
        "status text DEFAULT 'new', note text",
    )
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'a', 'open', NULL)")
        result = _copy(
            tables,
            [("id", "id"), ("code", "code"), ("status", "status"), ("note", "note")],
        )
        assert result.verified
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname, a.attnotnull, pg_get_expr(ad.adbin, ad.adrelid)
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                LEFT JOIN pg_catalog.pg_attrdef ad
                  ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
                WHERE c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
                """,
                (tables.dst,),
            )
            live = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
            assert live["id"][0] is True
            assert live["code"][0] is True
            assert live["note"][0] is False
            assert "new" in (live["status"][1] or "")

            cur.execute(
                """
                SELECT count(*) FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
                WHERE c.relname = %s AND i.indisprimary
                """,
                (tables.dst,),
            )
            assert cur.fetchone()[0] == 1
    finally:
        tables.drop()


@pytest.mark.parametrize(
    "ddl,structure",
    [
        ("id bigint, code text UNIQUE", "unique"),
        ("id bigint, age int CHECK (age > 0)", "check"),
        ("id bigint GENERATED ALWAYS AS IDENTITY, note text", "identity"),
    ],
)
def test_structure_this_path_cannot_carry_makes_it_decline(pg, ddl, structure):
    """Declining sends the route to the path that reproduces it properly."""
    tables = _Tables(pg, ddl)
    try:
        columns = ["id", "code"] if "code" in ddl else (
            ["id", "age"] if "age" in ddl else ["id", "note"]
        )
        with pytest.raises(FastPathUnavailable, match="cannot"):
            _copy(tables, [(c, c) for c in columns])
        # Nothing was created: the caller falls back with the destination clean.
        with pg.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{tables.dst}",))
            assert cur.fetchone()[0] is None
    finally:
        tables.drop()


def _indexes(conn, table: str):
    """(is_unique, sorted key columns) for every non-primary index on a table."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ix.indisunique,
                   array_agg(a.attname ORDER BY a.attnum)
            FROM pg_index ix
            JOIN pg_class c ON c.oid = ix.indrelid
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(ix.indkey)
            WHERE c.relname = %s AND NOT ix.indisprimary
            GROUP BY ix.indexrelid, ix.indisunique
            """,
            (table,),
        )
        return sorted((bool(u), sorted(cols)) for u, cols in cur.fetchall())


def test_secondary_indexes_are_carried_not_dropped(pg):
    """A value copy that left indexes behind hands back a different table.

    A UNIQUE index is a data-integrity guarantee and a plain index is the read
    cost the operator provisioned for; dropping either silently makes the
    destination behave differently while Gate-8, which only reads the data,
    still reports a clean load.
    """
    tables = _Tables(pg, "id bigint, email text, status text")
    try:
        tables.insert(
            "INSERT INTO {src} "
            "SELECT g, 'e'||g, 's'||(g%3) FROM generate_series(1, 200) g"
        )
        with pg.cursor() as cur:
            cur.execute(f'CREATE UNIQUE INDEX ux_email ON "{tables.src}" (email)')
            cur.execute(f'CREATE INDEX ix_status ON "{tables.src}" (status)')

        result = _copy(
            tables, [("id", "id"), ("email", "email"), ("status", "status")]
        )
        assert result.verified
        assert len(result.indexes_carried) == 2
        # Same rules, same columns — the unique flag in particular has to survive.
        assert _indexes(pg, tables.dst) == [
            (False, ["status"]),
            (True, ["email"]),
        ]
    finally:
        tables.drop()


def test_a_carried_unique_index_still_enforces_uniqueness(pg):
    """The carried index is the guarantee, not a decoration."""
    tables = _Tables(pg, "id bigint, email text")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'a@x'), (2, 'b@x')")
        with pg.cursor() as cur:
            cur.execute(f'CREATE UNIQUE INDEX ux_email ON "{tables.src}" (email)')
        assert _copy(tables, [("id", "id"), ("email", "email")]).verified

        psycopg2 = pytest.importorskip("psycopg2")
        with pytest.raises(psycopg2.errors.UniqueViolation):
            with pg.cursor() as cur:
                cur.execute(f'INSERT INTO "{tables.dst}" VALUES (3, %s)', ("a@x",))
    finally:
        tables.drop()


def test_a_renamed_column_carries_its_index_under_the_new_name(pg):
    tables = _Tables(pg, "id bigint, email text")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'a@x'), (2, 'b@x')")
        with pg.cursor() as cur:
            cur.execute(f'CREATE UNIQUE INDEX ux_email ON "{tables.src}" (email)')
        result = _copy(tables, [("id", "id"), ("email", "contact")])
        assert result.verified
        assert result.indexes_carried
        # The index follows the value to its new column name.
        assert _indexes(pg, tables.dst) == [(True, ["contact"])]
    finally:
        tables.drop()


@pytest.mark.parametrize(
    "index_sql,why",
    [
        ('CREATE INDEX ix_expr ON {src} (lower(email))', "expression"),
        (
            "CREATE INDEX ix_part ON {src} (status) WHERE status = 'open'",
            "partial",
        ),
        ('CREATE UNIQUE INDEX ux_expr ON {src} (lower(email))', "unique expression"),
    ],
)
def test_an_index_that_cannot_be_reproduced_declines_the_route(pg, index_sql, why):
    """Declining sends the whole route to the row path, which reproduces it.

    Carrying the values while quietly dropping a rule the destination cannot
    express — a case-insensitive UNIQUE, a filtered guarantee — is exactly the
    silent structure loss this path refuses to commit.
    """
    tables = _Tables(pg, "id bigint, email text, status text")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'a@x', 'open')")
        with pg.cursor() as cur:
            cur.execute(index_sql.format(src=f'"{tables.src}"'))
        with pytest.raises(FastPathUnavailable):
            _copy(tables, [("id", "id"), ("email", "email"), ("status", "status")])
        # The destination is left clean for the row path to build properly.
        with pg.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"public.{tables.dst}",))
            assert cur.fetchone()[0] is None
    finally:
        tables.drop()


def test_an_index_over_a_dropped_column_is_skipped_not_declined(pg):
    """The rule cannot exist once its column is gone; that is not a decline."""
    tables = _Tables(pg, "id bigint, email text, note text")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'a@x', 'hi')")
        with pg.cursor() as cur:
            cur.execute(f'CREATE INDEX ix_email ON "{tables.src}" (email)')
        # email is intentionally not copied, so its index has no column to cover.
        result = _copy(tables, [("id", "id"), ("note", "note")])
        assert result.verified
        assert result.indexes_carried == ()
        assert _indexes(pg, tables.dst) == []
    finally:
        tables.drop()


def test_a_partial_key_is_not_invented(pg):
    """Copying half a composite key must not declare it a primary key."""
    tables = _Tables(pg, "org_id bigint, code text, note text, PRIMARY KEY (org_id, code)")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'a', 'x')")
        result = _copy(tables, [("org_id", "org_id"), ("note", "note")])
        assert result.verified
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
                WHERE c.relname = %s AND i.indisprimary
                """,
                (tables.dst,),
            )
            assert cur.fetchone()[0] == 0
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


def test_a_gin_index_is_reproduced_not_silently_turned_into_btree(pg):
    tables = _Tables(pg, "id bigint, payload jsonb")
    try:
        tables.insert("INSERT INTO {src} SELECT 1, jsonb_build_object('a', 1)")
        with pg.cursor() as cur:
            cur.execute(f'CREATE INDEX ix_gin ON "{tables.src}" USING gin (payload)')
        result = _copy(tables, [("id", "id"), ("payload", "payload")])
        assert result.verified
        assert result.indexes_carried
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT am.amname
                FROM pg_index ix
                JOIN pg_class c ON c.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_am am ON am.oid = i.relam
                WHERE c.relname = %s AND NOT ix.indisprimary
                """,
                (tables.dst,),
            )
            methods = [r[0] for r in cur.fetchall()]
        assert methods == ["gin"]
    finally:
        tables.drop()


def test_an_operator_class_travels_with_the_index(pg):
    tables = _Tables(pg, "id bigint, code text")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'aa'), (2, 'ab')")
        with pg.cursor() as cur:
            cur.execute(
                f'CREATE INDEX ix_ops ON "{tables.src}" (code varchar_pattern_ops)'
            )
        result = _copy(tables, [("id", "id"), ("code", "code")])
        assert result.verified
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT pg_get_indexdef(ix.indexrelid)
                FROM pg_index ix
                JOIN pg_class c ON c.oid = ix.indrelid
                WHERE c.relname = %s AND NOT ix.indisprimary
                """,
                (tables.dst,),
            )
            defs = [r[0] for r in cur.fetchall()]
        assert any("varchar_pattern_ops" in d for d in defs)
    finally:
        tables.drop()


def test_a_non_default_collation_is_carried(pg):
    tables = _Tables(pg, 'id bigint, code text COLLATE "C"')
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'A'), (2, 'b')")
        result = _copy(tables, [("id", "id"), ("code", "code")])
        assert result.verified
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT col.collname
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_collation col ON col.oid = a.attcollation
                WHERE c.relname = %s AND a.attname = 'code' AND a.attnum > 0
                """,
                (tables.dst,),
            )
            assert cur.fetchone()[0] == "C"
    finally:
        tables.drop()


def test_a_stale_destination_shell_is_replaced_not_truncated(pg):
    """CREATE IF NOT EXISTS + TRUNCATE would keep the wrong types."""
    tables = _Tables(pg, "id bigint, amount numeric(12,2)")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 10.50)")
        with pg.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{tables.dst}" (id text, amount integer)'
            )
        result = _copy(tables, [("id", "id"), ("amount", "amount")])
        assert result.verified
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT format_type(a.atttypid, a.atttypmod)
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                WHERE c.relname = %s AND a.attname = 'amount' AND a.attnum > 0
                """,
                (tables.dst,),
            )
            assert "numeric" in cur.fetchone()[0]
    finally:
        tables.drop()


def test_a_trigger_makes_the_path_decline(pg):
    tables = _Tables(pg, "id bigint, note text")
    try:
        tables.insert("INSERT INTO {src} VALUES (1, 'a')")
        with pg.cursor() as cur:
            cur.execute(
                f"""
                CREATE FUNCTION {tables.src}_fn() RETURNS trigger AS $$
                BEGIN RETURN NEW; END;
                $$ LANGUAGE plpgsql
                """
            )
            cur.execute(
                f'CREATE TRIGGER {tables.src}_tg BEFORE INSERT ON "{tables.src}" '
                f"FOR EACH ROW EXECUTE PROCEDURE {tables.src}_fn()"
            )
        with pytest.raises(FastPathUnavailable, match="trigger"):
            _copy(tables, [("id", "id"), ("note", "note")])
    finally:
        with pg.cursor() as cur:
            cur.execute(f"DROP FUNCTION IF EXISTS {tables.src}_fn() CASCADE")
        tables.drop()
