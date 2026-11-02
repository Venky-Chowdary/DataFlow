"""An engine-computed digest has to catch everything the Python one catches.

Moving the Gate-8 digest into the database removes the largest cost in a
transfer, but it is only worth having if it still refuses every way a
destination can differ from its source. These tests are about detection, not
speed: each one makes the destination wrong in a specific way and asserts the
digest notices.

The encoding deserves particular suspicion. Reducing each field to a fixed-width
md5 before assembling the row is what stops a value from forging a field
boundary; a plain separator would let ``('a|b', 'c')`` and ``('a', 'b|c')``
collide, and the NULL flag is what stops NULL and the empty string from
digesting alike.
"""

from __future__ import annotations

import socket
import uuid

import pytest

from services.engine_checksum import (
    comparable_column_pairs,
    engine_supports_checksum,
    engines_comparable,
    postgresql_engine_checksum,
)


def _pg_reachable() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 5432), timeout=1).close()
        return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_reachable(), reason="PostgreSQL not reachable on 127.0.0.1:5432"
)

COLUMNS = ["id", "name", "amount", "created_at"]


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


class _Table:
    def __init__(self, conn, rows_sql: str):
        self.conn = conn
        self.name = "eck_" + uuid.uuid4().hex[:10]
        with conn.cursor() as cur:
            cur.execute(
                f'CREATE TABLE "{self.name}" '
                "(id bigint, name text, amount numeric(12,2), created_at timestamp)"
            )
            if rows_sql:
                cur.execute(f'INSERT INTO "{self.name}" {rows_sql}')

    @property
    def ref(self) -> str:
        return f'"{self.name}"'

    def digest(self):
        with self.conn.cursor() as cur:
            return postgresql_engine_checksum(cur, self.ref, COLUMNS)

    def sql(self, statement: str):
        with self.conn.cursor() as cur:
            cur.execute(statement.format(t=self.ref))

    def drop(self):
        with self.conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{self.name}"')


_SEED = (
    "SELECT g, 'name_'||g, (g%10000)::numeric/100, "
    "timestamp '2024-01-01' + (g||' seconds')::interval "
    "FROM generate_series(1, 500) g"
)


@pytest.fixture()
def pair(pg):
    left = _Table(pg, _SEED)
    right = _Table(pg, f'SELECT * FROM "{left.name}"')
    try:
        yield left, right
    finally:
        left.drop()
        right.drop()


def test_identical_populations_agree(pair):
    left, right = pair
    assert left.digest() == right.digest()


def test_digest_ignores_row_order(pair):
    """Neither side sorts, so the digest must not depend on scan order."""
    left, right = pair
    shuffled = _Table(left.conn, f'SELECT * FROM "{left.name}" ORDER BY random()')
    try:
        assert shuffled.digest().checksum == left.digest().checksum
    finally:
        shuffled.drop()


def test_one_changed_cell_is_caught(pair):
    left, right = pair
    right.sql("UPDATE {t} SET amount = amount + 0.01 WHERE id = 250")
    assert right.digest().checksum != left.digest().checksum


def test_missing_row_is_caught(pair):
    left, right = pair
    right.sql("DELETE FROM {t} WHERE id = 250")
    after = right.digest()
    assert after.row_count == left.digest().row_count - 1
    assert after.checksum != left.digest().checksum


def test_duplicated_row_is_caught(pair):
    """A digest that only summed distinct rows would miss this."""
    left, right = pair
    right.sql("INSERT INTO {t} SELECT * FROM {t} WHERE id = 250")
    assert right.digest().checksum != left.digest().checksum


def test_null_is_not_an_empty_string(pg):
    """The failure that has already bitten this codebase once."""
    nulled = _Table(pg, "SELECT 1, NULL::text, 5.00, timestamp '2024-01-01'")
    empty = _Table(pg, "SELECT 1, ''::text, 5.00, timestamp '2024-01-01'")
    try:
        assert nulled.digest().checksum != empty.digest().checksum
    finally:
        nulled.drop()
        empty.drop()


def test_values_cannot_forge_a_field_boundary(pg):
    """Fixed-width fields mean no value can impersonate a separator."""
    left = _Table(pg, "SELECT 1, 'a\x1fb', 1.00, timestamp '2024-01-01'")
    right = _Table(pg, "SELECT 1, 'a', 1.00, timestamp '2024-01-01'")
    try:
        assert left.digest().checksum != right.digest().checksum
    finally:
        left.drop()
        right.drop()


def test_swapped_values_between_columns_are_caught(pg):
    """Column identity has to survive into the digest.

    Two text columns holding each other's values are the same *multiset* of
    strings, so a digest that hashed field values without their position would
    call a transposed write correct.
    """
    name = "eckswap_" + uuid.uuid4().hex[:8]
    cols = ["first_name", "last_name"]
    with pg.cursor() as cur:
        cur.execute(f'CREATE TABLE "{name}" (first_name text, last_name text)')
        cur.execute(f"INSERT INTO \"{name}\" VALUES ('alpha', 'beta')")
        straight = postgresql_engine_checksum(cur, f'"{name}"', cols)
        cur.execute(f'UPDATE "{name}" SET first_name = \'beta\', last_name = \'alpha\'')
        swapped = postgresql_engine_checksum(cur, f'"{name}"', cols)
        cur.execute(f'DROP TABLE "{name}"')
    assert straight.checksum != swapped.checksum


def test_empty_table_digests_without_error(pg):
    empty = _Table(pg, "")
    try:
        result = empty.digest()
        assert result is not None
        assert result.row_count == 0
    finally:
        empty.drop()


def test_engines_must_match_and_be_supported():
    assert engines_comparable("postgresql", "postgresql")
    assert not engines_comparable("postgresql", "mysql")
    assert not engines_comparable("mysql", "mysql")
    assert not engines_comparable("", "")


IDENTITY = [
    {"source": "id", "target": "id"},
    {"source": "amount", "target": "amount"},
]
SAME = {"id": "bigint", "amount": "numeric(12,2)"}


def test_pure_carry_onto_identical_types_is_comparable():
    assert comparable_column_pairs(IDENTITY, SAME, SAME) == [
        ("id", "id"),
        ("amount", "amount"),
    ]


def test_rename_is_free_because_the_digest_is_positional():
    renamed = [
        {"source": "id", "target": "pk"},
        {"source": "amount", "target": "total"},
    ]
    dest = {"pk": "bigint", "total": "numeric(12,2)"}
    assert comparable_column_pairs(renamed, SAME, dest) == [
        ("id", "pk"),
        ("amount", "total"),
    ]


def test_widened_carrier_is_not_comparable():
    """numeric(12,2) renders 150.25 where unconstrained numeric renders 150.250."""
    widened = {"id": "bigint", "amount": "numeric"}
    assert comparable_column_pairs(IDENTITY, SAME, widened) is None


def test_transform_means_the_values_are_meant_to_differ():
    masked = [
        {"source": "id", "target": "id"},
        {"source": "amount", "target": "amount", "transform": "hash_pii"},
    ]
    assert comparable_column_pairs(masked, SAME, SAME) is None
    identity_named = [
        {"source": "id", "target": "id", "transform": "none"},
        {"source": "amount", "target": "amount", "transform": "identity"},
    ]
    assert comparable_column_pairs(identity_named, SAME, SAME) is not None


def test_declared_omission_is_not_comparable():
    """A digest over a subset would read as a full-population pass."""
    omitted = [
        {"source": "id", "target": "id"},
        {"source": "amount", "target": "", "intentional_omit": True},
    ]
    assert comparable_column_pairs(omitted, SAME, SAME) is None


def test_unknown_type_on_either_side_is_not_comparable():
    assert comparable_column_pairs(IDENTITY, SAME, {"id": "bigint"}) is None
    assert comparable_column_pairs(IDENTITY, {"id": "bigint"}, SAME) is None
    assert comparable_column_pairs([], SAME, SAME) is None


def test_supported_engines():
    assert engine_supports_checksum("postgresql")
    assert engine_supports_checksum("PostgreSQL")
    assert not engine_supports_checksum("mysql")
    assert not engine_supports_checksum("")
