"""D93 — generated and identity source columns are not a fidelity collapse.

Introspection types a PostgreSQL identity key ``BIGINT GENERATED ALWAYS`` and a
computed column ``DECIMAL(12,2) GENERATED ALWAYS``. Both are a DDL property of
the source (it refuses client writes), not part of the value: every value lands
exactly in a plain destination column. Grading the suffix as a collapse refused
every strict transfer of a table with a modern identity primary key unless a
Risk Contract was signed. Writing *into* a GENERATED ALWAYS destination still
fails at the engine, so that pair stays refused.

Live PostgreSQL source -> new SQLite table (run 1 creates, run 2 appends into
it) and -> new PostgreSQL table, preflight ON, strict.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal

import pytest

from services.type_coercion import is_lossy_coercion, is_precision_collapse_coercion
from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


@pytest.mark.parametrize(
    ("source", "target", "dest"),
    [
        ("BIGINT GENERATED ALWAYS", "BIGINT", "postgresql"),
        ("INT GENERATED ALWAYS AS IDENTITY", "INTEGER", "postgresql"),
        ("DECIMAL(12,2) GENERATED ALWAYS", "NUMERIC(12,2)", "postgresql"),
        ("DECIMAL(12,2) GENERATED ALWAYS", "TEXT", "sqlite"),
        ("BIGINT IDENTITY(1,1)", "BIGINT", "sqlserver"),
    ],
)
def test_generation_clause_is_not_part_of_the_value(source, target, dest):
    assert is_lossy_coercion(source, target, dest_db=dest) is False
    assert is_precision_collapse_coercion(source, target, dest_db=dest) is False


def test_writing_into_a_generated_destination_still_refuses():
    assert is_precision_collapse_coercion("INTEGER", "INTEGER GENERATED ALWAYS") is True


_ROWS = [
    (Decimal("12.50"), 2, "Ada"),
    (Decimal("0.00"), 0, ""),
    (Decimal("7.25"), 3, "   "),
    (Decimal("1.10"), 1, "東京 🚀"),
    (Decimal("3.33"), 3, None),
]


def _seed_pg(table: str) -> None:
    h.run_sql(
        "postgresql",
        f"CREATE TABLE {table} (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
        "price NUMERIC(10,2) NOT NULL, qty INT NOT NULL, note TEXT, "
        "total DECIMAL(12,2) GENERATED ALWAYS AS (price * qty) STORED)",
    )
    h.run_sql(
        "postgresql",
        f"INSERT INTO {table} (price, qty, note) VALUES (%s, %s, %s)",  # nosec B608
        params=_ROWS,
    )


def _expected(rows=_ROWS, start=1):
    return [
        (i, price, qty, note, price * qty)
        for i, (price, qty, note) in enumerate(rows, start=start)
    ]


def test_identity_and_computed_columns_load_into_new_sqlite_twice(tmp_path, isolated):  # noqa: F811
    table = f"d93_{h.token()}"
    dst = tmp_path / "dst.sqlite"
    try:
        _seed_pg(table)
        first = h.execute(
            isolated, h.endpoint("postgresql", table), h.sqlite_endpoint(dst, table),
            sync_mode="full_refresh_overwrite",
        )
        assert first.success, first.error
        more = [(Decimal("9.99"), 4, "Zoë")]
        h.run_sql(
            "postgresql",
            f"INSERT INTO {table} (price, qty, note) VALUES (%s, %s, %s)",  # nosec B608
            params=more,
        )
        second = h.execute(
            isolated, h.endpoint("postgresql", table), h.sqlite_endpoint(dst, table),
            sync_mode="full_refresh_overwrite",
        )
        assert second.success, second.error
        got = sqlite3.connect(dst).execute(
            f"SELECT id, price, qty, note, total FROM {table} ORDER BY id"  # nosec B608
        ).fetchall()
        want = _expected(_ROWS + more)
        assert [(r[0], Decimal(str(r[1])), r[2], r[3], Decimal(str(r[4]))) for r in got] == [
            (i, p, q, n, t) for i, p, q, n, t in want
        ]
    finally:
        h.drop("postgresql", table)


def test_identity_and_computed_columns_load_into_new_postgres(isolated):  # noqa: F811
    table = f"d93_{h.token()}"
    out = f"{table}_out"
    try:
        _seed_pg(table)
        result = h.execute(
            isolated, h.endpoint("postgresql", table), h.endpoint("postgresql", out),
            sync_mode="full_refresh_overwrite",
        )
        assert result.success, result.error
        got = h.run_sql(
            "postgresql", f"SELECT id, price, qty, note, total FROM {out} ORDER BY id"  # nosec B608
        )
        assert got == _expected()
    finally:
        h.drop("postgresql", table)
        h.drop("postgresql", out)
