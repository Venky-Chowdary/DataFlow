"""Key-generator catalog probe — seed, increment, and AUTOINCREMENT as data.

AWS DMS copies identity *values* and leaves the destination generator at 1.
A row checksum cannot see that. These tests pin the measured shape the planner
already reads via ``identity_seed_step``: a stepped IDENTITY is not IDENTITY(1,1),
and a SQLite AUTOINCREMENT column is not a plain BIGINT.
"""

from __future__ import annotations

import sqlite3

from services.identity_carry import (
    IdentityGenerator,
    IdentityGenerators,
    identity_seed_step,
    probe_identity_generators,
    stamp_identity_on_columns,
)
from services.schema_introspect import introspect_schema


class FakeCursor:
    """A DB-API cursor. Not a MagicMock: that auto-creates ``exec_driver_sql``."""

    def __init__(self, rows=(), error: Exception | None = None):
        self.rows = list(rows)
        self.error = error
        self.calls: list[tuple[str, object]] = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        if self.error is not None:
            raise self.error

    def fetchall(self):
        return list(self.rows)


class ScriptedCursor:
    """Return a different row set on each execute (sqlite_master then PRAGMA)."""

    def __init__(self, script):
        self.script = [list(rows) for rows in script]
        self.i = 0
        self.calls: list[tuple[str, object]] = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        self._current = self.script[self.i]
        self.i += 1

    def fetchall(self):
        return list(self._current)


def test_postgres_probe_reads_start_and_increment_from_pg_sequence():
    cur = FakeCursor([("id", "d", 5, 10)])
    measured = probe_identity_generators("postgresql", cur, "public", "orders")
    assert measured.status == "measured"
    spec = measured.items[0]
    assert spec.column == "id"
    assert spec.generation == "by_default"
    assert spec.start == 5
    assert spec.increment == 10
    assert spec.mechanism == "identity"


def test_postgres_serial_is_by_default_not_always():
    cur = FakeCursor([("id", "", 1, 1)])
    spec = probe_identity_generators("postgresql", cur, "public", "orders").items[0]
    assert spec.generation == "by_default"
    assert spec.mechanism == "serial"


def test_postgres_generated_always_polarity_survives():
    cur = FakeCursor([("id", "a", 1, 1)])
    spec = probe_identity_generators("postgresql", cur, "public", "orders").items[0]
    assert spec.generation == "always"


def test_sqlserver_sql_variant_bytes_decode_to_the_source_progression():
    cur = FakeCursor(
        [
            (
                "Id",
                b"\xe8\x03\x00\x00\x00\x00\x00\x00",
                b"\n\x00\x00\x00\x00\x00\x00\x00",
            )
        ]
    )
    spec = probe_identity_generators("sqlserver", cur, "dbo", "orders").items[0]
    assert spec.start == 1000
    assert spec.increment == 10


def test_oracle_identity_options_are_parsed_not_defaulted():
    cur = FakeCursor(
        [
            (
                "ID",
                "BY DEFAULT",
                "START WITH: 5, INCREMENT BY: 10, MAX_VALUE: 9999999999999999999999999999",
            )
        ]
    )
    spec = probe_identity_generators("oracle", cur, "APP", "ORDERS").items[0]
    assert spec.start == 5
    assert spec.increment == 10
    assert spec.generation == "by_default"


def test_mysql_auto_increment_is_flagged_without_inventing_a_column_step():
    cur = FakeCursor([("id",)])
    spec = probe_identity_generators("mysql", cur, "app", "orders").items[0]
    assert spec.mechanism == "auto_increment"
    assert (spec.start, spec.increment) == (1, 1)


def test_sqlite_autoincrement_is_the_integer_primary_key_column():
    cur = ScriptedCursor(
        [
            [("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",)],
            [(0, "id", "INTEGER", 1, None, 1), (1, "name", "TEXT", 0, None, 0)],
        ]
    )
    measured = probe_identity_generators("sqlite", cur, "", "t")
    assert measured.status == "measured"
    assert measured.items[0].column == "id"
    assert measured.items[0].mechanism == "autoincrement"


def test_sqlite_integer_primary_key_without_autoincrement_is_not_a_generator():
    cur = ScriptedCursor(
        [[("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",)]]
    )
    measured = probe_identity_generators("sqlite", cur, "", "t")
    assert measured.status == "measured"
    assert measured.items == ()


def test_unreadable_catalog_is_unavailable_not_empty():
    cur = FakeCursor(error=RuntimeError("permission denied"))
    measured = probe_identity_generators("postgresql", cur, "public", "orders")
    assert measured.status == "unavailable"
    assert measured.items == ()
    assert "permission denied" in measured.detail


def test_unknown_dialect_is_unavailable_not_invented():
    measured = probe_identity_generators("snowflake", FakeCursor(), "", "t")
    assert measured.status == "unavailable"
    assert "snowflake" in measured.detail


def test_failed_probe_does_not_clear_existing_identity_flags():
    cols = [
        {
            "name": "id",
            "inferred_type": "INT4 GENERATED BY DEFAULT",
            "is_identity": True,
        }
    ]
    stamp_identity_on_columns(
        cols,
        IdentityGenerators("postgresql", "unavailable", detail="catalog unreadable"),
    )
    assert cols[0]["is_identity"] is True
    assert "START WITH" not in cols[0]["inferred_type"]


def test_postgres_stamp_puts_seed_step_on_the_carrier_the_planner_parses():
    cols = [
        {
            "name": "id",
            "inferred_type": "INT4 GENERATED BY DEFAULT",
            "is_identity": True,
        }
    ]
    stamp_identity_on_columns(
        cols,
        IdentityGenerators(
            "postgresql",
            "measured",
            items=(
                IdentityGenerator(
                    column="id",
                    generation="by_default",
                    start=5,
                    increment=10,
                    mechanism="identity",
                ),
            ),
        ),
    )
    assert "(START WITH 5 INCREMENT BY 10)" in cols[0]["inferred_type"]
    assert identity_seed_step(cols[0]["inferred_type"]) == (5, 10)
    assert cols[0]["identity_increment"] == 10


def test_default_progression_is_not_stamped_onto_postgres_carriers():
    cols = [{"name": "id", "inferred_type": "SERIAL"}]
    stamp_identity_on_columns(
        cols,
        IdentityGenerators(
            "postgresql",
            "measured",
            items=(IdentityGenerator(column="id", start=1, increment=1, mechanism="serial"),),
        ),
    )
    assert cols[0]["is_identity"] is True
    assert cols[0]["inferred_type"] == "SERIAL"


def test_sqlserver_stamp_always_spells_identity_seed_step():
    cols = [{"name": "Id", "inferred_type": "BIGINT"}]
    stamp_identity_on_columns(
        cols,
        IdentityGenerators(
            "sqlserver",
            "measured",
            items=(
                IdentityGenerator(
                    column="Id", start=1, increment=1, mechanism="identity"
                ),
            ),
        ),
    )
    assert cols[0]["inferred_type"] == "BIGINT IDENTITY(1,1)"


def test_sqlite_stamp_flags_without_mutating_the_logical_type():
    cols = [{"name": "id", "inferred_type": "BIGINT"}]
    stamp_identity_on_columns(
        cols,
        IdentityGenerators(
            "sqlite",
            "measured",
            items=(
                IdentityGenerator(
                    column="id", mechanism="autoincrement"
                ),
            ),
        ),
    )
    assert cols[0]["is_identity"] is True
    assert cols[0]["inferred_type"] == "BIGINT"


def test_sqlite_introspect_flags_autoincrement_and_ignores_plain_rowid(tmp_path):
    db = tmp_path / "idprobe.db"
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(
            "CREATE TABLE with_gen (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"
        )
        conn.execute("CREATE TABLE rowid_only (id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()
    finally:
        conn.close()

    flagged = introspect_schema("sqlite", database=str(db), table="with_gen")
    assert flagged["ok"], flagged
    by_name = {c["name"]: c for c in flagged["columns"]}
    assert by_name["id"]["is_identity"] is True

    plain = introspect_schema("sqlite", database=str(db), table="rowid_only")
    assert plain["ok"], plain
    plain_id = {c["name"]: c for c in plain["columns"]}["id"]
    assert not plain_id.get("is_identity")
