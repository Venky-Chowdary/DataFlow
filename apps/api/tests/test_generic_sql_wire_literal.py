"""The generic SQL writer decodes the wire escape before binding text (review F2).

A source text that spells a wire marker reaches every writer escaped
(``__DF_ESC____DF_SQL_NULL__``). The generic SQL writer (also behind the SQL
Server and Oracle writers) bound that escaped text as it came: the insert path
then refused the whole batch at the NULL-marker guard, and the keyed (upsert)
path stored the escape itself as data. The literal must land byte-exact on
both paths, and a real NULL must land as NULL.
"""

from __future__ import annotations

import sqlite3

import pytest

from services.value_serializer import ESCAPED_WIRE_PREFIX, SQL_NULL_SENTINEL, cell_to_string

#: ``__DF_MISSING__`` text is left out: it lands as ``''`` on every SQL writer
#: (pre-existing, filed separately), not only on this one.
LITERALS = ["__DF_SQL_NULL__", "__DF_ESC__x", "__DF_ESC____DF_SQL_NULL__", "plain"]


def _wire(value):
    return cell_to_string(value, preserve_sql_null=True)


def _write(db, rows, *, write_mode="insert"):
    from connectors.generic_sql import write_mapped_rows

    return write_mapped_rows(
        host="", port=0, database=str(db), username="", password="", schema="",
        connection_string=f"sqlite:///{db}", ssl=False, table_name="t",
        headers=["id", "name"],
        data_rows=[[_wire(i), _wire(v)] for i, v in rows],
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        column_types={"id": "INTEGER", "name": "TEXT"},
        write_mode=write_mode,
        conflict_columns=["id"] if write_mode == "upsert" else None,
        type="generic_sql",
    )


def _rows(db):
    with sqlite3.connect(db) as conn:
        return conn.execute("SELECT id, name FROM t ORDER BY id").fetchall()


def test_the_wire_spells_the_literal_escaped():
    assert _wire(SQL_NULL_SENTINEL) == ESCAPED_WIRE_PREFIX + SQL_NULL_SENTINEL
    assert _wire(None) == SQL_NULL_SENTINEL


@pytest.mark.parametrize("write_mode", ["insert", "upsert"])
def test_literals_land_byte_exact_and_null_lands_null(tmp_path, write_mode):
    db = tmp_path / "g.sqlite"
    rows = [(i + 1, v) for i, v in enumerate(LITERALS)] + [(len(LITERALS) + 1, None)]
    if write_mode == "upsert":
        # An existing table with a key, so the keyed path runs.
        with sqlite3.connect(db) as conn:
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            conn.execute("INSERT INTO t VALUES (1, 'old')")
    result = _write(db, rows, write_mode=write_mode)
    assert result.ok, result.error
    assert _rows(db) == rows
