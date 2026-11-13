"""SQLite upsert updates the mapped columns in place; it never DELETEs.

Defect: ``sqlite_writer._sqlite_upsert_batch`` used ``INSERT … ON CONFLICT``
only when ``_df_lsn`` was mapped. Every other upsert DELETEd the matching
keys and re-INSERTed the row: destination columns the mapping does not write
were reset to NULL/DEFAULT (contract G15: dest-only columns stay off SET),
``ON DELETE CASCADE`` children were deleted, and the rowid changed.
"""

from __future__ import annotations

import sqlite3

from connectors.sqlite_writer import write_mapped_rows

_MAP = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


def _upsert(db, rows, table="ws_cust"):
    return write_mapped_rows(
        host="",
        port=0,
        database=str(db),
        username="",
        password="",
        schema="",
        connection_string="",
        ssl=False,
        table_name=table,
        headers=["id", "name"],
        data_rows=rows,
        mappings=_MAP,
        column_types={"id": "INTEGER", "name": "VARCHAR"},
        create_table=False,
        write_mode="upsert",
        conflict_columns=["id"],
    )


def test_upsert_keeps_unmapped_columns_and_cascade_children(tmp_path):
    db = tmp_path / "ws_up.db"
    c = sqlite3.connect(db)
    c.execute("PRAGMA foreign_keys=ON")
    c.execute("create table ws_cust(id integer primary key, name text, notes text)")
    c.execute(
        "create table ws_orders(oid integer primary key, "
        "cust_id integer references ws_cust(id) on delete cascade)"
    )
    c.execute("insert into ws_cust values(1,'old','VIP - do not churn')")
    c.execute("insert into ws_orders values(10,1)")
    c.commit()
    c.close()

    r = _upsert(db, [["1", "new"], ["2", "fresh"]])
    assert r.ok, r.error
    assert r.rows_written == 2

    c = sqlite3.connect(db)
    rows = sorted(c.execute("select id, name, notes from ws_cust"))
    orders = list(c.execute("select oid, cust_id from ws_orders"))
    c.close()
    assert rows == [(1, "new", "VIP - do not churn"), (2, "fresh", None)]
    assert orders == [(10, 1)]


def test_upsert_without_unique_key_updates_in_place(tmp_path):
    """No PRIMARY KEY / UNIQUE on the key: UPDATE-then-INSERT, rowid stays."""
    db = tmp_path / "ws_nokey.db"
    c = sqlite3.connect(db)
    c.execute("create table ws_cust(id integer, name text, notes text)")
    c.execute("insert into ws_cust(rowid, id, name, notes) values(7, 1,'old','keep me')")
    c.commit()
    c.close()

    r = _upsert(db, [["1", "new"], ["3", "third"]])
    assert r.ok, r.error

    c = sqlite3.connect(db)
    rows = sorted(c.execute("select rowid, id, name, notes from ws_cust where id = 1"))
    third = list(c.execute("select id, name, notes from ws_cust where id = 3"))
    indexes = list(c.execute("select name from sqlite_master where type = 'index'"))
    c.close()
    assert rows == [(7, 1, "new", "keep me")]
    assert third == [(3, "third", None)]
    # No silent DDL on the operator's table.
    assert indexes == []
