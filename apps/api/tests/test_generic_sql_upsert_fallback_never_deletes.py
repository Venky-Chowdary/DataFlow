"""Generic SQL upsert fallback is UPDATE-then-INSERT, never DELETE, and is a warning.

Defect: ``generic_sql._upsert_batch`` caught any native MERGE / ON CONFLICT
error (no unique index on the key, a stage-table privilege, an old server),
logged it at DEBUG and fell back to ``_delete_by_keys`` + INSERT for SQL
Server, Oracle, DuckDB, DB2, HANA, Vertica, Netezza, Informix, Firebird and
Sybase. Deleting the matched keys reset every destination column the mapping
does not write (contract G15), fired ON DELETE CASCADE and delete triggers,
and the run looked like a clean upsert.

The dialect's native SQL cannot run against SQLite, so passing its dialect
name with a SQLite connection drives exactly the "native upsert failed"
branch each of those destinations takes.
"""

from __future__ import annotations

import sqlite3

import pytest
import sqlalchemy as sa

from connectors.generic_sql import _upsert_batch
from connectors.upsert_fallback import upsert_fallback_warnings

MERGE_DIALECTS = [
    "mssql",
    "oracle",
    "duckdb",
    "ibm_db_sa",
    "hana",
    "vertica",
    "netezza",
    "informix",
    "firebird",
    "sybase",
]


def _seed(path):
    c = sqlite3.connect(path)
    # No PRIMARY KEY / UNIQUE on id: the native upsert has nothing to match.
    c.execute("create table ws_cust(id integer, name text, notes text)")
    c.execute("create table ws_orders(oid integer, cust_id integer)")
    c.execute("create table ws_deleted(id integer)")
    c.execute(
        "create trigger ws_cust_del after delete on ws_cust begin "
        "insert into ws_deleted values (old.id); "
        "delete from ws_orders where cust_id = old.id; end"
    )
    c.execute("insert into ws_cust(rowid, id, name, notes) values (5, 1, 'old', 'VIP')")
    c.execute("insert into ws_orders values (10, 1)")
    c.commit()
    c.close()


@pytest.mark.parametrize("dialect_name", MERGE_DIALECTS)
def test_native_merge_failure_updates_in_place(tmp_path, dialect_name):
    db = tmp_path / "ws_fallback.db"
    _seed(db)
    engine = sa.create_engine(f"sqlite:///{db}")
    meta = sa.MetaData()
    # The writer's table object carries the mapped columns only.
    table = sa.Table("ws_cust", meta, sa.Column("id", sa.Integer), sa.Column("name", sa.Text))
    # SQLite reads an unknown double-quoted name as a string literal, so the
    # mirror-lattice probe would report ``_deleted`` here; pin "no lattice".
    table.info["df_mirror_lattice"] = ()
    try:
        with engine.connect() as conn:
            n = _upsert_batch(
                conn,
                table,
                [{"id": 1, "name": "new"}, {"id": 2, "name": "fresh"}],
                ["id"],
                ["id", "name"],
                dialect_name,
            )
            conn.commit()
    finally:
        engine.dispose()
    assert n == 2

    c = sqlite3.connect(db)
    rows = sorted(c.execute("select rowid, id, name, notes from ws_cust"))
    deleted = list(c.execute("select id from ws_deleted"))
    orders = list(c.execute("select oid, cust_id from ws_orders"))
    c.close()
    assert deleted == [], "upsert fallback must never DELETE"
    assert orders == [(10, 1)]
    assert rows[0] == (5, 1, "new", "VIP")
    assert [r[1:] for r in rows[1:]] == [(2, "fresh", None)]

    warnings = upsert_fallback_warnings(table)
    assert len(warnings) == 1
    assert dialect_name in warnings[0]
    assert "UPDATE" in warnings[0]


def test_fallback_is_a_job_warning_end_to_end(tmp_path, monkeypatch):
    """write_mapped_rows surfaces the fallback on WriteResult.warnings."""
    import services.mirror_engine as mirror_engine
    from connectors.generic_sql import write_mapped_rows

    db = tmp_path / "ws_e2e.db"
    _seed(db)
    monkeypatch.setattr(mirror_engine, "_probe_physical_lattice", lambda conn, table_obj: ())
    result = write_mapped_rows(
        host="",
        port=0,
        database=str(db),
        username="",
        password="",
        schema="",
        connection_string=f"sqlite:///{db}",
        ssl=False,
        table_name="ws_cust",
        headers=["id", "name"],
        data_rows=[["1", "new"], ["2", "fresh"]],
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        column_types={"id": "INTEGER", "name": "VARCHAR"},
        create_table=False,
        write_mode="upsert",
        conflict_columns=["id"],
        type="sqlite",
    )
    assert result.ok, result.error
    assert result.rows_written == 2

    c = sqlite3.connect(db)
    rows = sorted(c.execute("select rowid, id, name, notes from ws_cust"))
    deleted = list(c.execute("select id from ws_deleted"))
    c.close()
    assert deleted == []
    assert rows[0] == (5, 1, "new", "VIP")
    assert any("UPDATE of matched keys" in w for w in result.warnings), result.warnings
