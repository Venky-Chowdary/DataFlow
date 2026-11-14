"""Review of the overwrite set-aside (53351cf30): no path empties without a copy.

* Generic SQL: when the row copy could not be made, the overwrite emptied the
  live table anyway ("emptying without one"); a load that then failed left the
  destination empty with nothing to restore.
* BigQuery: a copy job leaves out rows still in the streaming buffer, and the
  live table was deleted right after the copy.
"""

from __future__ import annotations

import sqlite3

import pytest


def test_generic_overwrite_refuses_when_no_copy_can_be_made(tmp_path, monkeypatch):
    import sqlalchemy as sa

    import connectors.generic_sql as generic_sql
    from connectors.table_manager import TableDropError, retire_generic_sql_overwrite

    db = tmp_path / "g.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE orders (id int, v text)")
        conn.execute("INSERT INTO orders VALUES (1, 'kept'), (2, 'kept')")
    real = sa.create_engine(f"sqlite:///{db}")

    class _NoCopyConn:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, stmt, *a, **k):
            if "CREATE TABLE" in str(stmt).upper():
                raise sa.exc.OperationalError(str(stmt), {}, Exception("permission denied for schema"))
            return self._conn.execute(stmt, *a, **k)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    class _Engine:
        dialect = real.dialect

        def connect(self):
            outer = real.connect()

            class _Ctx:
                def __enter__(self_inner):
                    return _NoCopyConn(outer)

                def __exit__(self_inner, *exc):
                    outer.close()
                    return False

            return _Ctx()

    monkeypatch.setattr(generic_sql, "get_sqlalchemy_engine", lambda cfg: _Engine())
    with pytest.raises(TableDropError):
        retire_generic_sql_overwrite("generic_sql", {"type": "sqlite"}, "orders", None)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 2


class _Job:
    def result(self):
        return None


class _FakeBigQuery:
    """Copy jobs miss the streaming buffer; queries see it."""

    def __init__(self):
        self.tables = {"p.d.orders": {"rows": 5, "buffered": 2}}
        self.deleted: list[str] = []

    def copy_table(self, src, dst, job_config=None, **kw):
        self.tables[dst] = {"rows": self.tables[src]["rows"] - self.tables[src]["buffered"], "buffered": 0}
        return _Job()

    def query(self, sql, job_retry=None):
        dst, src = sql.split("`")[1], sql.split("`")[3]
        self.tables[dst] = {"rows": self.tables[src]["rows"], "buffered": 0}
        return _Job()

    def get_table(self, ref, **kw):
        from types import SimpleNamespace

        t = self.tables[ref]
        return SimpleNamespace(streaming_buffer={"estimated_rows": t["buffered"]} if t["buffered"] else None)

    def delete_table(self, ref, not_found_ok=True, **kw):
        self.deleted.append(ref)
        self.tables.pop(ref, None)

    def close(self):
        return None


def test_bigquery_set_aside_copies_streaming_buffer_rows_too(monkeypatch):
    from connectors import overwrite_set_aside as osa

    fake = _FakeBigQuery()
    handler = osa._BigQuery()
    monkeypatch.setattr(handler, "_open", lambda cfg, schema: (fake, "p", "d", {}))
    monkeypatch.setattr(handler, "_exists", lambda client, table_id, call: table_id in fake.tables)
    backup = handler.retire({}, "orders", "d")
    assert fake.tables[f"p.d.{backup}"]["rows"] == 5, "buffered rows must be in the backup"
