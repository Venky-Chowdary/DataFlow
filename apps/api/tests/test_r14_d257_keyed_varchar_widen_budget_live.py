"""D257 follow-up: a keyed MySQL widen is the carrier the writer ALTERs to.

A key column ``id VARCHAR(200)`` whose source grew to ``VARCHAR(2000)`` was
offered "VARCHAR(2000) - Execute ALTERs it before writing". The keyed rule only
looked at TEXT / BLOB / JSON candidates, and MySQL cannot index 2000 utf8mb4
characters (3072-byte key limit), so the approved run never widened and a long
key failed at write. The proposal and the writer now share one budget: the
approved widen becomes VARCHAR(768), a key within it lands, and a key past it is
refused at Validate by name.

Live PostgreSQL -> MySQL through ``execute_tracked``, incremental upsert.
"""

from __future__ import annotations

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


def _contract(table: str) -> list[dict]:
    return [{"name": table, "sync_mode": "incremental_upsert", "primary_key": "id",
             "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
             "selected": True}]


def _run(fake, table, out, **kw):
    return h.execute(
        fake, h.endpoint("postgresql", table), h.endpoint("mysql", out),
        sync_mode="incremental_upsert", stream_contracts=_contract(table), **kw,
    )


def _id_type(out: str) -> str:
    rows = h.run_sql(
        "mysql",
        "SELECT column_type FROM information_schema.columns "
        f"WHERE table_schema = DATABASE() AND table_name = '{out}' AND column_name = 'id'",  # nosec B608
    )
    return str(rows[0][0]).upper()


def test_keyed_varchar_growth_widens_to_the_key_budget_and_refuses_past_it(isolated):  # noqa: F811
    if not h._port_up(3306) or not h._port_up(5432):
        pytest.skip("live PostgreSQL / MySQL not reachable")
    table = f"d257k_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (id VARCHAR(2000) PRIMARY KEY, "
                  "v INT, updated_at TIMESTAMP)")
        h.run_sql("mysql", f"CREATE TABLE {out} (id VARCHAR(200) NOT NULL PRIMARY KEY, "
                  "v INT, updated_at DATETIME(6)) DEFAULT CHARSET=utf8mb4")
        keys = ["a", "", "東京 🚀", "k" * 500]
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s, %s, %s)",  # nosec B608
                  params=[(k, i, f"2024-01-0{i + 1}") for i, k in enumerate(keys)])

        refused = _run(isolated, table, out)
        assert not refused.success
        message = refused.error or ""
        assert "VARCHAR(768)" in message and "VARCHAR(2000)" not in message, message
        assert _id_type(out) == "VARCHAR(200)"

        approved = _run(isolated, table, out, schema_drift_acknowledged=True)
        assert approved.success, approved.error
        assert _id_type(out) == "VARCHAR(768)"
        got = h.run_sql("mysql", f"SELECT id FROM {out} ORDER BY v")  # nosec B608
        assert [r[0] for r in got] == keys

        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s, 9, '2024-02-01')",  # nosec B608
                  params=[("z" * 1000,)])
        past = _run(isolated, table, out, schema_drift_acknowledged=True)
        assert not past.success
        # Refused at Validate, by column and carrier, before any row is written.
        assert "Blocking issues detected" in (past.error or ""), past.error
        assert "id VARCHAR(2000)" in past.error and "VARCHAR(768)" in past.error, past.error
        assert _id_type(out) == "VARCHAR(768)"
        assert len(h.run_sql("mysql", f"SELECT id FROM {out}")) == len(keys)  # nosec B608
    finally:
        h.drop("postgresql", table)
        h.drop("mysql", out)


def test_postgresql_approved_key_widen_alters_the_key_column(isolated):  # noqa: F811
    """PostgreSQL has no key-length budget: the approved key widen is the source's width."""
    if not h._port_up(5432):
        pytest.skip("live PostgreSQL not reachable")
    table = f"d257p_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (id VARCHAR(2000) PRIMARY KEY, "
                  "v INT, updated_at TIMESTAMP)",
                  f"CREATE TABLE {out} (id VARCHAR(200) PRIMARY KEY, v INT, updated_at TIMESTAMP)")
        keys = ["a", "", "東京 🚀", "k" * 1000]
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s, %s, %s)",  # nosec B608
                  params=[(k, i, f"2024-01-0{i + 1}") for i, k in enumerate(keys)])
        run = h.execute(
            isolated, h.endpoint("postgresql", table), h.endpoint("postgresql", out),
            sync_mode="incremental_upsert", stream_contracts=_contract(table),
            schema_drift_acknowledged=True,
        )
        assert run.success, run.error
        width = h.run_sql("postgresql", "SELECT character_maximum_length FROM "
                          f"information_schema.columns WHERE table_name = '{out}' "  # nosec B608
                          "AND column_name = 'id'")
        assert width[0][0] == 2000
        got = h.run_sql("postgresql", f"SELECT id FROM {out} ORDER BY v")  # nosec B608
        assert [r[0] for r in got] == keys
    finally:
        h.drop("postgresql", table)
        h.drop("postgresql", out)
