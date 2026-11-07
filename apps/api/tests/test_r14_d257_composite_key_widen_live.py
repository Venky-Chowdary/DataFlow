"""D257 review: the growing parts of one MySQL key are budgeted together.

PRIMARY KEY(a, b), both VARCHAR(100), from a PostgreSQL source with both at
VARCHAR(2000): each part was budgeted alone (668 + 668 characters = 5344 bytes),
``a`` was MODIFIED and committed and ``b`` then failed with a raw 1071, leaving
the key half-migrated. The parts now share the 3072-byte budget (384 + 384) and
MySQL gets one ALTER TABLE for both. A key with no room left is refused at
Validate before any DDL.

Live PostgreSQL -> MySQL through ``execute_tracked``, incremental upsert.
"""

from __future__ import annotations

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


def _contract(table: str) -> list[dict]:
    return [{"name": table, "sync_mode": "incremental_upsert", "primary_key": "a,b",
             "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
             "selected": True}]


def _run(fake, table, out, **kw):
    return h.execute(
        fake, h.endpoint("postgresql", table), h.endpoint("mysql", out),
        sync_mode="incremental_upsert", stream_contracts=_contract(table), **kw,
    )


def _types(out: str) -> dict[str, str]:
    rows = h.run_sql(
        "mysql",
        "SELECT column_name, column_type FROM information_schema.columns "
        f"WHERE table_schema = DATABASE() AND table_name = '{out}'",  # nosec B608
    )
    return {str(c).lower(): str(t).upper() for c, t in rows}


@pytest.fixture()
def live():
    if not h._port_up(3306) or not h._port_up(5432):
        pytest.skip("live PostgreSQL / MySQL not reachable")


def test_both_growing_key_parts_share_the_budget_in_one_alter(isolated, live):  # noqa: F811
    table = f"d257c_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (a VARCHAR(2000), b VARCHAR(2000), "
                  "v INT, updated_at TIMESTAMP, PRIMARY KEY (a, b))")
        h.run_sql("mysql", f"CREATE TABLE {out} (a VARCHAR(100) NOT NULL, b VARCHAR(100) NOT NULL, "
                  "v INT, updated_at DATETIME(6), PRIMARY KEY (a, b)) DEFAULT CHARSET=utf8mb4")
        rows = [("x", "", 1), ("東京 🚀", "y", 2), ("a" * 300, "b" * 384, 3)]
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s, %s, %s, '2024-01-01')",  # nosec B608
                  params=rows)
        refused = _run(isolated, table, out)
        assert not refused.success
        assert "VARCHAR(384)" in (refused.error or ""), refused.error
        approved = _run(isolated, table, out, schema_drift_acknowledged=True)
        assert approved.success, approved.error
        types = _types(out)
        assert types["a"].startswith("VARCHAR(384)") and types["b"].startswith("VARCHAR(384)"), types
        got = h.run_sql("mysql", f"SELECT a, b, v FROM {out} ORDER BY v")  # nosec B608
        assert [tuple(r) for r in got] == rows
    finally:
        h.drop("postgresql", table)
        h.drop("mysql", out)


def test_a_key_with_no_room_is_refused_at_validate_before_any_ddl(isolated, live):  # noqa: F811
    table = f"d257n_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (a VARCHAR(2000), b VARCHAR(668), "
                  "v INT, updated_at TIMESTAMP, PRIMARY KEY (a, b))")
        # 100 + 668 utf8mb4 characters = 3072 bytes: the key is full.
        h.run_sql("mysql", f"CREATE TABLE {out} (a VARCHAR(100) NOT NULL, b VARCHAR(668) NOT NULL, "
                  "v INT, updated_at DATETIME(6), PRIMARY KEY (a, b)) DEFAULT CHARSET=utf8mb4")
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s, 'k', 1, '2024-01-01')",  # nosec B608
                  params=[("a" * 150,)])
        before = _types(out)
        refused = _run(isolated, table, out, schema_drift_acknowledged=True)
        assert not refused.success
        assert "Blocking issues detected" in (refused.error or ""), refused.error
        assert "a VARCHAR(2000)" in refused.error, refused.error
        assert _types(out) == before
        assert h.run_sql("mysql", f"SELECT COUNT(*) FROM {out}")[0][0] == 0  # nosec B608
    finally:
        h.drop("postgresql", table)
        h.drop("mysql", out)
