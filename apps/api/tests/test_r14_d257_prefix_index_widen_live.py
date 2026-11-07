"""D257 review item 3: a MySQL prefix index does not cap the column it prefixes.

``KEY ix_name (name(32))`` indexes 32 characters whatever the column's width,
so ``name VARCHAR(100)`` widened to the source's VARCHAR(2000) before
f51b52202 and was refused after it: neither the writer's index read nor the
proposal's key groups knew SUB_PART. A prefix part now costs its prefix bytes
in any key it shares, and a column whose every index is a prefix index widens
as asked.

Live PostgreSQL -> MySQL through ``execute_tracked``.
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


def test_a_prefix_indexed_column_widens_to_the_source_width(isolated):  # noqa: F811
    if not h._port_up(3306) or not h._port_up(5432):
        pytest.skip("live PostgreSQL / MySQL not reachable")
    table = f"px_{h.token()}"
    out = f"{table}_d"
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(2000), "
                  "updated_at TIMESTAMP)")
        h.run_sql("mysql", f"CREATE TABLE {out} (id INT PRIMARY KEY, name VARCHAR(100), "
                  "updated_at DATETIME(6), KEY ix_name (name(32))) DEFAULT CHARSET=utf8mb4")
        rows = [(1, ""), (2, "東京 🚀"), (3, "n" * 1800), (4, None)]
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (%s, %s, '2024-01-01')",  # nosec B608
                  params=rows)
        refused = h.execute(
            isolated, h.endpoint("postgresql", table), h.endpoint("mysql", out),
            sync_mode="incremental_upsert", stream_contracts=_contract(table),
        )
        assert "VARCHAR(2000)" in (refused.error or ""), refused.error
        run = h.execute(
            isolated, h.endpoint("postgresql", table), h.endpoint("mysql", out),
            sync_mode="incremental_upsert", stream_contracts=_contract(table),
            schema_drift_acknowledged=True,
        )
        assert run.success, run.error
        width = h.run_sql("mysql", "SELECT character_maximum_length FROM information_schema.columns "
                          f"WHERE table_schema = DATABASE() AND table_name = '{out}' "  # nosec B608
                          "AND column_name = 'name'")
        assert int(width[0][0]) == 2000
        got = h.run_sql("mysql", f"SELECT id, name FROM {out} ORDER BY id")  # nosec B608
        assert [tuple(r) for r in got] == rows
    finally:
        h.drop("postgresql", table)
        h.drop("mysql", out)


def test_a_prefix_part_costs_its_prefix_in_a_shared_key():
    from connectors.schema_drift import mysql_keyed_widen_carrier
    from services.schema_drift import dest_key_groups

    meta = {"items": [{"name": "ix", "columns": [{"name": "name", "prefix_length": 32},
                                                  {"name": "code"}]}]}
    groups = dest_key_groups(None, None, None, meta)
    assert groups == [["name(32)", "code"]]
    live = {"name": "VARCHAR(100)", "code": "VARCHAR(100)"}
    # 3072 - 32 * 4 = 2944 bytes left: code gets 736 characters.
    assert mysql_keyed_widen_carrier(groups, "code", "VARCHAR(2000)", live) == ("VARCHAR(736)", False)
    # name sits in the key only as a prefix: it widens as asked.
    assert mysql_keyed_widen_carrier(groups, "name", "VARCHAR(2000)", live) == ("VARCHAR(2000)", False)
