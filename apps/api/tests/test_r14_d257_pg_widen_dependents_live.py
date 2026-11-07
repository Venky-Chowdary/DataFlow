"""D257 review S4: PostgreSQL names what blocks a widen instead of skipping it.

``ALTER ... TYPE`` on a column a view, rule or stored generated column reads
fails with "cannot alter type of a column used by ...". The widen pass skipped
"cannot alter" / "not supported", so the run loaded against the narrow
column. Every column PostgreSQL would widen is now checked first and the
refusal names the dependent object; nothing is altered or written.

Live PostgreSQL -> PostgreSQL through ``execute_tracked``.
"""

from __future__ import annotations

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


@pytest.mark.parametrize("dependent", ["view", "generated column"])
def test_a_dependent_object_refuses_the_widen_by_name(isolated, dependent):  # noqa: F811
    if not h._port_up(5432):
        pytest.skip("live PostgreSQL not reachable")
    table, out = f"pd_{h.token()}", f"pd_{h.token()}_d"
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(200), "
                  "updated_at TIMESTAMP)")
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (1, %s, '2024-01-01'), "  # nosec B608
                  "(2, '', '2024-01-02')", params=[("Zoë " + "y" * 150,)])
        if dependent == "view":
            h.run_sql("postgresql", f"CREATE TABLE {out} (id INT PRIMARY KEY, name VARCHAR(20), "
                      "updated_at TIMESTAMP)", f"CREATE VIEW {out}_v AS SELECT name FROM {out}")
            expected = f"view {out}_v"
        else:
            h.run_sql("postgresql", f"CREATE TABLE {out} (id INT PRIMARY KEY, name VARCHAR(20), "
                      "updated_at TIMESTAMP, shout TEXT GENERATED ALWAYS AS (upper(name)) STORED)")
            expected = "generated column shout"
        run = h.execute(
            isolated, h.endpoint("postgresql", table), h.endpoint("postgresql", out),
            sync_mode="incremental_upsert",
            stream_contracts=[{"name": table, "sync_mode": "incremental_upsert", "primary_key": "id",
                               "cursor_field": "updated_at",
                               "cursor_semantics": "modification_timestamp", "selected": True}],
            schema_drift_acknowledged=True,
        )
        assert not run.success
        assert "Could not widen column name" in (run.error or ""), run.error
        assert expected in run.error, run.error
        width = h.run_sql("postgresql", "SELECT character_maximum_length FROM information_schema."
                          f"columns WHERE table_name = '{out}' AND column_name = 'name'")  # nosec B608
        assert width[0][0] == 20
        assert h.run_sql("postgresql", f"SELECT COUNT(*) FROM {out}")[0][0] == 0  # nosec B608
    finally:
        h.run_sql("postgresql", f"DROP VIEW IF EXISTS {out}_v")
        h.drop("postgresql", table)
        h.drop("postgresql", out)
