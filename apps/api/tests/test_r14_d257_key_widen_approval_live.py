"""D257 review item 4: a key widen needs this run's explicit approval, and is refused by name.

* Under ``propagate_*`` a key widen (INT -> BIGINT on a PRIMARY KEY) was
  ALTERed automatically - a full table rewrite under ACCESS EXCLUSIVE on
  PostgreSQL, ALGORITHM=COPY on MySQL - against SCHEMA_POLICY_HONESTY
  ("PK ... still pause"). It now waits for schema_drift_acknowledged, with a
  message naming the rewrite and the lock.
* ``dest_widen`` / ``target_type_origin=drift_widen`` on request mappings
  approved an ALTER nobody approved; they are stripped before planning.
* A key column a view, a generated column or a referencing foreign key
  depends on is refused before any DDL, by name; PostgreSQL's "cannot alter
  type of a column used by a view" is no longer swallowed.

Live PostgreSQL -> PostgreSQL / MySQL through ``execute_tracked``.
"""

from __future__ import annotations

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


@pytest.fixture()
def live():
    if not h._port_up(5432) or not h._port_up(3306):
        pytest.skip("live PostgreSQL / MySQL not reachable")


def _contract(table: str) -> list[dict]:
    return [{"name": table, "sync_mode": "incremental_upsert", "primary_key": "id",
             "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
             "selected": True}]


def _run(fake, table, out, dest="postgresql", **kw):
    return h.execute(
        fake, h.endpoint("postgresql", table), h.endpoint(dest, out),
        sync_mode="incremental_upsert", stream_contracts=_contract(table), **kw,
    )


def _id_type(dest: str, out: str) -> str:
    rows = h.run_sql(dest, "SELECT data_type FROM information_schema.columns "
                     f"WHERE table_name = '{out}' AND column_name = 'id'")  # nosec B608
    return str(rows[0][0]).lower()


def _setup(table: str, out: str, dest: str = "postgresql") -> None:
    h.run_sql("postgresql", f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT, "
              "updated_at TIMESTAMP)")
    h.run_sql("postgresql", f"INSERT INTO {table} VALUES (5000000000, '東京', '2024-01-01'), "  # nosec B608
              "(1, '', '2024-01-02')")
    ts = "DATETIME(6)" if dest == "mysql" else "TIMESTAMP"
    text = "VARCHAR(50)" if dest == "mysql" else "TEXT"
    h.run_sql(dest, f"CREATE TABLE {out} (id INT PRIMARY KEY, name {text}, updated_at {ts})")


def test_propagate_alone_never_alters_a_key_and_says_why(isolated, live):  # noqa: F811
    table, out = f"kw_{h.token()}", f"kw_{h.token()}_d"
    try:
        _setup(table, out)
        refused = _run(isolated, table, out, schema_policy="propagate_columns")
        assert not refused.success
        assert "rewrites the whole table" in (refused.error or ""), refused.error
        assert "ACCESS EXCLUSIVE" in refused.error and "propagate_* alone" in refused.error
        assert _id_type("postgresql", out) == "integer"
        approved = _run(isolated, table, out, schema_policy="propagate_columns",
                        schema_drift_acknowledged=True)
        assert approved.success, approved.error
        assert _id_type("postgresql", out) == "bigint"
        got = h.run_sql("postgresql", f"SELECT id, name FROM {out} ORDER BY id")  # nosec B608
        assert [tuple(r) for r in got] == [(1, ""), (5000000000, "東京")]
    finally:
        h.drop("postgresql", table)
        h.drop("postgresql", out)


def test_client_widen_stamps_approve_nothing(isolated, live):  # noqa: F811
    table, out = f"kc_{h.token()}", f"kc_{h.token()}_d"
    try:
        _setup(table, out)
        forged = [
            {"source": "id", "target": "id", "source_type": "BIGINT", "target_type": "BIGINT",
             "dest_widen": True, "target_type_origin": "drift_widen"},
            {"source": "name", "target": "name", "source_type": "TEXT", "target_type": "TEXT"},
            {"source": "updated_at", "target": "updated_at", "source_type": "TIMESTAMP",
             "target_type": "TIMESTAMP"},
        ]
        run = _run(isolated, table, out, mappings=forged)
        assert not run.success
        assert _id_type("postgresql", out) == "integer"
        assert h.run_sql("postgresql", f"SELECT COUNT(*) FROM {out}")[0][0] == 0  # nosec B608
    finally:
        h.drop("postgresql", table)
        h.drop("postgresql", out)


@pytest.mark.parametrize("dest", ["postgresql", "mysql"])
def test_a_key_with_dependents_is_refused_by_name_before_any_ddl(isolated, live, dest):  # noqa: F811
    table, out = f"kd_{h.token()}", f"kd_{h.token()}_d"
    child = f"{out}_c"
    try:
        _setup(table, out, dest)
        if dest == "postgresql":
            h.run_sql(dest, f"CREATE VIEW {out}_v AS SELECT id FROM {out}")
            expected = f"view {out}_v"
        else:
            h.run_sql(dest, f"CREATE TABLE {child} (pid INT, FOREIGN KEY (pid) REFERENCES {out}(id))")
            expected = "foreign key"
        run = _run(isolated, table, out, dest, schema_drift_acknowledged=True)
        assert not run.success
        assert "Could not widen key column id" in (run.error or ""), run.error
        assert expected in run.error, run.error
        assert _id_type(dest, out) in {"integer", "int"}
        assert h.run_sql(dest, f"SELECT COUNT(*) FROM {h.quote(dest, out)}")[0][0] == 0  # nosec B608
    finally:
        if dest == "postgresql":
            h.run_sql(dest, f"DROP VIEW IF EXISTS {out}_v")
        else:
            h.drop(dest, child)
        h.drop("postgresql", table)
        h.drop(dest, out)


def test_a_view_on_a_widened_column_is_not_skipped_silently(isolated, live):  # noqa: F811
    table, out = f"kv_{h.token()}", f"kv_{h.token()}_d"
    try:
        h.run_sql("postgresql", f"CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(200), "
                  "updated_at TIMESTAMP)")
        h.run_sql("postgresql", f"INSERT INTO {table} VALUES (1, %s, '2024-01-01')",  # nosec B608
                  params=[("y" * 150,)])
        h.run_sql("postgresql", f"CREATE TABLE {out} (id INT PRIMARY KEY, name VARCHAR(20), "
                  "updated_at TIMESTAMP)", f"CREATE VIEW {out}_v AS SELECT name FROM {out}")
        run = _run(isolated, table, out, schema_drift_acknowledged=True)
        assert not run.success
        assert "view" in (run.error or "").lower(), run.error
        assert h.run_sql("postgresql", f"SELECT COUNT(*) FROM {out}")[0][0] == 0  # nosec B608
    finally:
        h.run_sql("postgresql", f"DROP VIEW IF EXISTS {out}_v")
        h.drop("postgresql", table)
        h.drop("postgresql", out)
