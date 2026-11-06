"""D257 review F7: a widen that waits on another session's lock retries, then refuses.

The MySQL combined MODIFY returned [] on a lock timeout and PostgreSQL's widen
loop skipped "lock timeout": one transient lock skipped every widen for the
table and the run loaded against the narrow columns it had been approved to
widen. A MySQL widen now asks again a bounded number of times (a failed
ALTER changes nothing) and then refuses by name; a PostgreSQL lock timeout
(which aborts the transaction) is refused. Never a silent skip.

Live PostgreSQL -> MySQL / PostgreSQL with a second session holding the
destination's metadata lock.
"""

from __future__ import annotations

import threading

import pytest

from tests import r13_mapping_harness as h
from tests.r13_mapping_harness import isolated  # noqa: F401 - fixture

pytestmark = pytest.mark.timeout(300)


@pytest.fixture()
def live(monkeypatch):
    if not h._port_up(3306) or not h._port_up(5432):
        pytest.skip("live PostgreSQL / MySQL not reachable")
    monkeypatch.setenv("DATAFLOW_DEST_LOCK_WAIT_SECONDS", "5")
    monkeypatch.setenv("DATAWRAP_DEST_LOCK_WAIT_SECONDS", "5")


def _contract(table: str) -> list[dict]:
    return [{"name": table, "sync_mode": "incremental_upsert", "primary_key": "id",
             "cursor_field": "updated_at", "cursor_semantics": "modification_timestamp",
             "selected": True}]


def _setup(dest: str, table: str, out: str) -> None:
    h.run_sql("postgresql", f"CREATE TABLE {table} (id INT PRIMARY KEY, name VARCHAR(200), "
              "updated_at TIMESTAMP)")
    h.run_sql("postgresql", f"INSERT INTO {table} VALUES (1, %s, '2024-01-01'), "  # nosec B608
              "(2, '', '2024-01-02')", params=[("Zoë " + "y" * 150,)])
    ts = "DATETIME(6)" if dest == "mysql" else "TIMESTAMP"
    h.run_sql(dest, f"CREATE TABLE {out} (id INT PRIMARY KEY, name VARCHAR(20), updated_at {ts})")


def _hold_lock(dest: str, out: str):
    """A second session that has read the table: it holds the metadata lock until commit."""
    conn = h.connect(dest)
    conn.autocommit = False
    cur = conn.cursor()
    if dest == "mysql":
        cur.execute("START TRANSACTION")
    cur.execute(f"SELECT * FROM {h.quote(dest, out)}")  # nosec B608
    cur.fetchall()
    return conn


def _name_type(dest: str, out: str) -> str:
    rows = h.run_sql(dest, "SELECT character_maximum_length FROM information_schema.columns "
                     f"WHERE table_name = '{out}' AND column_name = 'name'")  # nosec B608
    return str(rows[0][0])


def _run(fake, table, out, dest):
    return h.execute(
        fake, h.endpoint("postgresql", table), h.endpoint(dest, out),
        sync_mode="incremental_upsert", stream_contracts=_contract(table),
        schema_drift_acknowledged=True,
    )


def test_mysql_widen_retries_a_lock_that_clears(isolated, live):  # noqa: F811
    table, out = f"lk_{h.token()}", f"lk_{h.token()}_d"
    try:
        _setup("mysql", table, out)
        holder = _hold_lock("mysql", out)
        threading.Timer(6.0, holder.commit).start()
        run = _run(isolated, table, out, "mysql")
        holder.close()
        assert run.success, run.error
        assert _name_type("mysql", out) == "200"
        assert len(h.run_sql("mysql", f"SELECT id FROM {out}")) == 2  # nosec B608
    finally:
        h.drop("postgresql", table)
        h.drop("mysql", out)


@pytest.mark.parametrize("dest", ["mysql", "postgresql"])
def test_a_lock_that_never_clears_refuses_the_widen_by_name(isolated, live, dest):  # noqa: F811
    table, out = f"lk_{h.token()}", f"lk_{h.token()}_d"
    holder = None
    try:
        _setup(dest, table, out)
        holder = _hold_lock(dest, out)
        run = _run(isolated, table, out, dest)
        assert not run.success
        assert "another session held a lock" in (run.error or ""), run.error
        holder.commit()
        assert _name_type(dest, out) == "20"
        assert h.run_sql(dest, f"SELECT COUNT(*) FROM {h.quote(dest, out)}")[0][0] == 0  # nosec B608
    finally:
        if holder is not None:
            holder.close()
        h.drop("postgresql", table)
        h.drop(dest, out)
