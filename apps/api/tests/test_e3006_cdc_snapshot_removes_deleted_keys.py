"""E3-006 / D196 — a CDC snapshot into a populated destination removes keys the source deleted.

Repro (live SQL Server change tracking -> PostgreSQL, and MySQL binlog ->
PostgreSQL): load the table with a plain overwrite, then on the source update
id 1, NULL a cell, delete id 2, insert id 101, and run CDC. On 3e258e90f the
CDC run snapshots (no stored position for the route), upserts the current
image and reports success — with id 2 still on the destination. A restart
whose slot/position is gone re-snapshots the same way (QA E3-006: "deleted 30
remains, source 54, dest 55").

The fix sweeps, after a *complete* snapshot and before streaming, every
destination key the snapshot did not present (services/cdc_snapshot_sweep.py,
mirror kernel hard delete).
"""

from __future__ import annotations

import os
import uuid

import pytest

from tests.test_d196_sqlserver_change_tracking_cdc_live import (  # noqa: F401 — fixture
    _DESTS,
    _MS,
    _dest_conn,
    _exec,
    _fetch,
    _ms,
    _up,
    isolated,
)

_MY = {
    "host": os.environ.get("DATAFLOW_TEST_MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("DATAFLOW_TEST_MYSQL_PORT", "3306")),
    "database": os.environ.get("DATAFLOW_TEST_MYSQL_DB", "dataflow"),
    "username": os.environ.get("DATAFLOW_TEST_MYSQL_USER", "root"),
    "password": os.environ.get("DATAFLOW_TEST_MYSQL_PASSWORD", ""),
}
_ROWS = [(1, "alpha", "note1"), (2, "bravo", "note2"), (3, "naïve 中文 ✓", None), (4, "", "  "), (5, None, "x")]


def _src_conn(engine):
    if engine == "sqlserver":
        return _ms()
    import pymysql

    return pymysql.connect(host=_MY["host"], port=_MY["port"], user=_MY["username"],
                           password=_MY["password"], database=_MY["database"],
                           autocommit=True, charset="utf8mb4")


def _q(engine, table):
    return f"dbo.[{table}]" if engine == "sqlserver" else f"`{table}`"


def _create(engine, table):
    conn = _src_conn(engine)
    try:
        if engine == "sqlserver":
            _exec(conn, "IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID()) "
                        "ALTER DATABASE CURRENT SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)")
            _exec(conn, f"CREATE TABLE {_q(engine, table)} (id INT NOT NULL PRIMARY KEY, name NVARCHAR(80) NULL, note NVARCHAR(80) NULL)")
            _exec(conn, f"ALTER TABLE {_q(engine, table)} ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON)")
            _exec(conn, f"INSERT INTO {_q(engine, table)} (id, name, note) VALUES (?, ?, ?)", _ROWS)
        else:
            cur = conn.cursor()
            cur.execute(f"CREATE TABLE {_q(engine, table)} (id INT PRIMARY KEY, name VARCHAR(80) NULL, note VARCHAR(80) NULL) CHARSET utf8mb4")
            cur.executemany(f"INSERT INTO {_q(engine, table)} VALUES (%s, %s, %s)", _ROWS)
    finally:
        conn.close()


def _change(engine, table):
    p = "?" if engine == "sqlserver" else "%s"
    conn = _src_conn(engine)
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE {_q(engine, table)} SET name = {p} WHERE id = 1", ("upd 🚀 ✓",))
        cur.execute(f"UPDATE {_q(engine, table)} SET note = NULL WHERE id = 4")
        cur.execute(f"DELETE FROM {_q(engine, table)} WHERE id = 2")
        cur.execute(f"INSERT INTO {_q(engine, table)} (id, name, note) VALUES (101, {p}, {p})", ("new", ""))
        if engine == "sqlserver":
            conn.commit()
    finally:
        conn.close()


def _drop(engine, table):
    conn = _src_conn(engine)
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {_q(engine, table)}")
        if engine == "sqlserver":
            conn.commit()
    finally:
        conn.close()
    d = _dest_conn("postgresql")
    try:
        _exec(d, f'DROP TABLE IF EXISTS "{table}"')
    finally:
        d.close()


def _request(engine, table, sync):
    from src.transfer.models import EndpointConfig, TransferRequest

    if engine == "sqlserver":
        src = EndpointConfig(kind="database", format="sqlserver", schema="dbo", table=table,
                             extra={"trust_server_certificate": True}, **_MS)
    else:
        src = EndpointConfig(kind="database", format="mysql", table=table, **_MY)
    return TransferRequest(
        source=src,
        destination=EndpointConfig(kind="database", format="postgresql", table=table, **_DESTS["postgresql"]),
        sync_mode=sync,
        stream_contracts=[{"name": table, "selected": True, "primary_key": ["id"], "sync_mode": sync,
                           "cursor_semantics": "cdc_position"}],
        skip_preflight=True, validation_mode="strict")


def _run(fake, engine, table, sync):
    from src.transfer.engine import UniversalTransferEngine

    jid = "e3006" + uuid.uuid4().hex[:12]
    fake.update_job_status(jid, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(engine, table, sync), jid)


def _dest(table):
    d = _dest_conn("postgresql")
    try:
        return {r[0]: (r[1], r[2]) for r in _fetch(d, f'SELECT id, name, note FROM "{table}"')}
    finally:
        d.close()


@pytest.mark.parametrize(("engine", "cdc_mode"), [("sqlserver", "cdc_incremental"), ("mysql", "cdc")])
def test_cdc_snapshot_into_loaded_table_applies_source_deletes(isolated, engine, cdc_mode):
    cfg = _MS if engine == "sqlserver" else _MY
    if not (_up(cfg) and _up(_DESTS["postgresql"])):
        pytest.skip(f"live {engine} / postgresql not reachable")
    table = "e3006_" + uuid.uuid4().hex[:8]
    _create(engine, table)
    try:
        first = _run(isolated, engine, table, "full_refresh_overwrite")
        assert first.success, first.error
        assert set(_dest(table)) == {1, 2, 3, 4, 5}
        _change(engine, table)

        second = _run(isolated, engine, table, cdc_mode)
        assert second.success, second.error
        dest = _dest(table)
        assert 2 not in dest, "a key deleted at the source must not survive the CDC snapshot"
        assert set(dest) == {1, 3, 4, 5, 101}
        assert dest[1] == ("upd 🚀 ✓", "note1")
        assert dest[3] == ("naïve 中文 ✓", None) and dest[4] == ("", None) and dest[5] == (None, "x")
        assert dest[101] == ("new", "")
        assert (second.destination_summary or {}).get("snapshot_leftover_deleted") == 1

        quiet = _run(isolated, engine, table, cdc_mode)
        assert quiet.success, quiet.error
        assert set(_dest(table)) == {1, 3, 4, 5, 101}
    finally:
        _drop(engine, table)


def test_sweep_never_runs_after_a_partial_dump():
    from services.cdc_snapshot_sweep import plan_snapshot_sweep

    def _plan(plan, **over):
        kw = dict(dest_type="postgresql", dest_absent_before=False, source_cfg={"type": "mysql"},
                  mappings=[], pk_sources=["id"], shared_destination=False, delete_mode="")
        kw.update(over)
        return plan_snapshot_sweep(plan, **kw)

    full = {"run_snapshot": True, "reason": "watermark_missing"}
    assert _plan(full).allowed
    assert _plan({**full, "reason": "snapshot_in_progress"}) is None
    assert _plan({**full, "snapshot_dump_open": True}) is None
    assert _plan(full, dest_absent_before=True) is None
    assert _plan(full, dest_absent_before=None) is None
    assert _plan({"run_snapshot": False}) is None
    assert _plan(full, dest_type="mongodb") is None


def test_a_snapshot_row_without_a_full_key_disables_the_sweep():
    from services.cdc_engine import ChangeBatch
    from services.cdc_snapshot_sweep import SnapshotKeyCensus, SweepPlan

    census = SnapshotKeyCensus(["id"], SweepPlan("hard", False, True))
    census.add(ChangeBatch(inserts=[{"id": 1}, {"id": None}]))
    assert census.complete is False
    out = census.settle(object(), mappings=[], pk_target_cols=["id"])
    assert out["status"] == "unmeasured" and out["removed"] == 0
    census.close()
