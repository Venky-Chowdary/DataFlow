"""A pinned snapshot yields to a session waiting on its locks (review 3, item 3).

A pinned side holds its snapshot's locks for the whole hold: PostgreSQL's
AccessShareLock on the table, MySQL/MariaDB's shared metadata lock. A
TRUNCATE, a table swap or DDL then waited up to the 30-minute cap, and every
statement queued behind it. The pin now watches, on a second connection, for
sessions waiting on those locks and lets its snapshot go as soon as one waits:
the waiter proceeds within a poll interval, the scan reads on statement by
statement, and the result says why it was not pinned ("no difference found").
"""

from __future__ import annotations

import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402
from test_parity_scale_verification_live import _env, _job, _report, _sql, tables  # noqa: E402,F401


def _svc():
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service()


pg = pytest.mark.skipif(not parity_engines.available("postgresql"),
                        reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")


def _slow_pinned_statements(monkeypatch, side_cls) -> threading.Event:
    """Each statement takes a little longer (the scan lasts a few seconds); the event is set
    once a pinned side has read the table (it holds the table's lock from then on)."""
    pinned_read = threading.Event()
    name = "_execute" if "_execute" in vars(side_cls) else "execute"
    real = getattr(side_cls, name)

    def slow(self, sql, params=()):
        rows = real(self, sql, params)
        if self.pinned and "COUNT(*)" in sql.upper():
            pinned_read.set()
        time.sleep(0.15)
        return rows

    monkeypatch.setattr(side_cls, name, slow)
    return pinned_read


@pg
def test_a_truncate_waiting_on_a_pinned_scan_proceeds_within_seconds(tables, monkeypatch) -> None:
    import psycopg2

    from services.parity_scale import verification
    from services.parity_scale.dialects.postgresql import PostgresSide
    from services.parity_scale.driver import ScanConfig

    src, dst = tables
    job_id = _job(src, dst)
    pinned_read = _slow_pinned_statements(monkeypatch, PostgresSide)
    poll = {"lock_poll_s": 0.2} if "lock_poll_s" in ScanConfig.__dataclass_fields__ else {}
    result: dict[str, Any] = {}
    worker = threading.Thread(target=lambda: result.update(block=verification.start_for_job(
        job_id, background=False, config=ScanConfig(segment_rows=50, **poll))))
    worker.start()
    try:
        assert pinned_read.wait(30), "the scan never read a pinned side"
        c = parity_engines.cfg("postgresql")
        other = psycopg2.connect(host=c["host"], port=c["port"], dbname=c["database"],
                                 user=c["username"], password=c["password"])
        with other.cursor() as cur:
            cur.execute("SET lock_timeout = '20s'")
            started = time.monotonic()
            cur.execute(f'TRUNCATE "{dst}"')  # waits for the snapshot's AccessShareLock
            waited = time.monotonic() - started
        other.rollback()  # nothing truncated: the comparison below still matches
        other.close()
    finally:
        worker.join(120)
    # Before: the TRUNCATE waited for the whole scan (9.7 s here; up to the 30-minute cap).
    assert waited < 3, waited
    block = result["block"]
    assert block["state"] == "passed"
    consistency = _svc().get_job(job_id)["reconciliation"]["parity_at_scale"]["consistency"]
    assert consistency["kind"] == "not_established"
    reasons = " ".join(consistency["reasons"])
    assert "released the snapshot so session" in reasons and "AccessExclusiveLock" in reasons
    report = _report(job_id)
    assert report["verdict"] == "no_difference_found"
    assert "released the snapshot" in report["verdict_reason"]


@pg
def test_without_a_second_connection_nothing_is_pinned(tables) -> None:
    """No lock watch, no held snapshot: the pin is let go at once and the result says why."""
    from services.parity_scale import verification
    from services.parity_scale.driver import ParityScan, ScanConfig
    from services.parity_scale.store import MemoryScanStore

    job_id = _job(*tables)
    spec, _why = verification.table_spec(_svc().get_job(job_id))
    opener = verification.side_opener(spec)

    def without_monitor(role, plan):
        side = opener(role, plan)
        side._monitor_connect = None
        return side

    scan = ParityScan.start(verification.plan_for(spec), store=MemoryScanStore(), open_side=without_monitor,
                            config=ScanConfig(segment_rows=300, pin_snapshot=True, pin_max_hold_s=1800))
    scan.run()
    ev = scan.result()
    assert ev["outcome"] == "match"
    assert ev["consistency"]["kind"] == "not_established"
    assert "no second connection" in " ".join(ev["consistency"]["reasons"])


@pg
def test_a_streamed_read_let_go_mid_way_is_read_again(tables, monkeypatch) -> None:
    """The lock watch releases an idle pinned stream; that part is read again, nothing lost."""
    from services.parity_scale import verification
    from services.parity_scale.driver import ParityScan, ScanConfig
    from services.parity_scale.store import MemoryScanStore

    src, dst = tables
    _sql(f"UPDATE \"{dst}\" SET name = upper(name)")
    job_id = _job(src, dst)
    svc = _svc()
    request = svc.get_job(job_id)["transfer_request"]
    request["mappings"][1]["transform"] = "upper"  # name goes through the streamed pass
    svc.update_job_fields(job_id, {"transfer_request": request})
    spec, _why = verification.table_spec(svc.get_job(job_id))
    plan = verification.plan_for(spec)
    assert [c.target for c in plan.stream_columns] == ["name"]
    scan = ParityScan.start(plan, store=MemoryScanStore(), open_side=verification.side_opener(spec),
                            config=ScanConfig(segment_rows=300, pin_snapshot=True, pin_max_hold_s=1800,
                                              stream_batch=200, lock_poll_s=3600))
    while not any(i["t"] == "stream" for i in scan.state["queue"][:1]):
        scan.run(max_seconds=0.01)
    # A session waits on the destination's snapshot while the stream is between fetches.
    dest = scan.side("destination")
    real_tick = scan._tick
    released = []

    def tick() -> None:
        real_tick()
        if dest.pinned and not released and dest._streaming:
            released.append(True)
            dest.yield_reason = "released the snapshot so session 1 (waiting for AccessExclusiveLock on t) could proceed"
            with dest._use:  # what the watch does to a pinned stream between fetches
                dest.unpin()

    dest.tick = tick
    scan.run()
    ev = scan.result()
    assert released
    assert ev["outcome"] == "match", ev["differences"]
    assert ev["coverage"]["stream"]["rows_source"] == ev["coverage"]["stream"]["rows_destination"] == 3000
    assert ev["consistency"]["kind"] == "not_established"
    assert "released the snapshot" in " ".join(ev["consistency"]["reasons"])


def _mysql_usable() -> bool:
    if not parity_engines.available("mysql"):
        return False
    try:
        parity_engines.connect("mysql").close()
        return True
    except Exception:  # noqa: BLE001 — a reachable port with other credentials is not usable here
        return False


@pytest.mark.skipif(not _mysql_usable(), reason="MySQL not usable (DATAFLOW_TEST_MYSQL_*)")
def test_mysql_ddl_waiting_on_a_pinned_snapshot_proceeds_within_seconds() -> None:
    from services.parity_scale.dialects.mysql import MySQLSide
    from services.parity_scale.plan import build_plan

    table = f"psd1_mdl_{uuid.uuid4().hex[:8]}"
    conn = parity_engines.connect("mysql")
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, v INT) ENGINE=InnoDB")
        cur.execute(f"INSERT INTO `{table}` VALUES (1, 1), (2, 2)")
    conn.commit()
    try:
        plan = build_plan([{"source": "id", "target": "id"}, {"source": "v", "target": "v"}],
                          source_types={"id": "bigint", "v": "int"}, dest_types={"id": "bigint", "v": "int"},
                          source_engine="mysql", dest_engine="mysql", key_columns=["id"])
        side = MySQLSide(role="source", plan=plan, table=table, connection=parity_engines.connect("mysql"),
                         connect=lambda: parity_engines.connect("mysql"))
        assert side.pin()["kind"] == "pinned"
        assert side.watch_locks(0.2) == ""
        assert side.total_count() == 2  # the snapshot now holds the table's metadata lock
        with conn.cursor() as cur:
            cur.execute("SET SESSION lock_wait_timeout = 20")
            started = time.monotonic()
            cur.execute(f"ALTER TABLE `{table}` COMMENT = 'swapped'")  # waits for an exclusive MDL
            waited = time.monotonic() - started
        assert waited < 5, waited
        assert not side.pinned
        assert "released the snapshot so session" in side.yield_reason
        assert "metadata lock" in side.yield_reason
        assert side.total_count() == 2  # and the side reads on, statement by statement
        side.close()
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.commit()
        conn.close()
