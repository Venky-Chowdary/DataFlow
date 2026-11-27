"""A cancelled full-refresh overwrite puts the pre-run rows back.

Owner decision (D302 follow-up): a cancel must not leave the live table empty
while Datawrap still holds the pre-run rows. The engine's set-aside overwrite
(MySQL ``<table>__df_bak`` row copy, MongoDB, the generic-SQL row copy and the
warehouse set-aside) empties the live object before the route writes; on a
cancel it used to drop that copy. D134 excluded cancels from the restore for
resumable writes (their committed prefix is behind the checkpoint); an
overwrite restarts from the first row, so the restore is safe.

Live, mid-load through a paced source view so the read is certainly still
running when the cancel lands: PG->MySQL and PG->PG, default route and the
row path. A cancel that lands after the load committed keeps the existing
"cancel landed after N rows committed" outcome.
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest

from tests.test_d302_cancel_releases_pg_source_txn_e2e import (
    _endpoint,
    _engine,
    _paced_source,
    _setup,
    _source_lock_holders,
    _teardown,
)

PRE_RUN = [(-2, "keep-2", 0), (-1, "keep-1", 0)]
RESTORED_MESSAGE = "Cancelled. The destination holds the rows it had before this run (2 rows)."


def _pg_dest(p: dict):
    import psycopg2

    conn = psycopg2.connect(
        host=p["host"], port=int(p["port"]), user=p["username"],
        password=p["password"], dbname=p["database"],
    )
    conn.autocommit = True
    return conn


def _make_dest(kind: str, my, pgd, dst_t: str) -> None:
    ddl = "(id INT PRIMARY KEY, a VARCHAR(40), n INT)"
    if kind == "mysql":
        with my.cursor() as cur:
            cur.execute(f"CREATE TABLE `{dst_t}` {ddl}")
            cur.executemany(f"INSERT INTO `{dst_t}` VALUES (%s, %s, %s)", PRE_RUN)  # nosec B608
    else:
        with pgd.cursor() as cur:
            cur.execute(f"CREATE TABLE {dst_t} {ddl}")
            cur.executemany(f"INSERT INTO {dst_t} VALUES (%s, %s, %s)", PRE_RUN)  # nosec B608


def _dest_rows(kind: str, my, pgd, dst_t: str) -> list[tuple]:
    if kind == "mysql":
        with my.cursor() as cur:
            cur.execute(f"SELECT id, a, n FROM `{dst_t}` ORDER BY id")  # nosec B608
            return [tuple(r) for r in cur.fetchall()]
    with pgd.cursor() as cur:
        cur.execute(f"SELECT id, a, n FROM {dst_t} ORDER BY id")  # nosec B608
        return [tuple(r) for r in cur.fetchall()]


def _leftovers(kind: str, my, pgd, dst_t: str) -> list[str]:
    """Any table besides the destination that carries its name (backups, stages)."""
    if kind == "mysql":
        with my.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name LIKE %s AND table_name <> %s",
                (f"%{dst_t}%", dst_t),
            )
            return [r[0] for r in cur.fetchall()]
    with pgd.cursor() as cur:
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE tablename LIKE %s AND tablename <> %s",
            (f"%{dst_t}%", dst_t),
        )
        return [r[0] for r in cur.fetchall()]


def _overwrite_started(kind: str, my, pgd, dst_t: str) -> bool:
    if kind == "mysql":
        return any("__df_bak" in name for name in _leftovers(kind, my, pgd, dst_t))
    with pgd.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_locks l JOIN pg_class c ON c.oid = l.relation "
            "WHERE c.relname = %s AND l.mode = 'AccessExclusiveLock' "
            "AND l.pid <> pg_backend_pid() LIMIT 1",
            (dst_t,),
        )
        return cur.fetchone() is not None


def _drop_dest(kind: str, my, pgd, dst_t: str) -> None:
    for name in [dst_t, *_leftovers(kind, my, pgd, dst_t)]:
        if kind == "mysql":
            with my.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{name}`")
        else:
            with pgd.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{name}"')


def _run_and_cancel_mid_load(monkeypatch, dest: str, route: str):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    store = _engine(monkeypatch)
    p, c, pg, my, src_t, dst_t = _setup(150_000)
    view = _paced_source(pg, src_t)
    pgd = _pg_dest(p)
    _make_dest(dest, my, pgd, dst_t)
    request = TransferRequest(
        source=_endpoint("postgresql", p, view),
        destination=_endpoint(dest, p if dest == "postgresql" else c, dst_t),
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
        source_filter={"column": "id", "op": "gt", "value": 0} if route == "row" else None,
        skip_preflight=True,
    )
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "pending", transfer_request={})
    result: dict = {}
    worker = threading.Thread(
        target=lambda: result.setdefault(
            "r", UniversalTransferEngine().execute_tracked(request, job_id)
        )
    )
    worker.start()
    deadline = time.time() + 120
    # Mid-load means: the source read is running AND the destination has
    # been taken over by the overwrite (MySQL: the pre-run rows were set
    # aside; PostgreSQL: the load transaction holds the table). A cancel
    # before that point tests nothing about the restore.
    while time.time() < deadline and worker.is_alive() and not (
        _source_lock_holders(pg, src_t) and _overwrite_started(dest, my, pgd, dst_t)
    ):
        time.sleep(0.05)
    time.sleep(1.0)  # rows in flight: the paced read lasts ~15 s
    alive = worker.is_alive() and _overwrite_started(dest, my, pgd, dst_t)
    store.request_job_cancel(job_id)
    store.update_job_status(job_id, "cancelled", operator_command=True)
    worker.join(180)
    return store, job_id, result, alive, worker, (p, c, pg, my, src_t, dst_t, pgd)


@pytest.mark.parametrize("route", ["default", "row"])
@pytest.mark.parametrize("dest", ["mysql", "postgresql"])
def test_a_cancelled_overwrite_puts_the_pre_run_rows_back(monkeypatch, dest, route):
    store, job_id, result, alive, worker, env = _run_and_cancel_mid_load(monkeypatch, dest, route)
    p, c, pg, my, src_t, dst_t, pgd = env
    try:
        assert alive, f"the cancel did not land mid-load: {result}"
        assert not worker.is_alive(), "the cancelled job is still running"
        r = result.get("r")
        assert r is not None and r.success is False, r
        job = store.get_job(job_id) or {}
        assert job.get("phase") == "cancelled", (job.get("phase"), job.get("message"))
        # The live table holds exactly its pre-run rows; no backup or stage is left.
        assert _dest_rows(dest, my, pgd, dst_t) == PRE_RUN
        assert _leftovers(dest, my, pgd, dst_t) == []
        outcome = job.get("cancel_outcome") or {}
        assert outcome.get("destination_rows_now") == 2, outcome
        if dest == "mysql":
            # Set-aside overwrite: emptied first, put back on the cancel.
            assert job.get("message") == RESTORED_MESSAGE, job.get("message")
            assert outcome.get("pre_run_rows_restored") is True, outcome
            assert outcome.get("rows_committed") == 0, outcome
            assert outcome.get("destination_rows_before") == 2, outcome
            assert outcome.get("rolled_back") is True, outcome
        elif route == "default":
            # PostgreSQL overwrites inside one transaction: the cancel rolled
            # it back and the route said so; there is no backup to restore.
            assert outcome.get("rows_committed") == 0, outcome
            assert "holds 2 row(s), as it did before the run" in str(job.get("message")), job
        else:
            # The PG row path rolls back too but does not report it: the
            # record states the count and does not attribute it (honest).
            assert "holds 2 row(s)" in str(job.get("message")), job
    finally:
        worker.join(30)
        _drop_dest(dest, my, pgd, dst_t)
        pgd.close()
        _teardown(pg, my, src_t, dst_t)


def test_a_cancel_after_the_overwrite_committed_keeps_the_new_rows(monkeypatch):
    """The cancel lands after the load committed and was verified: the new
    rows stay, the backup is discarded, and the job keeps the existing
    "cancel landed after N rows committed" outcome."""
    import services.append_continuity as continuity
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    store = _engine(monkeypatch)
    p, c, pg, my, src_t, dst_t = _setup(3000)
    pgd = _pg_dest(p)
    _make_dest("mysql", my, pgd, dst_t)
    job_id = uuid.uuid4().hex[:24]
    real = continuity.record_before_success_status

    def cancel_then_record(*a, **kw):
        store.request_job_cancel(job_id)
        store.update_job_status(job_id, "cancelled", operator_command=True)
        return real(*a, **kw)

    monkeypatch.setattr(continuity, "record_before_success_status", cancel_then_record)
    request = TransferRequest(
        source=_endpoint("postgresql", p, src_t),
        destination=_endpoint("mysql", c, dst_t),
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
        skip_preflight=True,
    )
    store.update_job_status(job_id, "pending", transfer_request={})
    try:
        UniversalTransferEngine().execute_tracked(request, job_id)
        job = store.get_job(job_id) or {}
        assert job.get("status") == "cancelled", job.get("status")
        assert "the load had finished when the cancel landed" in str(job.get("message")), job
        assert (job.get("cancel_outcome") or {}).get("rows_committed") == 3000
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*), MIN(id) FROM `{dst_t}`")  # nosec B608
            assert cur.fetchone() == (3000, 1)
        assert _leftovers("mysql", my, pgd, dst_t) == []
    finally:
        _drop_dest("mysql", my, pgd, dst_t)
        pgd.close()
        _teardown(pg, my, src_t, dst_t)
