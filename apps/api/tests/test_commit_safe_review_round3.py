"""PR 407 round-3 review — regressions of the commit-safe incremental window.

Each test reproduces a round-3 finding. Live cases need a reachable
PostgreSQL; they skip, never pass, when it is not there.
"""

from __future__ import annotations

import os
import random
import threading
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.test_commit_safe_review_round2 import _connect, isolated  # noqa: E402,F401

SCHEMA = "df_csw_round3"


def _sql(statement: str, fetch: bool = False):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
            return cur.fetchall() if fetch else None
    finally:
        conn.close()


def _endpoint(table: str) -> EndpointConfig:
    from tests.helpers.live_env import pg_creds

    c = pg_creds()
    return EndpointConfig(
        kind="database", format="postgresql", host=c["host"], port=int(c["port"]),
        database=c["database"], schema=SCHEMA, table=table,
        username=c["username"], password=c["password"],
    )


@pytest.fixture()
def pg():
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not reachable")
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    _sql(f'CREATE SCHEMA "{SCHEMA}"')
    yield SCHEMA
    _sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')


def _run(fake, src: str, dst: str, *, cursor: str, pk: str = ""):
    contract = {"name": src, "cursor_field": cursor, "sync_mode": "incremental_append", "selected": True}
    if pk:
        contract["primary_key"] = pk
    request = TransferRequest(
        source=_endpoint(src), destination=_endpoint(dst), sync_mode="incremental_append",
        stream_contracts=[contract], skip_preflight=True, validation_mode="strict",
    )
    job_id = "r3p" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _window(result) -> dict:
    return (result.destination_summary or {}).get("commit_safe_window") or {}


def _missing_and_dups(src: str, dst: str) -> tuple[int, int]:
    missing = _sql(
        f'SELECT count(*) FROM "{SCHEMA}".{src} s LEFT JOIN "{SCHEMA}".{dst} d ON d.id = s.id '
        "WHERE d.id IS NULL",
        fetch=True,
    )[0][0]
    dups = _sql(f'SELECT count(*) - count(DISTINCT id) FROM "{SCHEMA}".{dst}', fetch=True)[0][0]
    return int(missing), int(dups)


# --------------------------------------------------------------------------
# Finding 1 — the late-commit count and its page walk share one snapshot
# --------------------------------------------------------------------------


def test_busy_table_rereads_nothing_and_a_late_commit_is_still_recovered(isolated, pg):
    """The reviewer's busy-table probe: old rows behind the watermark are
    edited in place every millisecond or so by another connection. The count
    and the page walk used to run as separate READ COMMITTED statements, so the
    candidate set moved between them, "checked != count", and every run re-read
    the table from its oldest edited row. On one snapshot nothing is re-read,
    and a real late commit is still found and recovered."""
    _sql(f'CREATE TABLE "{SCHEMA}".bz (id bigint PRIMARY KEY, v text)')
    _sql(f"INSERT INTO \"{SCHEMA}\".bz SELECT g, 'r' FROM generate_series(1, 5000) g")
    _sql(f'ANALYZE "{SCHEMA}".bz')
    first = _run(isolated, "bz", "bz_out", cursor="id")
    assert first.success, first.error
    _sql(f"UPDATE \"{SCHEMA}\".bz SET v = v || '!' WHERE id <= 1500")

    stop = threading.Event()
    edits = [0]

    def churn():
        conn = _connect()
        try:
            while not stop.is_set():
                with conn.cursor() as cur:
                    cur.execute(
                        f'UPDATE "{SCHEMA}".bz SET v = v || \'.\' WHERE id = %s',
                        (random.randint(1, 4900),),
                    )
                edits[0] += 1
                time.sleep(0.001)
        finally:
            conn.close()

    worker = threading.Thread(target=churn, daemon=True)
    worker.start()
    try:
        for i in range(3):
            lo = 5001 + i * 10
            _sql(f"INSERT INTO \"{SCHEMA}\".bz SELECT g, 'new' FROM generate_series({lo}, {lo + 9}) g")
            busy = _run(isolated, "bz", "bz_out", cursor="id")
            assert busy.success, busy.error
            window = _window(busy)
            assert window.get("late_rows_detected"), window
            assert window.get("late_rows_checked") == window.get("late_rows_detected"), window
            assert not window.get("replay_seed"), window
            assert not window.get("rows_replayed"), window
            assert busy.records_transferred == 10, (busy.records_transferred, window)
        late = _connect(autocommit=False)
        try:
            late.cursor().execute(f"INSERT INTO \"{SCHEMA}\".bz VALUES (5035, 'LATE')")
            _sql(f"INSERT INTO \"{SCHEMA}\".bz SELECT g, 'fast' FROM generate_series(5040, 5049) g")
            assert _run(isolated, "bz", "bz_out", cursor="id").success
            late.commit()
        finally:
            late.close()
        recovering = _run(isolated, "bz", "bz_out", cursor="id")
        assert recovering.success, recovering.error
        window = _window(recovering)
        assert window.get("replay_seed") == "5035", window
        assert window.get("rows_recovered") == 1, window
    finally:
        stop.set()
        worker.join()
    assert edits[0] > 0
    assert _missing_and_dups("bz", "bz_out") == (0, 0)


def test_rows_only_partly_checked_are_never_said_to_be_more_than_were_found():
    """The evidence never says more rows "could be checked" than were found."""
    from services import commit_safe_window as csw

    def _plan(late):
        return csw.plan_commit_safe_window(
            source_type="postgresql", sync_mode="incremental_append", cursor_column="id",
            cursor_type="bigint", stored_watermark="100", can_replay=True, contract={},
            metadata={csw.HORIZON_KEY: {"xmin": "10", "strategy": csw.STRATEGY_PG}},
            capture_bound=lambda: {"xmin": "20", "strategy": csw.STRATEGY_PG},
            probe_late=lambda **_kw: late, relation_kind="r",
        )

    over = _plan(csw.LateRows(count=5, min_cursor="3", undelivered=0, checked=7, rule="horizon"))
    assert over.replays and str(over.seed) == "3", over
    assert "7 of them" not in over.reason, over.reason
    assert "only" not in over.reason, over.reason
    partly = _plan(csw.LateRows(count=5, min_cursor="3", undelivered=0, checked=3, rule="horizon"))
    assert "only 3 of them" in partly.reason, partly.reason

