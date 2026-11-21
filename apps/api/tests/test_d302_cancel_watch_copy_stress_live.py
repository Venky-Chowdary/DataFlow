"""D302: the cancel watch must never free a connection a COPY thread is using.

psycopg2 2.9 runs the COPY OUT data loop (``_pq_copy_out_v3`` ->
``PQgetCopyData``) without holding its connection lock, so the watch's
``conn.close()`` (``PQfinish``) from its own thread could free the PGconn
while the COPY thread was blocked inside libpq waiting for the next row: a
use-after-free in the API process. The window is widest when the source
emits nothing for a while (a filtered scan, a slow shard) and the watch's
tick beats the producer's per-row cancel poll.

Live PostgreSQL only: many COPYs that go silent between rows, each cancelled
at a random moment by a fast-ticking watch. A freed PGconn rarely crashes on
the spot, so the connection records, at every ``close()``, where the COPY
thread is: still inside ``cursor.copy_expert`` (C, libpq) and not in the
Python file callback is the use-after-free window, and must never happen.
Every COPY must also end cleanly (the COPY thread raises, nothing hangs) and
leave no backend behind.
"""

from __future__ import annotations

import contextvars
import inspect
import random
import sys
import threading
import time

import pytest

from services.error_handling import TransferCancelled

ITERATIONS = 60


def _copy_call_line() -> int:
    """The line in copy_expert_keeping_cancel that enters psycopg2's COPY."""
    from services.cancel_check import copy_expert_keeping_cancel

    lines, first = inspect.getsourcelines(copy_expert_keeping_cancel)
    hits = [first + i for i, text in enumerate(lines) if "cur.copy_expert(" in text]
    assert len(hits) == 1, hits
    return hits[0]


def _pg():
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not configured")
    import psycopg2
    import psycopg2.extensions

    p = pg_creds()
    call_line = _copy_call_line()

    class _Recording(psycopg2.extensions.connection):
        """Records a close() made while the COPY thread is inside libpq."""

        copy_thread: int | None = None
        unsafe_closes = 0

        def close(self):
            tid = self.copy_thread
            frame = sys._current_frames().get(tid) if tid is not None else None
            if (
                frame is not None
                and frame.f_code.co_name == "copy_expert_keeping_cancel"
                and frame.f_lineno == call_line
            ):
                type(self).unsafe_closes += 1
            return super().close()

    def connect(**kw):
        return psycopg2.connect(
            host=p["host"], port=int(p["port"]), user=p["username"],
            password=p["password"], dbname=p["database"],
            connection_factory=_Recording, **kw,
        )

    return connect, _Recording


def _one_cancelled_copy(connect, side, rng: random.Random) -> tuple[str, float]:
    from services import cancel_check
    from services.source_read_cancel import SourceReadCancelWatch

    conn = connect(application_name="d302-stress")
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("SELECT pg_backend_pid()")
        pid = cur.fetchone()[0]
    flag = threading.Event()

    def probe(_rows=None):
        if flag.is_set():
            raise TransferCancelled("Transfer cancelled by user")

    outcome: dict = {}

    def copy_thread():
        cancel_check.set_cancel_probe(probe)
        conn.copy_thread = threading.get_ident()

        class _Sink:
            def write(self, data):
                return len(data)

        try:
            with conn.cursor() as cur:
                # Bursts of rows separated by silences: most of the time the
                # COPY thread is parked inside PQgetCopyData.
                cancel_check.copy_expert_keeping_cancel(
                    cur,
                    "COPY (SELECT g, CASE WHEN g % 20 = 0 THEN pg_sleep(0.03) END "
                    "FROM generate_series(1, 4000) g) TO STDOUT",
                    _Sink(),
                    poll=True,
                )
            outcome["end"] = "finished"
        except TransferCancelled:
            outcome["end"] = "cancelled"
        except Exception as exc:  # noqa: BLE001 - the session was ended under it
            outcome["end"] = type(exc).__name__

    watch = SourceReadCancelWatch(conn, probe, interval=0.005).start()
    t = threading.Thread(target=contextvars.copy_context().run, args=(copy_thread,))
    t.start()
    time.sleep(rng.uniform(0.0, 0.25))
    flag.set()
    started = time.monotonic()
    t.join(10)
    assert not t.is_alive(), "the COPY thread hung after the cancel"
    watch.stop()
    if not conn.closed:  # usually the watch closed it already
        conn.close()
    # The server ended the session (and its snapshot) promptly.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with side.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_stat_activity WHERE pid = %s", (pid,))
            if cur.fetchone() is None:
                break
        time.sleep(0.02)
    else:
        pytest.fail(f"backend {pid} outlived the cancel")
    return str(outcome.get("end")), time.monotonic() - started


def test_cancel_watch_never_closes_the_connection_under_a_running_copy():
    connect, recording = _pg()
    side = connect()
    side.autocommit = True
    rng = random.Random(302)
    ends: dict[str, int] = {}
    try:
        for _ in range(ITERATIONS):
            end, _secs = _one_cancelled_copy(connect, side, rng)
            ends[end] = ends.get(end, 0) + 1
    finally:
        side.close()
    assert recording.unsafe_closes == 0, (
        f"{recording.unsafe_closes} close() call(s) freed the connection while "
        f"the COPY thread was inside libpq ({ends})"
    )
    # A cancel is the only way any of these COPYs may end (4000 rows with
    # 200 silences cannot finish inside the cancel window).
    assert "finished" not in ends, ends
    assert "None" not in ends, ends
