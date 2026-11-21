"""D302 cancel watch and COPY handoff: the rules that need no database.

* ``settle_watch`` / ``as_cancel``: what a route raises around its commit.
* Once settled, no abort starts, however long the probe was blocked (the old
  watch joined for 5 s and then let a late abort close the source under a
  committed destination).
* The watch closes a connection only through the libpq gate, which a COPY
  holds whenever its thread may be inside libpq (psycopg2 runs the COPY data
  loop without its connection lock; a close there was a use-after-free).
* A cancel between shard waves launches no further wave.
* A FIFO consumer that fails before opening the pipe does not leave the
  producer parked in ``open()``.
"""

from __future__ import annotations

import contextvars
import threading
import time

import pytest

from services.error_handling import TransferCancelled


class _Conn:
    """A weak-referenceable connection double that records cancel/close."""

    def __init__(self) -> None:
        self.cancels = 0
        self.closed = 0

    def cancel(self) -> None:
        self.cancels += 1

    def close(self) -> None:
        self.closed = 1


def _cancelled(_rows=None):
    raise TransferCancelled("Transfer cancelled by user")


def _wait_for(pred, budget: float = 5.0) -> bool:
    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.01)
    return pred()


def test_settle_watch_raises_the_cancel_the_watch_acted_on():
    from services.source_read_cancel import SourceReadCancelWatch, settle_watch

    conn = _Conn()
    watch = SourceReadCancelWatch(conn, _cancelled, interval=0.01).start()
    assert _wait_for(lambda: conn.closed)
    with pytest.raises(TransferCancelled):
        settle_watch(watch)


def test_settle_watch_is_clean_without_a_cancel_and_tolerates_none():
    from services.source_read_cancel import SourceReadCancelWatch, settle_watch

    conn = _Conn()
    watch = SourceReadCancelWatch(conn, lambda _r=None: None, interval=0.01).start()
    time.sleep(0.05)
    settle_watch(watch)
    settle_watch(None)
    assert not conn.closed and conn.cancels == 0


def test_as_cancel_turns_the_closed_session_error_into_the_cancel():
    from services.source_read_cancel import SourceReadCancelWatch, as_cancel

    conn = _Conn()
    watch = SourceReadCancelWatch(conn, _cancelled, interval=0.01).start()
    assert _wait_for(lambda: conn.closed)
    driver_error = RuntimeError("connection already closed")
    err = as_cancel(watch, driver_error)
    assert isinstance(err, TransferCancelled)
    own = TransferCancelled("superseded", superseded=True)
    assert as_cancel(watch, own) is own
    # No watch, or a watch that never acted: the error is the error.
    assert as_cancel(None, driver_error) is driver_error
    idle = SourceReadCancelWatch(_Conn(), lambda _r=None: None, interval=10)
    assert as_cancel(idle, driver_error) is driver_error


def test_no_abort_starts_after_settle_even_if_the_probe_was_blocked():
    """The probe (a job-store read) hangs past the old 5 s join; the route
    settles and commits; then the probe reports the cancel. The source must
    not be closed under the committed destination."""
    from services.source_read_cancel import SourceReadCancelWatch, settle_watch

    entered, release = threading.Event(), threading.Event()

    def blocked_probe(_rows=None):
        entered.set()
        release.wait(30)
        raise TransferCancelled("Transfer cancelled by user")

    conn = _Conn()
    watch = SourceReadCancelWatch(conn, blocked_probe, interval=0.01).start()
    assert entered.wait(5)
    started = time.monotonic()
    settle_watch(watch)  # must neither raise nor wait for the probe
    assert time.monotonic() - started < 1.0
    release.set()
    time.sleep(0.3)
    assert not conn.closed and conn.cancels == 0
    assert watch.aborted is None


def test_abort_waits_for_the_copy_thread_to_leave_libpq():
    from services.cancel_check import libpq_gate
    from services.source_read_cancel import abort_connection

    conn = _Conn()
    gate = libpq_gate(conn)
    gate.acquire()  # a COPY thread inside PQgetCopyData
    done = threading.Event()
    t = threading.Thread(
        target=lambda: (abort_connection(conn), done.set()), daemon=True
    )
    t.start()
    try:
        assert _wait_for(lambda: conn.cancels >= 1)
        time.sleep(1.4)
        assert not conn.closed, "closed while the COPY thread was inside libpq"
        assert conn.cancels >= 2, "the server-side cancel is resent while waiting"
    finally:
        gate.release()  # the COPY returns (the cancel aborted it)
    assert done.wait(5)
    assert conn.closed
    assert not gate.locked()


def test_abort_gives_up_on_the_close_rather_than_racing_libpq():
    from services.cancel_check import libpq_gate
    from services.source_read_cancel import abort_connection

    conn = _Conn()
    gate = libpq_gate(conn)
    gate.acquire()
    try:
        assert abort_connection(conn, gate_wait=0.5) is False
        assert not conn.closed and conn.cancels >= 1
    finally:
        gate.release()


def test_copy_holds_the_gate_in_libpq_and_releases_it_in_the_callback():
    from services.cancel_check import copy_expert_keeping_cancel, libpq_gate

    conn = _Conn()
    seen: list[tuple[str, bool]] = []

    class _Cur:
        connection = conn

        def copy_expert(self, sql, file):
            gate = libpq_gate(conn)
            for chunk in (b"1\n", b"2\n"):
                seen.append(("libpq", gate.locked()))
                file.write(chunk)
            seen.append(("libpq", gate.locked()))

    class _Sink:
        def write(self, data):
            seen.append(("callback", libpq_gate(conn).locked()))

    copy_expert_keeping_cancel(_Cur(), "COPY x TO STDOUT", _Sink())
    assert seen == [
        ("libpq", True), ("callback", False),
        ("libpq", True), ("callback", False),
        ("libpq", True),
    ]
    assert not libpq_gate(conn).locked()


def test_copy_releases_the_gate_when_the_callback_raises_the_cancel():
    from services import cancel_check

    conn = _Conn()

    class _Cur:
        connection = conn

        def copy_expert(self, sql, file):
            file.write(b"1\n")

    def run():
        cancel_check.set_cancel_probe(_cancelled)
        cancel_check.copy_expert_keeping_cancel(_Cur(), "COPY", _SinkOk(), poll=True)

    class _SinkOk:
        def write(self, data):
            return len(data)

    with pytest.raises(TransferCancelled):
        contextvars.copy_context().run(run)
    assert not cancel_check.libpq_gate(conn).locked()


def test_a_cancel_between_shard_waves_launches_no_further_wave(monkeypatch):
    from services import cancel_check, copy_pg_mysql

    launched: list[str] = []
    state = {"cancel": False}

    def fake_shard(**kw):
        launched.append(kw["copy_sql"])
        state["cancel"] = True  # the operator cancels while wave 1 runs

    def probe(_rows=None):
        if state["cancel"]:
            raise TransferCancelled("Transfer cancelled by user")

    monkeypatch.setattr(copy_pg_mysql, "_shard_thread", fake_shard)

    def run():
        cancel_check.set_cancel_probe(probe)
        copy_pg_mysql._launch_copy_shards(
            copy_sqls=[f"COPY {i}" for i in range(4)],
            src_cur=None, dst_cur=None, dest_conn=None,
            source_cfg={}, dest_cfg={}, snapshot_id="s", table_q="`t`",
            target_cols=["id"], max_parallel=2,
        )

    with pytest.raises(TransferCancelled):
        contextvars.copy_context().run(run)
    assert launched == ["COPY 0", "COPY 1"]


@pytest.mark.timeout(60)
def test_a_consumer_that_fails_before_opening_the_fifo_frees_the_producer():
    from services.copy_fast_path import fifo_streaming_supported, stream_between_cursors

    if not fifo_streaming_supported():
        pytest.skip("no named pipes on this host")
    producer_done = threading.Event()
    producer_error: list[BaseException] = []

    def producer(path: str) -> None:
        try:
            with open(path, "wb") as fh:
                for _ in range(1000):
                    fh.write(b"x" * 65536)
        except BaseException as exc:  # noqa: BLE001 - recorded for the assert
            producer_error.append(exc)
            raise
        finally:
            producer_done.set()

    def consumer(path: str) -> None:
        raise TransferCancelled("Transfer cancelled by user")  # before the LOAD

    started = time.monotonic()
    with pytest.raises(TransferCancelled):
        stream_between_cursors(prefix="df_test_", producer=producer, consumer=consumer)
    assert time.monotonic() - started < 10, "the producer held the handoff"
    assert producer_done.is_set(), "the producer is still parked on the pipe"
    assert producer_error and isinstance(producer_error[0], OSError)


class _Store:
    """A job store double for the notes the cancel path records."""

    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {"job1": {"status": "running"}}

    def get_job(self, job_id):
        return dict(self.jobs.get(job_id) or {})

    def update_job_fields(self, job_id, fields):
        self.jobs.setdefault(job_id, {}).update(fields)


def _in_run(store: _Store, fn):
    """Run ``fn`` in a context bound to job1 (as the engine binds a run)."""
    from services import cancel_check

    def run():
        cancel_check.set_progress_meter(cancel_check.ProgressMeter(store, "job1"))
        return fn()

    return contextvars.copy_context().run(run)


def test_abort_closes_at_once_while_the_copy_is_parked_in_its_callback():
    """A COPY whose destination stalled sits in the file callback (a pipe
    write), outside libpq: the gate is free and the session is closed now,
    not when the destination answers."""
    from services.cancel_check import copy_expert_keeping_cancel
    from services.source_read_cancel import abort_connection

    conn = _Conn()
    parked, unblock = threading.Event(), threading.Event()

    class _Cur:
        connection = conn

        def copy_expert(self, sql, file):
            file.write(b"1\n")

    class _StalledSink:
        def write(self, data):
            parked.set()
            unblock.wait(30)  # the FIFO is full: the LOAD is stalled

    t = threading.Thread(
        target=lambda: copy_expert_keeping_cancel(_Cur(), "COPY", _StalledSink()),
        daemon=True,
    )
    t.start()
    try:
        assert parked.wait(5)
        started = time.monotonic()
        assert abort_connection(conn) is True
        assert conn.closed and time.monotonic() - started < 1.0
    finally:
        unblock.set()
        t.join(5)


def test_a_slow_release_keeps_waiting_and_is_recorded_on_the_job(monkeypatch):
    """N2: past the 5 s promise the job says the source session is still
    open; the watch keeps waiting (resending the cancel) and closes it as
    soon as the read leaves libpq, then records how long it took."""
    from services import source_read_cancel as src
    from services.cancel_check import libpq_gate

    monkeypatch.setattr(src, "RELEASE_BUDGET_SECONDS", 0.3)
    conn, store = _Conn(), _Store()
    gate = libpq_gate(conn)
    gate.acquire()  # the read is inside libpq; the cancel is not answered
    watch = _in_run(store, lambda: src.SourceReadCancelWatch(conn, _cancelled, interval=0.01))
    watch.start()
    try:
        assert _wait_for(lambda: "source_session_warning" in store.jobs["job1"], 5)
        warning = store.jobs["job1"]["source_session_warning"]
        assert "still open" in warning and "read lock" in warning, warning
        time.sleep(0.5)  # well past the old fixed give-up point, scaled
        assert not conn.closed and conn.cancels >= 1
    finally:
        gate.release()
    watch.join(5)
    assert conn.closed and watch.released is True
    assert "source_session_released_after_s" in store.jobs["job1"]


def test_the_watch_stops_waiting_when_the_owner_takes_over():
    from services import source_read_cancel as src
    from services.cancel_check import libpq_gate

    conn = _Conn()
    gate = libpq_gate(conn)
    gate.acquire()
    watch = src.SourceReadCancelWatch(conn, _cancelled, interval=0.01).start()
    try:
        assert _wait_for(lambda: conn.cancels >= 1)
        watch.stop()  # the run is ending; its owner closes through the gate
        watch.join(2)
        assert not watch._thread.is_alive()
        assert watch.released is False and not conn.closed
    finally:
        gate.release()


def test_a_failing_cancel_is_logged_at_warning(caplog):
    from services.source_read_cancel import abort_connection

    class _NoCancel(_Conn):
        def cancel(self):
            raise RuntimeError("cancel request dropped by the pooler")

    with caplog.at_level("WARNING", logger="services.source_read_cancel"):
        abort_connection(_NoCancel())
    assert any("cancel" in r.getMessage() and r.levelname == "WARNING" for r in caplog.records)


def test_a_cancelled_session_is_not_handed_back_to_an_endpoint_pool():
    """A scoped (pooled) connection's close() rolls back and lends it out
    again; a cancelled source session must really end."""
    from services.source_read_cancel import abort_connection

    released: list[bool] = []

    class _Pooled(_Conn):
        def __init__(self):
            super().__init__()
            self._df_release = lambda: released.append(True)

        def close(self):
            release, self._df_release = self._df_release, None
            if release is not None:
                release()  # back to the pool, session still open
                return
            self.closed = 1

    conn = _Pooled()
    assert abort_connection(conn) is True
    assert conn.closed and released == []


def test_owner_close_waits_for_the_copy_to_leave_libpq_and_is_bounded():
    from services.cancel_check import libpq_gate
    from services.source_read_cancel import close_connection

    idle = _Conn()
    assert close_connection(idle) is True and idle.closed and idle.cancels == 0

    busy = _Conn()
    gate = libpq_gate(busy)
    gate.acquire()  # the pump is still inside PQgetCopyData
    try:
        started = time.monotonic()
        assert close_connection(busy, budget=0.5) is False
        assert time.monotonic() - started < 2.0
        assert not busy.closed and busy.cancels >= 1
    finally:
        gate.release()


@pytest.mark.timeout(60)
def test_a_load_error_stops_a_producer_waiting_on_a_silent_source():
    """N3: the LOAD failed (1062/1205) after reading part of the pipe while
    the producer was waiting inside the driver for more source rows. The
    handoff cancels the source COPY and its whole wait is bounded."""
    from services.copy_fast_path import fifo_streaming_supported, stream_between_cursors

    if not fifo_streaming_supported():
        pytest.skip("no named pipes on this host")
    source_answers = threading.Event()  # set by the source-side cancel
    producer_done = threading.Event()
    aborted: list[bool] = []

    def producer(path: str) -> None:
        try:
            with open(path, "wb") as fh:
                fh.write(b"1\ta\t1\n")
                fh.flush()
                source_answers.wait(120)  # inside PQgetCopyData, silent source
                for _ in range(100):
                    fh.write(b"x" * 65536)
        finally:
            producer_done.set()

    def consumer(path: str) -> None:
        with open(path, "rb") as fh:
            fh.read(6)
        raise RuntimeError("(1062, \"Duplicate entry '1' for key 'PRIMARY'\")")

    def abort_producer() -> None:
        aborted.append(True)
        source_answers.set()

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="1062"):
        stream_between_cursors(
            prefix="df_test_", producer=producer, consumer=consumer,
            abort_producer=abort_producer,
        )
    assert aborted == [True]
    assert time.monotonic() - started < 20
    assert producer_done.wait(5)


def test_a_stage_that_cannot_be_dropped_is_named_on_the_job(caplog):
    """N4: a failed DROP of the run's stage was swallowed at debug; nothing
    else ever removes a randomly named stage."""
    from services.copy_pg_mysql import _drop_run_table

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql):
            raise RuntimeError("Lock wait timeout exceeded")

    class _Dest:
        def cursor(self):
            return _Cur()

        def commit(self):
            pass

    store = _Store()
    with caplog.at_level("WARNING", logger="services.copy_pg_mysql"):
        dropped = _in_run(store, lambda: _drop_run_table(_Dest(), "`df_ow_orders_ab12cd34`", staged=True))
    assert dropped is False
    assert store.jobs["job1"]["orphaned_tables"] == ["df_ow_orders_ab12cd34"]
    assert "df_ow_orders_ab12cd34" in store.jobs["job1"]["orphaned_tables_note"]
    assert any(r.levelname == "WARNING" and "df_ow_orders_ab12cd34" in r.getMessage() for r in caplog.records)
