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
    t = threading.Thread(target=lambda: (abort_connection(conn), done.set()))
    t.start()
    assert _wait_for(lambda: conn.cancels >= 1)
    time.sleep(0.6)
    assert not conn.closed, "closed while the COPY thread was inside libpq"
    assert conn.cancels >= 2, "the server-side cancel is resent while waiting"
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
