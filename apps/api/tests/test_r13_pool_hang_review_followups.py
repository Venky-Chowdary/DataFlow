"""Review follow-ups on the r13 pool-hang family (D19, D280, D281).

1. A phase pulse must not hide a dead worker: it stamps ``heartbeat_at`` and
   leaves ``updated_at`` to real progress, so ``job_stall_seconds`` (the
   operator's silence signal) keeps counting; past its bound it stops saying
   "has not stalled".
3. An abandoned probe must not shut a session the live call is using: a
   session handed back to the endpoint scope leaves the probe's registry, and
   registering a connection moves its ownership.
4. A pooled connection stays registered across checkin, so the pre-ping that
   SQLAlchemy runs *before* the checkout event is interruptible too; close and
   invalidate unregister it.
5. A descriptor number reused by another socket after the worker closed its
   connection is never shut: the socket's identity is checked first.
6. A superseded attempt's pulse stops instead of overwriting the resumed run.
"""

from __future__ import annotations

import contextvars
import os
import socket
import threading
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.cancel_check as cc  # noqa: E402
from services.mongodb_service import MemoryMongoDBService  # noqa: E402
from src.ai.copilot import tool_deadline as td  # noqa: E402


def _wait_until(predicate, seconds: float) -> bool:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


# --- 1: the pulse leaves updated_at alone and stops asserting liveness ------


def _heartbeat_kwargs(**extra):
    """Only the keywords this build's job_phase_heartbeat accepts (the parent
    had no bound); the behaviour is asserted either way."""
    import inspect

    from src.transfer.reconcile_heartbeat import job_phase_heartbeat

    accepted = inspect.signature(job_phase_heartbeat).parameters
    return {k: v for k, v in extra.items() if k in accepted}


def test_phase_pulse_stamps_heartbeat_at_and_leaves_updated_at_to_progress():
    from src.transfer.reconcile_heartbeat import job_phase_heartbeat

    store = MemoryMongoDBService()
    store.update_job_status("j1", "running", phase="reading", message="start")
    before = store.get_job("j1")["updated_at"]
    time.sleep(0.05)
    with job_phase_heartbeat(
        store, "j1", phase="reading", message="Reading from the source (pg 10.0.0.5:5432)",
        interval_s=0.05,
    ):
        time.sleep(0.3)
    job = store.get_job("j1")
    # The worker proved it was alive through heartbeat_at...
    assert job.get("heartbeat_at"), job
    assert "Reading from the source" in job["message"]
    # ...and never through updated_at, the operator's stall signal. A pulse
    # that refreshed it hid a worker blocked for good behind a silent source.
    assert job["updated_at"] == before, (job["updated_at"], before)


def test_phase_pulse_stops_asserting_liveness_past_its_bound():
    from src.transfer.reconcile_heartbeat import job_phase_heartbeat

    store = MemoryMongoDBService()
    store.update_job_status("j1b", "running", phase="reading", message="start")
    with job_phase_heartbeat(
        store, "j1b", phase="reading", message="Reading from the source (pg 10.0.0.5:5432)",
        interval_s=0.05, **_heartbeat_kwargs(bound_seconds=0.3),
    ):
        time.sleep(0.6)
    message = store.get_job("j1b")["message"]
    # Past the bound the message says the system has not answered instead of
    # promising the job has not stalled.
    assert "has not stalled" not in message, message
    assert "has not answered" in message and "10.0.0.5:5432" in message, message


def test_blocking_step_pulse_never_moves_updated_at():
    store = MemoryMongoDBService()
    store.update_job_status("j2", "running", total_rows=10, records_processed=0)
    before = store.get_job("j2")["updated_at"]
    cc.set_progress_meter(cc.ProgressMeter(store, "j2", interval=0))
    try:
        with cc.blocking_step("Reading from the source (x)"):
            after_entry = store.get_job("j2")["updated_at"]
            time.sleep(0.1)
    finally:
        cc.set_progress_meter(None)
    job = store.get_job("j2")
    # Entering the step is real progress (a status write); nothing after it
    # moved updated_at.
    assert after_entry >= before
    assert job["updated_at"] == after_entry, (job["updated_at"], after_entry)


# --- 6: a superseded attempt's pulse stops ----------------------------------


class _RecordingStore(MemoryMongoDBService):
    """Every message written to the job, by either write path."""

    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        if "message" in kwargs:
            self.messages.append(str(kwargs["message"]))
        return super().update_job_status(job_id, status, **kwargs)

    def update_job_fields(self, job_id: str, fields: dict, **kw) -> bool:
        if "message" in fields:
            self.messages.append(str(fields["message"]))
        return super().update_job_fields(job_id, fields, **kw)


def test_blocking_step_pulse_stops_once_the_attempt_is_superseded(monkeypatch):
    monkeypatch.setattr(cc, "BLOCKING_STEP_PULSE_SECONDS", 0.05, raising=False)
    store = _RecordingStore()
    job_id = uuid.uuid4().hex[:24]
    store.update_job_status(job_id, "running", total_rows=10, records_processed=0)
    assert cc.begin_run_attempt(store, job_id)
    cc.set_progress_meter(cc.ProgressMeter(store, job_id, interval=0))
    pulses = lambda: [m for m in store.messages if m.startswith("Reading from the source (y)") and "elapsed" in m]  # noqa: E731
    try:
        with cc.blocking_step("Reading from the source (y)"):
            assert _wait_until(lambda: len(pulses()) >= 1, 2), store.messages
            # resume_job stamps a fresh attempt while this one is still waiting.
            store.update_job_fields(job_id, {"run_attempt": uuid.uuid4().hex})
            store.update_job_status(job_id, "running", message="new attempt: Streaming 10 rows…")
            time.sleep(0.3)
            seen = len(pulses())
            time.sleep(0.3)
    finally:
        cc.set_progress_meter(None)
        cc.end_run_attempt()
    # No further pulse after the supersession, and the new attempt's message stands.
    assert len(pulses()) == seen, store.messages
    assert store.get_job(job_id)["message"].startswith("new attempt"), store.get_job(job_id)["message"]


# --- 3: ownership moves; a released session leaves the probe's registry -----


class _Conn:
    closed = False

    def __init__(self) -> None:
        self.cancelled = 0

    def cancel(self) -> None:
        self.cancelled += 1


def test_a_session_handed_back_by_a_probe_is_not_shut_when_the_probe_is_abandoned():
    from services.endpoint_session import borrow, endpoint_session_scope

    conn = _Conn()
    progress = td._Progress(limit=30)
    token = td._progress.set(progress)
    try:
        with endpoint_session_scope():
            probe_thread: dict[str, int] = {}

            def probe() -> None:
                probe_thread["id"] = threading.get_ident()
                c, release = borrow("pg", ("h", 1), open_fn=lambda: conn, close=lambda c: None)
                td.register_connection(c)
                release()  # hands the session back to the scope

            ctx_thread = threading.Thread(target=contextvars.copy_context().run, args=(probe,))
            ctx_thread.start()
            ctx_thread.join()
            # The main thread borrows the same session and uses it.
            again, _rel = borrow("pg", ("h", 1), open_fn=lambda: _Conn(), close=lambda c: None)
            assert again is conn
            td.register_connection(again)
            # The probe overran: only its own connections are interrupted.
            td.interrupt_connections(progress, probe_thread["id"])
            assert conn.cancelled == 0
            # The live call's interrupt still reaches it (it owns it now).
            td.interrupt_connections(progress)
            assert conn.cancelled == 1
    finally:
        td._progress.reset(token)


def test_registering_a_connection_moves_it_out_of_the_previous_owner():
    conn = _Conn()
    old_call = td._Progress(limit=30)
    new_call = td._Progress(limit=30)
    token = td._progress.set(old_call)
    try:
        td.register_connection(conn)
    finally:
        td._progress.reset(token)
    token = td._progress.set(new_call)
    try:
        td.register_connection(conn)  # a pooled session checked out by another call
    finally:
        td._progress.reset(token)
    td.interrupt_connections(old_call)
    assert conn.cancelled == 0, "the old call shut a session another call owns"
    td.interrupt_connections(new_call)
    assert conn.cancelled == 1


# --- 4: pooled connections stay registered across checkin -------------------


def test_pooled_connection_stays_registered_across_checkin_until_closed(tmp_path):
    sa = pytest.importorskip("sqlalchemy")
    from services import statement_budget

    statement_budget.install()
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'p.db'}", pool_pre_ping=True, pool_size=1)
    progress = td._Progress(limit=30)
    token = td._progress.set(progress)
    try:
        with engine.connect() as c:
            c.execute(sa.text("SELECT 1"))
            raw = c.connection.dbapi_connection
        # Checked in: still owned by the call, so the next checkout's pre-ping
        # on a socket that went silent could be interrupted. (SQLAlchemy runs
        # the ping before the checkout event: a connection unregistered at
        # checkin is uninterruptible exactly then.)
        owners = getattr(td, "_owners", {})
        assert owners.get(id(raw)) is progress, "pooled connection not registered after checkin"
        engine.dispose()  # closes the pooled connection -> unregistered
        assert owners.get(id(raw)) is None, "closed pooled connection still registered"
    finally:
        td._progress.reset(token)
        engine.dispose()


# --- 5: a reused descriptor number is never shut ----------------------------


def _shutdown(fd: int, identity) -> bool:
    """Call this build's descriptor shutdown; the parent took no identity."""
    import inspect

    if len(inspect.signature(td._shutdown_descriptor).parameters) >= 2:
        return td._shutdown_descriptor(fd, identity)
    td._shutdown_descriptor(fd)
    return True


def test_a_reused_descriptor_is_not_shut_down():
    a, b = socket.socketpair()
    try:
        identity_of = getattr(td, "_socket_identity", None)
        identity = identity_of(a) if identity_of else None
        fd = a.fileno()
        a.close()  # the worker closed its connection...
        c, d = socket.socketpair()  # ...and the number came back for another socket
        try:
            target = c if c.fileno() == fd else d
            victim = d if target is c else c
            # The registered identity belongs to the closed socket, so the
            # (possibly reused) number is left alone and the other socket
            # keeps working.
            assert _shutdown(target.fileno(), identity) is False
            victim.sendall(b"still open")
            target.settimeout(2)
            assert target.recv(16) == b"still open"
            # With the right identity the shutdown happens.
            assert _shutdown(target.fileno(), identity_of(target) if identity_of else None) is True
            with pytest.raises(OSError):
                target.sendall(b"x")
        finally:
            c.close()
            d.close()
    finally:
        b.close()
