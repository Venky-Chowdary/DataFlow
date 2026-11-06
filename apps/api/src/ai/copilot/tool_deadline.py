"""A deadline for pilot/MCP tools that reach out to a customer's database (D169, D181).

``start_transfer`` / ``plan_transfer`` introspect the source and destination and
run a preflight on the request path. Against a source that accepts the TCP
connection and never answers, each driver step waits out its own socket timeout
and the steps stack, so the MCP client gave up first with a bare
``-32001 Request timed out`` that names nothing and staged nothing.

``run_with_deadline`` bounds the whole call. When the deadline passes it returns
a failure that names the host:port and the step that was waiting. The abandoned
worker keeps running only until its driver's own (bounded) timeout fires; it
cannot hold a request or the event loop. It runs on the connector executor
(``services.connector_executor``): a pool of its own with a per-workspace cap,
where a worker that outlives the deadline is counted until it really ends.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import contextvars
import logging
import os
import socket
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

#: Under the MCP client's own request timeout, so the caller gets our answer.
DEFAULT_DEADLINE_SECONDS = 45.0

#: Interactive reads/probes the operator waits on. A legitimate query can run
#: longer than a plan/preflight, so these get a more generous default (D181).
DEFAULT_QUERY_DEADLINE_SECONDS = 120.0

#: Tools that open connections to a customer system on the request path.
DEADLINE_TOOLS = frozenset(
    {
        "plan_transfer",
        "start_transfer",
        "plan_transfer_route",
        "start_dataset_transfer",
        "sample_connector_object",
        "list_connector_objects",
        "introspect_connector_schema",
        "rank_connector_tables",
        "map_connector_schemas",
        "diff_schemas",
        "compare_connectors",
    }
)

#: Interactive tools that also dial out but had no deadline at all (D181).
QUERY_DEADLINE_TOOLS = frozenset({"run_query", "test_connector", "aggregate_data"})


def _seconds_from_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    try:
        value = float(raw) if raw.strip() else default
    except ValueError:
        value = default
    return value if value > 0 else default


def deadline_seconds(tool: str = "") -> float:
    if tool in QUERY_DEADLINE_TOOLS:
        return _seconds_from_env(
            "DATAFLOW_QUERY_TOOL_DEADLINE_SECONDS", DEFAULT_QUERY_DEADLINE_SECONDS
        )
    return _seconds_from_env("DATAFLOW_TOOL_DEADLINE_SECONDS", DEFAULT_DEADLINE_SECONDS)


@dataclass
class _Slot:
    """One step in flight on one thread."""

    step: str = ""
    role: str = ""
    target: str = ""
    #: ``(role, host:port)`` of every other system the same step also reaches.
    also: tuple[tuple[str, str], ...] = ()
    #: The endpoint this thread is talking to right now (:func:`waiting_on`).
    waiting: str = ""
    #: True for a slot :func:`waiting_on` opened on a thread that had none.
    opened_by_wait: bool = False


@dataclass
class _Progress:
    """What the running tool is waiting on; read by the deadline watcher.

    ``slots`` holds one entry per thread working for the call: the call's own
    worker and any probe it runs in parallel (:func:`parallel_steps`). A probe
    that finished takes its entry away, so a timeout names only what was still
    running when the deadline passed.
    """

    started: float = field(default_factory=time.monotonic)
    limit: float = 0.0
    slots: dict[int, _Slot] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    #: Driver connections opened for this call, per worker thread, so an
    #: abandoned worker can be interrupted (D281). Never stores a socket or
    #: file descriptor: those are read from the live connection at interrupt
    #: time, so a closed connection whose descriptor was reused is left alone.
    connections: dict[int, dict[int, Any]] = field(default_factory=dict)
    #: The call was abandoned (``True``) or these worker threads were: a
    #: connection they open afterwards is interrupted as soon as it registers,
    #: so a worker that carries on (a retry, the next step) cannot start new
    #: blocking I/O nobody will end.
    interrupted: bool = False
    interrupted_threads: set[int] = field(default_factory=set)

    # Read-only views of the call's own (first) step, kept for callers that
    # inspect a single step.
    def _main(self) -> _Slot:
        return next(iter(self.slots.values()), _Slot())

    @property
    def step(self) -> str:
        return self._main().step

    @property
    def role(self) -> str:
        return self._main().role

    @property
    def target(self) -> str:
        return self._main().target

    @property
    def also(self) -> tuple[tuple[str, str], ...]:
        return self._main().also


_progress: contextvars.ContextVar[_Progress | None] = contextvars.ContextVar(
    "df_tool_progress", default=None
)

#: A tighter deadline (monotonic) for one optional step: see :func:`bounded_step`.
_step_deadline: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "df_tool_step_deadline", default=None
)

#: Seconds a tool keeps back, after its optional steps, to finish its answer.
ANSWER_RESERVE_SECONDS = 4.0


def remaining_seconds() -> float | None:
    """Seconds left before this tool call's deadline; ``None`` outside a tool.

    Inside :func:`bounded_step` it is the step's own remaining budget.
    """
    progress = _progress.get()
    if progress is None or progress.limit <= 0:
        return None
    left = progress.limit - (time.monotonic() - progress.started)
    step_end = _step_deadline.get()
    if step_end is not None:
        left = min(left, step_end - time.monotonic())
    return max(0.0, left)


def step_seconds(cap: float, *, reserve: float = ANSWER_RESERVE_SECONDS) -> float:
    """Budget for an optional step: ``cap``, never past the deadline less ``reserve``.

    Outside a tool call there is no deadline, so the step keeps ``cap``.
    """
    left = remaining_seconds()
    if left is None:
        return cap
    return max(0.0, min(cap, left - reserve))


def statement_timeout_seconds() -> float | None:
    """How long one statement sent for this tool call may run on the server.

    A tool that gave up at its deadline left the statement running for nobody:
    PostgreSQL reader sessions carry ``statement_timeout = 0`` and the query
    route allowed 120s, so the abandoned worker stayed busy for as long as the
    server kept scanning (D181). Bounding every statement by the call's own
    remaining budget lets the server cancel it and the worker return by itself.
    ``None`` outside a tool call (transfer jobs keep their own settings).
    """
    left = remaining_seconds()
    if left is None:
        return None
    # A statement is never given less than a second: a zero timeout means
    # "no limit" to most servers.
    return max(1.0, left)


def describe_target(conn: Any) -> str:
    """``host:port`` of a connector dict, without any credential."""
    if not isinstance(conn, dict):
        return ""
    cfg = conn.get("config") if isinstance(conn.get("config"), dict) else {}
    host = str(conn.get("host") or cfg.get("host") or "").strip()
    port = conn.get("port") or cfg.get("port") or ""
    if not host:
        raw = str(
            conn.get("connection_string") or cfg.get("connection_string") or ""
        ).strip()
        if raw:
            parsed = urlparse(raw)
            host = parsed.hostname or ""
            port = port or parsed.port or ""
    if not host:
        # D187: a Snowflake connector may carry only its account (top level or
        # under ``extra``). Without it the destination dropped out of a
        # timeout message and the source was blamed alone.
        extra = conn.get("extra") if isinstance(conn.get("extra"), dict) else {}
        account = str(
            conn.get("account") or cfg.get("account") or extra.get("account") or ""
        ).strip()
        if account:
            return account
        return ""
    return f"{host}:{port}" if port else host


def _system_label(conn: Any) -> str:
    """``host:port`` or, for a system without one, its connector name / type."""
    target = describe_target(conn)
    if target or not isinstance(conn, dict):
        return target
    name = str(conn.get("name") or "").strip()
    kind = str(conn.get("type") or conn.get("format") or "").strip()
    if name and kind:
        return f"{kind} connector '{name}'"
    return name or kind


def note_step(
    step: str,
    conn: Any = None,
    *,
    role: str = "",
    also: Sequence[tuple[str, Any]] = (),
    if_unset: bool = False,
) -> None:
    """Record the step about to block on a customer system (cheap, thread-safe).

    A step that reaches several systems (the preflight reads the source and the
    destination) passes the others as ``also=((role, conn), ...)`` so a timeout
    names every system it may have been waiting on, not just the first.
    """
    progress = _progress.get()
    if progress is None:
        return
    slot = _Slot(
        step=step,
        role=role,
        target=describe_target(conn) if conn is not None else "",
        also=tuple(
            (other_role, target)
            for other_role, other in also
            # A system with no host still has a name: never drop it (D187).
            if (target := _system_label(other))
        ),
    )
    with progress.lock:
        if if_unset and progress.slots.get(threading.get_ident()) is not None:
            # A more precise step is already recorded on this thread.
            return
        progress.slots[threading.get_ident()] = slot


def _host_of(target: str) -> str:
    text = (target or "").strip().lower()
    if "://" in text:
        text = urlparse(text).hostname or text
    if text.count(":") == 1:
        text = text.split(":", 1)[0]
    return text


def _same_system(a: str, b: str) -> bool:
    """``acme-xy123`` (a Snowflake account) is ``acme-xy123.snowflakecomputing.com:443``."""
    ha, hb = _host_of(a), _host_of(b)
    if not ha or not hb:
        return False
    return ha == hb or ha.startswith(hb + ".") or hb.startswith(ha + ".")


@contextlib.contextmanager
def waiting_on(conn: Any):
    """Mark the system this thread is talking to for the duration of the block.

    A step such as the preflight reaches the source *and* the destination; a
    timeout used to say only "one of them is slow" or blame the source while a
    Snowflake destination login held the call (D187). The connection layers
    (the connector dial scope and the Snowflake login) wrap their work in this,
    so a timeout names the endpoint that was actually waited on.
    """
    progress = _progress.get()
    target = _system_label(conn) if conn is not None and not isinstance(conn, str) else str(conn or "")
    if progress is None or not target:
        yield
        return
    ident = threading.get_ident()
    with progress.lock:
        slot = progress.slots.get(ident)
        opened = slot is None
        if opened:
            slot = _Slot(step=f"talking to {target}", opened_by_wait=True)
            progress.slots[ident] = slot
        previous = slot.waiting
        slot.waiting = target
    try:
        yield
    finally:
        with progress.lock:
            if opened and progress.slots.get(ident) is slot:
                progress.slots.pop(ident, None)
            else:
                slot.waiting = previous


def _waited(slot: _Slot) -> tuple[str, str]:
    """``(role, host:port)`` of the step's system the slot is talking to.

    A dial scope may know only the host; the step's own record of that system
    carries its port, so the message names the full endpoint.
    """
    for role, target in ((slot.role, slot.target), *slot.also):
        if target and _same_system(slot.waiting, target):
            return role or "source", target
    return (slot.role if not slot.also else ""), slot.waiting


def _clear_step() -> None:
    progress = _progress.get()
    if progress is None:
        return
    with progress.lock:
        progress.slots.pop(threading.get_ident(), None)


def register_connection(conn: Any) -> None:
    """Remember a driver connection this thread opened for the current tool call.

    A call abandoned at its deadline is interrupted through these: a worker
    blocked on a connected socket that never answers (a tunnel that accepted
    and went silent) has no server-side timeout that can reach it, and such
    workers filled the connector pool for hours (D281). A no-op outside a tool
    call, so transfer jobs' sessions are never touched.
    """
    progress = _progress.get()
    if progress is None or conn is None:
        return
    thread = threading.get_ident()
    with progress.lock:
        late = progress.interrupted or thread in progress.interrupted_threads
        if not late:
            progress.connections.setdefault(thread, {})[id(conn)] = conn
    if late:
        try:
            _interrupt_connection(conn)
        except Exception as exc:  # noqa: BLE001 - best effort
            logger.debug("late connection interrupt skipped: %s", exc)


def unregister_connection(conn: Any) -> None:
    """Forget a connection handed back to a pool (another consumer owns it now)."""
    progress = _progress.get()
    if progress is None or conn is None:
        return
    with progress.lock:
        for thread_conns in progress.connections.values():
            thread_conns.pop(id(conn), None)


def _shutdown_descriptor(fd: int) -> None:
    # ``shutdown`` acts on the kernel socket, so a duplicate descriptor ends
    # the recv the worker thread is blocked in without owning its object.
    dup = socket.socket(fileno=os.dup(fd))
    try:
        dup.shutdown(socket.SHUT_RDWR)
    finally:
        dup.close()


def _interrupt_connection(conn: Any) -> bool:
    """End the blocking I/O of one driver connection from another thread."""
    # pymysql keeps the socket object; a closed connection has none.
    sock = getattr(conn, "_sock", None)
    if sock is not None and hasattr(sock, "shutdown"):
        sock.shutdown(socket.SHUT_RDWR)
        return True
    # psycopg2 (and anything else with a live descriptor): closed ones raise.
    fileno = getattr(conn, "fileno", None)
    if callable(fileno) and not getattr(conn, "closed", False):
        _shutdown_descriptor(int(fileno()))
        return True
    # python-oracledb: break the call in flight on the connection.
    cancel = getattr(conn, "cancel", None)
    if callable(cancel):
        cancel()
        return True
    return False


def interrupt_connections(progress: _Progress, thread: int | None = None) -> int:
    """Interrupt the connections a call (or one of its threads) still holds.

    Returns how many were interrupted. Best effort: a connection that is
    already closed, or a driver with no reachable socket (pyodbc carries the
    call's budget as its own ``timeout`` instead), is skipped.
    """
    with progress.lock:
        if thread is None:
            progress.interrupted = True
            conns = [c for bucket in progress.connections.values() for c in bucket.values()]
            progress.connections.clear()
        else:
            progress.interrupted_threads.add(thread)
            conns = list(progress.connections.pop(thread, {}).values())
    done = 0
    for conn in conns:
        try:
            if _interrupt_connection(conn):
                done += 1
        except Exception as exc:  # noqa: BLE001 - best effort, per connection
            logger.debug("connection interrupt skipped: %s", exc)
    return done


def stuck_label(tool: str, progress: _Progress) -> str:
    """``tool: step (role host:port)`` for the pool's own messages."""
    with progress.lock:
        slots = [s for s in progress.slots.values() if s.step]
    if not slots:
        return tool
    slot = slots[0]
    where = f" ({slot.role or 'source'} {slot.target})" if slot.target else ""
    return f"{tool}: {slot.step}{where}"


def _timeout_message(tool: str, seconds: float, progress: _Progress) -> str:
    with progress.lock:
        slots = [s for s in progress.slots.values() if s.step]
    waited = f"{seconds:g}s"
    if len(slots) > 1:
        return _parallel_timeout_message(tool, waited, slots)
    slot = slots[0] if slots else _Slot()
    step, role, target, also = slot.step, slot.role, slot.target, slot.also
    if slot.waiting:
        # The connection layer said which endpoint it was talking to (D187).
        waited_role, endpoint = _waited(slot)
        subject = (
            f"{waited_role} unreachable/slow: {endpoint}"
            if waited_role
            else f"unreachable/slow: {endpoint}"
        )
        where = f" (step: {step})" if step and not slot.opened_by_wait else ""
        return (
            f"{subject} did not answer within {waited}{where}. Check the host and "
            "port, the firewall or VPN, and that the system is healthy (and its "
            "warehouse or server is running), then try again."
        )
    if target and also:
        # The step reads more than one system and cannot tell which one held it:
        # blaming the first would send the operator to a healthy database.
        named = ", ".join(f"the {r} ({t})" for r, t in ((role or "source", target), *also))
        where = f" (step: {step})" if step else ""
        return (
            f"{tool} did not finish within {waited}{where}. It reads {named}, and "
            "one of them is slow or unreachable. Check the hosts and ports, the "
            "firewall or VPN, and that each system is healthy and its warehouse or "
            "server is running, then try again."
        )
    if target:
        subject = f"{role} unreachable/slow: {target}" if role else f"unreachable/slow: {target}"
        where = f" (step: {step})" if step else ""
        return (
            f"{subject} did not answer within {waited}{where}. "
            "Check the host and port, the firewall or VPN, and that the database "
            "is healthy, then try again."
        )
    if step and role and not target:
        return (
            f"{role} unreachable/slow: it did not answer within {waited} "
            f"(step: {step}). Check that the {role} is reachable and healthy "
            "(and its warehouse or server is running), then try again."
        )
    if step and not role:
        # ``note_step(step)`` with no system: Datawrap's own work (Map). Naming a
        # database here would send the operator to a healthy host (D189).
        return (
            f"{tool} did not finish within {waited} (step: {step}). That step is "
            "Datawrap's own work and waits on neither the source nor the "
            "destination, so neither system is the cause; nothing was staged. "
            "Try again, and report it if it repeats."
        )
    where = f" at step: {step}" if step else ""
    return (
        f"{tool} did not finish within {waited}{where}. A system it reads from "
        "is unreachable or slow; nothing was staged."
    )


def _parallel_timeout_message(tool: str, waited: str, slots: list[_Slot]) -> str:
    """Several probes were still running: name each one and the system it reads."""
    running = []
    systems: dict[str, set[str]] = {}
    for slot in slots:
        reads = [(slot.role, slot.target), *slot.also] if slot.target else []
        if slot.waiting:
            # Narrowed by the connection layer to the endpoint it was on (D187).
            reads = [_waited(slot)]
        for r, t in reads:
            systems.setdefault(r or "source", set()).add(t)
        where = ", ".join(f"the {r or 'source'} ({t})" for r, t in reads)
        running.append(f"{slot.step} ({where})" if where else f"{slot.step} (Datawrap's own work)")
    if len(systems) == 1:
        role, targets = next(iter(systems.items()))
        subject = f"{role} unreachable/slow: {', '.join(sorted(targets))}"
        return (
            f"{subject} did not answer within {waited}. Still running: "
            + "; ".join(running)
            + ". Check the host and port, the firewall or VPN, and that the "
            "database is healthy, then try again."
        )
    return (
        f"{tool} did not finish within {waited}. Still running: "
        + "; ".join(running)
        + ". One of these systems is slow or unreachable. Check the hosts and "
        "ports, the firewall or VPN, and that each system is healthy and its "
        "warehouse or server is running, then try again."
    )


def run_with_deadline(
    tool: str,
    fn: Callable[[], Any],
    on_timeout: Callable[[str], Any],
    *,
    seconds: float | None = None,
) -> Any:
    """Run ``fn`` on the connector executor; return its value or ``on_timeout(message)``.

    The deadline, a workspace already at its share of calls, and a stuck worker
    that is still being counted all answer through ``on_timeout`` with a message
    that names what was waiting.
    """
    from services.connector_executor import (
        ConnectorCallTimeout,
        ConnectorCapacityExceeded,
        get_executor,
    )
    from services.workspace_filter import current_workspace_stamp

    from services import statement_budget
    from services.endpoint_session import endpoint_session_scope

    statement_budget.install()
    limit = deadline_seconds(tool) if seconds is None else seconds
    progress = _Progress(limit=limit)

    def work() -> Any:
        # ``_progress`` is set on the worker's copy of the caller's context, so
        # only this call sees it. Its clock started at submit, like the
        # executor's own deadline.
        _progress.set(progress)
        # One session per endpoint for the whole call, whatever the engine.
        with endpoint_session_scope():
            return fn()

    try:
        return get_executor().run(
            tool,
            work,
            workspace=current_workspace_stamp(for_write=False),
            seconds=limit,
            describe_timeout=lambda: _timeout_message(tool, limit, progress),
            label=lambda: stuck_label(tool, progress),
            # The worker that outlives the deadline is blocked in a driver:
            # end its I/O so it returns and frees its pool worker (D281).
            interrupt=lambda: interrupt_connections(progress),
        )
    except (ConnectorCallTimeout, ConnectorCapacityExceeded) as exc:
        return on_timeout(str(exc))


@dataclass
class Step:
    """One independent probe for :func:`parallel_steps`."""

    label: str
    fn: Callable[[], Any]
    conn: Any = None
    role: str = ""


@dataclass
class StepResult:
    value: Any = None
    error: BaseException | None = None
    #: The step had not finished when its budget ran out.
    timed_out: bool = False
    seconds: float = 0.0


def _run_step(step: Step) -> StepResult:
    started = time.monotonic()
    note_step(step.label, step.conn, role=step.role)
    try:
        return StepResult(value=step.fn(), seconds=time.monotonic() - started)
    except BaseException as exc:  # noqa: BLE001 - handed back to the caller
        return StepResult(error=exc, seconds=time.monotonic() - started)
    finally:
        _clear_step()


def parallel_steps(
    steps: Sequence[Step],
    *,
    seconds: float | None = None,
    bound_each: bool = False,
) -> list[StepResult]:
    """Run independent probes at once on the connector executor.

    A plan read the destination, then the source, then sampled, then probed the
    destination again and counted the source: every wait stacked, so a slow
    Snowflake login plus a big PostgreSQL table outlasted the deadline even
    though no single read did (D187). Steps that do not depend on each other
    now overlap; the call costs its slowest probe, not their sum.

    The calling thread runs steps itself and takes back any step no worker has
    started yet, so a saturated pool degrades to the old serial order instead
    of deadlocking. ``seconds`` bounds the whole group (default: the tool's
    remaining budget); a step still running then is ``timed_out`` and its
    statements are cancelled by their own server-side timeout. Outside a tool
    call the steps simply run in order.

    ``bound_each=True`` runs every step under ``seconds`` as its own step
    budget too (its statements carry it), for optional probes.
    """
    if not steps:
        return []
    progress = _progress.get()
    if progress is None:
        return [_run_step(step) for step in steps]
    from services.connector_executor import get_executor

    left = remaining_seconds()
    budget = left if seconds is None else (seconds if left is None else min(seconds, left))
    deadline = time.monotonic() + max(0.0, budget or 0.0)
    if bound_each:
        steps = [
            Step(st.label, _under_step_deadline(st.fn, deadline), st.conn, st.role)
            for st in steps
        ]
    if len(steps) == 1:
        return [_run_step(steps[0])]
    pool = get_executor()
    # The caller's own step is put back once the group is done.
    with progress.lock:
        own = progress.slots.get(threading.get_ident())
    futures: list[concurrent.futures.Future | None] = [None] * len(steps)
    results: list[StepResult | None] = [None] * len(steps)
    threads: list[dict[str, int]] = [{} for _ in steps]
    for index, step in enumerate(steps[1:], start=1):
        futures[index] = pool.spawn(
            f"step:{step.label}",
            _on_recorded_thread(lambda s=step: _run_step(s), threads[index]),
            label=lambda s=step: _step_label(s),
        )
    try:
        results[0] = _run_step(steps[0])
        for index in range(1, len(steps)):
            future = futures[index]
            assert future is not None
            if future.cancel():
                # Nobody picked it up: run it here rather than wait for a worker.
                results[index] = _run_step(steps[index])
                continue
            try:
                results[index] = future.result(timeout=max(0.0, deadline - time.monotonic()))
            except concurrent.futures.TimeoutError:
                results[index] = StepResult(timed_out=True, seconds=budget or 0.0)
                _abandon_step(pool, future, progress, threads[index])
    finally:
        with progress.lock:
            if own is not None:
                progress.slots[threading.get_ident()] = own
            else:
                progress.slots.pop(threading.get_ident(), None)
    return [r if r is not None else StepResult(timed_out=True) for r in results]


def _on_recorded_thread(fn: Callable[[], Any], cell: dict[str, int]) -> Callable[[], Any]:
    """Run ``fn`` and record the worker thread it ran on in ``cell``."""

    def run() -> Any:
        thread = threading.get_ident()
        cell["thread"] = thread
        try:
            return fn()
        finally:
            # This pool thread may serve another probe of the same call next:
            # an interrupt meant for this probe must not follow it there.
            progress = _progress.get()
            if progress is not None:
                with progress.lock:
                    progress.interrupted_threads.discard(thread)
                    progress.connections.pop(thread, None)

    return run


def _step_label(step: Step) -> str:
    target = describe_target(step.conn) if step.conn is not None else ""
    where = f" ({step.role or 'source'} {target})" if target else ""
    return f"{step.label}{where}"


def _abandon_step(
    pool: Any,
    future: concurrent.futures.Future,
    progress: _Progress,
    cell: dict[str, int],
) -> None:
    """A probe that outlived its budget: count it stuck and end its I/O.

    Only that probe's own connections are interrupted; the call that started
    it goes on with a partial answer on its other connections.
    """
    thread = cell.get("thread")
    pool.abandon(
        future,
        interrupt=(
            (lambda: interrupt_connections(progress, thread)) if thread is not None else None
        ),
    )


def _under_step_deadline(fn: Callable[[], Any], deadline: float) -> Callable[[], Any]:
    def run() -> Any:
        token = _step_deadline.set(deadline)
        try:
            return fn()
        finally:
            _step_deadline.reset(token)

    return run


def bounded_step(
    label: str,
    fn: Callable[[], Any],
    *,
    seconds: float,
    conn: Any = None,
    role: str = "",
) -> StepResult:
    """Run one optional step under its own budget, inside the tool's.

    A slow optional step (an exact COUNT(*) on a big table, a uniqueness scan)
    used to spend the whole tool deadline and kill the call. Here it gets
    ``seconds`` at most (never past the tool's deadline less the answer
    reserve); its statements carry that budget as their server-side timeout,
    and the caller gets ``timed_out=True`` to answer with a labelled partial
    result instead of failing. Outside a tool call it runs unbounded, as before.
    """
    if _progress.get() is None:
        return _run_step(Step(label, fn, conn, role))
    budget = step_seconds(seconds)
    if budget <= 0:
        return StepResult(timed_out=True)
    from services.connector_executor import get_executor

    def inner() -> StepResult:
        token = _step_deadline.set(time.monotonic() + budget)
        try:
            return _run_step(Step(label, fn, conn, role))
        finally:
            _step_deadline.reset(token)

    cell: dict[str, int] = {}
    pool = get_executor()
    future = pool.spawn(
        f"step:{label}",
        _on_recorded_thread(inner, cell),
        label=lambda: _step_label(Step(label, fn, conn, role)),
    )
    started_by = time.monotonic() + 0.25
    while not (future.running() or future.done()) and time.monotonic() < started_by:
        time.sleep(0.005)
    if future.cancel():
        # No worker picked it up: run it here, still under the step budget
        # (its statements carry it).
        return inner()
    try:
        return future.result(timeout=budget)
    except concurrent.futures.TimeoutError:
        progress = _progress.get()
        if progress is not None:
            _abandon_step(pool, future, progress, cell)
        return StepResult(timed_out=True, seconds=budget)


#: Validate outside a tool call (Studio REST, engine Execute preflight). Same
#: default as the interactive reads/probes an operator waits on (D181); it is
#: that existing budget, not a new or larger one.
PREFLIGHT_DEADLINE_ENV = "DATAFLOW_PREFLIGHT_DEADLINE_SECONDS"


def preflight_deadline_seconds() -> float:
    return _seconds_from_env(PREFLIGHT_DEADLINE_ENV, DEFAULT_QUERY_DEADLINE_SECONDS)


@contextlib.contextmanager
def deadline_scope(seconds: float, progress: _Progress | None = None):
    """Bind a wall-clock budget to blocking work on this thread.

    Outside a tool call ``remaining_seconds`` was ``None``, so neither
    :func:`bounded_step` nor the statement budget applied: a REST or engine
    preflight ran every catalog statement unbounded and hung on a server with
    thousands of schemas (QA round 11, Ensembl). Inside the scope both apply,
    and pooled sessions carry ``statement_timeout`` / ``MAX_EXECUTION_TIME``.
    An enclosing tool deadline is tighter and is kept.
    """
    from services import statement_budget

    statement_budget.install()
    if _progress.get() is not None:
        yield _progress.get()
        return
    bound = progress or _Progress(limit=seconds)
    token = _progress.set(bound)
    try:
        yield bound
    finally:
        _progress.reset(token)


def without_deadline(rows: Any) -> Any:
    """Iterate ``rows`` with the deadline lifted for each fetch.

    The pre-write population walk is not a catalog probe: a transfer job keeps
    its long socket timeouts for it, as before the preflight deadline existed.
    Set and reset inside one resume, so nothing leaks into the caller.
    """
    if rows is None:
        return None
    iterator = iter(rows)

    def _gen() -> Any:
        while True:
            token = _progress.set(None)
            try:
                item = next(iterator)
            except StopIteration:
                return
            finally:
                _progress.reset(token)
            yield item

    return _gen()


def deadline_timeout_message(tool: str, seconds: float, progress: _Progress) -> str:
    """The operator-facing message naming what was still waiting at the deadline."""
    return _timeout_message(tool, seconds, progress)


@contextlib.contextmanager
def deadline_context_for_tests(seconds: float):
    """Bind a tool budget to the current thread (tests and scripts only)."""
    token = _progress.set(_Progress(limit=seconds))
    try:
        yield
    finally:
        _progress.reset(token)
