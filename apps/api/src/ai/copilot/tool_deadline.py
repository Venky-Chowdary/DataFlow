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
        return ""
    return f"{host}:{port}" if port else host


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
            if (target := describe_target(other))
        ),
    )
    with progress.lock:
        if if_unset and progress.slots.get(threading.get_ident()) is not None:
            # A more precise step is already recorded on this thread.
            return
        progress.slots[threading.get_ident()] = slot


def _clear_step() -> None:
    progress = _progress.get()
    if progress is None:
        return
    with progress.lock:
        progress.slots.pop(threading.get_ident(), None)


def _timeout_message(tool: str, seconds: float, progress: _Progress) -> str:
    with progress.lock:
        slots = [s for s in progress.slots.values() if s.step]
    waited = f"{seconds:g}s"
    if len(slots) > 1:
        return _parallel_timeout_message(tool, waited, slots)
    slot = slots[0] if slots else _Slot()
    step, role, target, also = slot.step, slot.role, slot.target, slot.also
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
            workspace=current_workspace_stamp(),
            seconds=limit,
            describe_timeout=lambda: _timeout_message(tool, limit, progress),
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
    for index, step in enumerate(steps[1:], start=1):
        futures[index] = pool.spawn(f"step:{step.label}", lambda s=step: _run_step(s))
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
    finally:
        with progress.lock:
            if own is not None:
                progress.slots[threading.get_ident()] = own
            else:
                progress.slots.pop(threading.get_ident(), None)
    return [r if r is not None else StepResult(timed_out=True) for r in results]


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

    future = get_executor().spawn(f"step:{label}", inner)
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
        return StepResult(timed_out=True, seconds=budget)


@contextlib.contextmanager
def deadline_context_for_tests(seconds: float):
    """Bind a tool budget to the current thread (tests and scripts only)."""
    token = _progress.set(_Progress(limit=seconds))
    try:
        yield
    finally:
        _progress.reset(token)
