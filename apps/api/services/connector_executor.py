"""A bounded executor of its own for calls that reach a customer's system (D181).

Connector actions used to run on whatever thread asked: ``asyncio.to_thread`` on
the default pool shared with every other route. A source that accepted the
connection and never answered held its worker for good, and enough of them
queued every later connector call behind them while ``/health`` (which never
touches that pool) kept answering.

Every pilot/MCP tool that dials out now runs here instead:

* a fixed number of workers, so connector I/O cannot take threads from anything
  else;
* a per-workspace cap on calls in flight, so one tenant cannot take them all;
* a deadline per call: the caller gets a typed error at the deadline and the
  worker that is still stuck is *counted* (``abandoned``) until it really ends;
* counters that ``/health`` and ``/health/ready`` publish.

An abandoned call gives its workspace slot back at the deadline, and the pool
gets a replacement worker for it (up to ``max_workers`` extra), so a stuck
source cannot lock its workspace, or everyone, out of connector calls (QA
D181: "limit 6 in flight" refused every call while abandoned calls sat in the
slots). Abandoned workers themselves end on their own: every statement sent
during a tool call carries the call's remaining budget as its server-side
timeout (``tool_deadline.statement_timeout_seconds``).

``abandoned`` / ``abandoned_total`` are the abandoned calls whose worker is
still running (they return to 0 when those workers end);
``abandoned_lifetime`` counts every abandon since start.

A server-side statement timeout cannot end a worker that is blocked on a
connected socket that never answers (a tunnel that accepts and goes silent,
a half-open TCP session): those workers lived for hours, the replacement cap
filled up, and from then on every call queued forever and reported "did not
answer within 45s" against a healthy source (QA D281: ``in_flight`` 24 over
``max_workers`` 16, ``abandoned`` 21, every row read hung, ``/health`` fine).
Three things changed:

* an abandoned call is *interrupted*: the caller passes ``interrupt`` (the
  tool deadline shuts the sockets the worker opened for this call), so the
  blocked driver raises and the worker ends within seconds;
* a parallel probe (``spawn``) that outlives its budget is abandoned the same
  way (``abandon``) instead of silently holding a worker with no replacement;
* a call that cannot start because stuck workers fill the pool fails at once,
  and one that queued without a worker until its deadline says so, naming the
  stuck calls, instead of blaming the source it never reached.

Settings (``DATAWRAP_*`` is read before ``DATAFLOW_*``):

``DATAFLOW_CONNECTOR_POOL_WORKERS``          workers (default 16)
``DATAFLOW_CONNECTOR_WORKSPACE_MAX``         calls in flight per workspace (default 6)
``DATAFLOW_CONNECTOR_READY_MAX_AGE_SECONDS`` readiness fails when the oldest call
                                             has run longer than this (default 300)
"""

from __future__ import annotations

import concurrent.futures
import contextvars
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from services.brand_env import getenv_brand

logger = logging.getLogger(__name__)

DEFAULT_WORKERS = 16
DEFAULT_WORKSPACE_MAX = 6
DEFAULT_READY_MAX_AGE_SECONDS = 300.0
_NO_WORKSPACE = "(none)"


class ConnectorCallTimeout(TimeoutError):
    """A connector call did not finish inside its deadline."""


class ConnectorCapacityExceeded(RuntimeError):
    """A workspace already has its share of connector calls in flight."""


class ConnectorPoolSaturated(ConnectorCapacityExceeded):
    """Every worker the pool may run is held by a call that is still stuck.

    The call that raised it never reached a customer system, so the message
    names the pool and the stuck calls, not a source.
    """


def _int_setting(name: str, default: int) -> int:
    try:
        value = int(float(str(getenv_brand(name, "") or "").strip() or default))
    except ValueError:
        return default
    return value if value > 0 else default


def _float_setting(name: str, default: float) -> float:
    try:
        value = float(str(getenv_brand(name, "") or "").strip() or default)
    except ValueError:
        return default
    return value if value > 0 else default


class _Call:
    __slots__ = (
        "workspace",
        "tool",
        "submitted",
        "started",
        "abandoned",
        "child",
        "label",
        "interrupt",
    )

    def __init__(
        self,
        workspace: str,
        tool: str,
        *,
        child: bool = False,
        label: Callable[[], str] | None = None,
        interrupt: Callable[[], None] | None = None,
    ) -> None:
        self.workspace = workspace
        self.tool = tool
        self.submitted = time.monotonic()
        self.started: float | None = None
        self.abandoned = False
        #: A probe run in parallel for an admitted call: not charged to the
        #: workspace cap a second time.
        self.child = child
        #: Short operator-facing text for what the call is waiting on
        #: ("sample_connector_object: reading rows (source 10.0.0.5:3306)").
        self.label = label
        #: Ends the worker's blocking I/O once the call is abandoned.
        self.interrupt = interrupt

    def describe(self, now: float) -> str:
        text = ""
        if self.label is not None:
            try:
                text = str(self.label() or "")
            except Exception:  # noqa: BLE001 - a label must never fail the pool
                text = ""
        text = text or self.tool
        age = now - (self.started if self.started is not None else self.submitted)
        return f"{text} ({age:.0f}s)"


class ConnectorExecutor:
    def __init__(self) -> None:
        self.max_workers = _int_setting("DATAFLOW_CONNECTOR_POOL_WORKERS", DEFAULT_WORKERS)
        self.workspace_max = _int_setting(
            "DATAFLOW_CONNECTOR_WORKSPACE_MAX", DEFAULT_WORKSPACE_MAX
        )
        self._pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers, thread_name_prefix="connector-io"
        )
        self._lock = threading.Lock()
        self._calls: set[_Call] = set()
        #: Parallel probes by their future, so the caller can abandon one.
        self._spawned: dict[concurrent.futures.Future, _Call] = {}
        self._rejected_total = 0
        self._abandoned_total = 0

    def run(
        self,
        tool: str,
        fn: Callable[[], Any],
        *,
        workspace: str,
        seconds: float,
        describe_timeout: Callable[[], str],
        label: Callable[[], str] | None = None,
        interrupt: Callable[[], None] | None = None,
    ) -> Any:
        """Run ``fn`` on the pool; return its value or raise at the deadline.

        ``label`` names what the call waits on (for the pool's own messages);
        ``interrupt`` is called once if the call is abandoned, to end the
        worker's blocking I/O.
        """
        key = workspace or _NO_WORKSPACE
        call = _Call(key, tool, label=label, interrupt=interrupt)
        with self._lock:
            stuck = self._stuck_message_locked(tool)
            if stuck:
                self._rejected_total += 1
                raise ConnectorPoolSaturated(stuck)
            # An abandoned call is over for its caller: it no longer holds the
            # workspace's share (its worker is still counted by the pool).
            held = sum(
                1
                for c in self._calls
                if c.workspace == key and not c.abandoned and not c.child
            )
            if held >= self.workspace_max:
                self._rejected_total += 1
                raise ConnectorCapacityExceeded(
                    f"Workspace {key} already has {held} connector calls in flight "
                    f"(limit {self.workspace_max}); {tool} was not started. "
                    "Wait for them to finish or check the sources they are waiting on."
                )
            self._calls.add(call)
        ctx = contextvars.copy_context()

        future = self._pool.submit(self._work(call, ctx, fn))
        try:
            return future.result(timeout=seconds)
        except concurrent.futures.TimeoutError:
            never_started = False
            with self._lock:
                if future.cancel():
                    # Never started: nothing is stuck, give the slot back. The
                    # pool, not the source, held this call (D281).
                    self._calls.discard(call)
                    never_started = True
                    message = self._never_started_message_locked(tool, seconds)
                elif call in self._calls:
                    self._abandon_locked(call)
            if never_started:
                logger.warning(
                    "connector call %s never started in %.0fs (workspace %s): %s",
                    tool,
                    seconds,
                    key,
                    message,
                )
                raise ConnectorPoolSaturated(message) from None
            self._interrupt(call)
            message = describe_timeout()
            logger.warning(
                "connector call %s abandoned after %.0fs (workspace %s): %s",
                tool,
                seconds,
                key,
                message,
            )
            raise ConnectorCallTimeout(message) from None

    def abandon(
        self,
        future: concurrent.futures.Future,
        *,
        interrupt: Callable[[], None] | None = None,
    ) -> bool:
        """Give up on a probe started with :meth:`spawn` that is still running.

        A probe that outlived its step budget used to keep its worker without
        being counted as abandoned, so the pool had no replacement for it and
        nothing ever ended it. Returns True when the probe was running and is
        now abandoned (and interrupted when ``interrupt`` or the probe's own
        interrupt is set); False when it had already finished or never started
        (a never-started probe is cancelled and leaves the pool).
        """
        with self._lock:
            call = self._spawned.get(future)
            if call is None or future.done():
                return False
            if future.cancel():
                self._calls.discard(call)
                self._spawned.pop(future, None)
                return False
            if interrupt is not None:
                call.interrupt = interrupt
            self._abandon_locked(call)
        self._interrupt(call)
        return True

    def _abandon_locked(self, call: _Call) -> None:
        if call.abandoned:
            return
        call.abandoned = True
        self._abandoned_total += 1
        self._resize_locked()

    @staticmethod
    def _interrupt(call: _Call) -> None:
        fn = call.interrupt
        if fn is None:
            return
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - best effort, the pool stays up
            logger.debug("connector call %s interrupt failed: %s", call.tool, exc)

    def _stuck_locked(self) -> list[_Call]:
        return [c for c in self._calls if c.started is not None and c.abandoned]

    def _stuck_message_locked(self, tool: str) -> str:
        """Why ``tool`` cannot start now: every worker is held by a stuck call."""
        stuck = self._stuck_locked()
        cap = self._pool._max_workers
        if len(stuck) < cap:
            return ""
        return (
            f"{tool} was not started: all {cap} connector pool workers are held by "
            f"{len(stuck)} abandoned calls that are still waiting on their systems "
            f"({self._stuck_summary(stuck)}). Nothing reached the source. The pool "
            "frees them as they end; check those hosts, then try again."
        )

    def _never_started_message_locked(self, tool: str, seconds: float) -> str:
        running = [c for c in self._calls if c.started is not None]
        stuck = self._stuck_locked()
        cap = self._pool._max_workers
        detail = (
            f", {len(stuck)} of them abandoned calls still waiting on their systems "
            f"({self._stuck_summary(stuck)})"
            if stuck
            else ""
        )
        return (
            f"{tool} was never started: the connector pool's {cap} workers were all "
            f"busy for {seconds:g}s ({len(running)} calls running{detail}). Nothing "
            "reached the source. Try again once they end."
        )

    @staticmethod
    def _stuck_summary(stuck: list[_Call], limit: int = 6) -> str:
        now = time.monotonic()
        ordered = sorted(stuck, key=lambda c: c.started or c.submitted)
        shown = "; ".join(c.describe(now) for c in ordered[:limit])
        more = len(ordered) - limit
        return shown + (f"; +{more} more" if more > 0 else "")

    def _work(self, call: _Call, ctx: contextvars.Context, fn: Callable[[], Any]):
        def work() -> Any:
            with self._lock:
                call.started = time.monotonic()
            try:
                return ctx.run(fn)
            finally:
                with self._lock:
                    self._calls.discard(call)
                    if call.abandoned:
                        self._resize_locked()

        return work

    def _resize_locked(self) -> None:
        """One replacement worker per abandoned call still running (capped)."""
        stuck = sum(1 for c in self._calls if c.abandoned)
        # ThreadPoolExecutor reads ``_max_workers`` when it decides whether to
        # start a thread; idle extra threads are harmless once the stuck ones end.
        self._pool._max_workers = self.max_workers + min(stuck, self.max_workers)

    def spawn(
        self,
        tool: str,
        fn: Callable[[], Any],
        *,
        label: Callable[[], str] | None = None,
        interrupt: Callable[[], None] | None = None,
    ) -> concurrent.futures.Future:
        """Start ``fn`` for an already admitted call (a parallel probe).

        It runs in a copy of the caller's context, is counted in the pool
        numbers, and is not charged to the workspace cap again. The caller
        bounds the wait and may ``cancel()`` it to run it inline instead, or
        :meth:`abandon` it when it outlives its budget.
        """
        call = _Call(_NO_WORKSPACE, tool, child=True, label=label, interrupt=interrupt)
        with self._lock:
            self._calls.add(call)
        future = self._pool.submit(self._work(call, contextvars.copy_context(), fn))
        with self._lock:
            self._spawned[future] = call

        def _done(f: concurrent.futures.Future) -> None:
            with self._lock:
                self._spawned.pop(f, None)
                if f.cancelled():
                    self._calls.discard(call)

        future.add_done_callback(_done)
        return future

    def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        with self._lock:
            running = [c for c in self._calls if c.started is not None]
            queued = len(self._calls) - len(running)
            oldest = max((now - c.started for c in running), default=0.0)  # type: ignore[operator]
            per_workspace: dict[str, int] = {}
            for c in self._calls:
                if c.child or c.abandoned:
                    continue
                per_workspace[c.workspace] = per_workspace.get(c.workspace, 0) + 1
            stuck_calls = [c for c in running if c.abandoned]
            stuck = len(stuck_calls)
            cap = self._pool._max_workers
            return {
                "max_workers": self.max_workers,
                "workspace_max": self.workspace_max,
                "in_flight": len(running),
                "queued": queued,
                "abandoned": stuck,
                # Every worker the pool may run is held: no new call can start
                # (QA D281 read this as "every row read hangs").
                "saturated": len(running) >= cap,
                "stuck_calls": [c.describe(now) for c in stuck_calls[:8]],
                # Abandoned calls whose worker still runs: back to 0 once they
                # end. The lifetime count is ``abandoned_lifetime``.
                "abandoned_total": stuck,
                "abandoned_lifetime": self._abandoned_total,
                "rejected_total": self._rejected_total,
                "oldest_in_flight_seconds": round(oldest, 1),
                "per_workspace": per_workspace,
            }

    def readiness(self) -> dict[str, Any]:
        snap = self.snapshot()
        max_age = _float_setting(
            "DATAFLOW_CONNECTOR_READY_MAX_AGE_SECONDS", DEFAULT_READY_MAX_AGE_SECONDS
        )
        reason = ""
        live = snap["in_flight"] - snap["abandoned"]
        if (
            snap["saturated"]
            or live >= snap["max_workers"]
            or snap["abandoned"] >= snap["max_workers"]
        ):
            reason = (
                f"connector pool saturated: {snap['in_flight']} of "
                f"{snap['max_workers']} workers busy ({snap['abandoned']} abandoned "
                f"and still running), {snap['queued']} queued"
            )
        elif snap["oldest_in_flight_seconds"] > max_age:
            reason = (
                f"oldest in-flight connector call has run "
                f"{snap['oldest_in_flight_seconds']:.0f}s (limit {max_age:g}s)"
            )
        return {"ok": not reason, "reason": reason, **snap}


_executor: ConnectorExecutor | None = None
_executor_lock = threading.Lock()


def get_executor() -> ConnectorExecutor:
    global _executor
    with _executor_lock:
        if _executor is None:
            _executor = ConnectorExecutor()
        return _executor


def snapshot() -> dict[str, Any]:
    return get_executor().snapshot()


def readiness() -> dict[str, Any]:
    return get_executor().readiness()


def reset_for_tests() -> None:
    """Drop the executor so the next call re-reads the environment."""
    global _executor
    with _executor_lock:
        _executor = None
