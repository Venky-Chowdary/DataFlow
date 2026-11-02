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
    __slots__ = ("workspace", "tool", "submitted", "started", "abandoned", "child")

    def __init__(self, workspace: str, tool: str, *, child: bool = False) -> None:
        self.workspace = workspace
        self.tool = tool
        self.submitted = time.monotonic()
        self.started: float | None = None
        self.abandoned = False
        #: A probe run in parallel for an admitted call: not charged to the
        #: workspace cap a second time.
        self.child = child


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
    ) -> Any:
        """Run ``fn`` on the pool; return its value or raise at the deadline."""
        key = workspace or _NO_WORKSPACE
        call = _Call(key, tool)
        with self._lock:
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
            with self._lock:
                if future.cancel():
                    # Never started: nothing is stuck, give the slot back.
                    self._calls.discard(call)
                elif call in self._calls:
                    call.abandoned = True
                    self._abandoned_total += 1
                    self._resize_locked()
            message = describe_timeout()
            logger.warning(
                "connector call %s abandoned after %.0fs (workspace %s): %s",
                tool,
                seconds,
                key,
                message,
            )
            raise ConnectorCallTimeout(message) from None

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

    def spawn(self, tool: str, fn: Callable[[], Any]) -> concurrent.futures.Future:
        """Start ``fn`` for an already admitted call (a parallel probe).

        It runs in a copy of the caller's context, is counted in the pool
        numbers, and is not charged to the workspace cap again. The caller
        bounds the wait and may ``cancel()`` it to run it inline instead.
        """
        call = _Call(_NO_WORKSPACE, tool, child=True)
        with self._lock:
            self._calls.add(call)
        future = self._pool.submit(self._work(call, contextvars.copy_context(), fn))

        def _drop_if_cancelled(f: concurrent.futures.Future) -> None:
            if f.cancelled():
                with self._lock:
                    self._calls.discard(call)

        future.add_done_callback(_drop_if_cancelled)
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
            stuck = sum(1 for c in running if c.abandoned)
            return {
                "max_workers": self.max_workers,
                "workspace_max": self.workspace_max,
                "in_flight": len(running),
                "queued": queued,
                "abandoned": stuck,
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
        if live >= snap["max_workers"] or snap["abandoned"] >= snap["max_workers"]:
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
