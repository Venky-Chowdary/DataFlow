"""
Datawrap — Training Agent runtime: a dedicated daemon thread and a stop signal.

Training is long and CPU-bound. Run on the event loop's default executor
(``asyncio.to_thread``), shutdown cannot stop it: cancelling the awaiting task
leaves the thread running, and ``asyncio.Runner.close()`` then blocks in
``shutdown_default_executor()`` for up to 300 s before abandoning a thread that
keeps holding the GIL after the app has stopped.

Here every run gets its own daemon thread and shares one ``threading.Event``.
Shutdown sets the event, cancels the scheduling tasks and moves on after a
short bounded wait; the run checks the event between phases and inside its
long loops and returns ``status="stopped"`` without writing partial output.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any, Callable, Coroutine

logger = logging.getLogger("datatransfer.training_agent")

# Longest shutdown waits for a stopped run to leave its thread. Never more:
# a daemon thread that misses it dies with the process.
SHUTDOWN_JOIN_SECONDS = 1.0


class TrainingStopped(Exception):
    """Raised inside a training run once a stop was requested."""


def raise_if_stopped(stop_event: threading.Event | None) -> None:
    """Checkpoint for a training run: unwind if shutdown asked it to stop."""
    if stop_event is not None and stop_event.is_set():
        raise TrainingStopped("training stop requested")


class TrainingRuntime:
    """Owns the training tasks and threads of one app lifespan."""

    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self._tasks: list[asyncio.Task] = []
        self._threads: list[threading.Thread] = []
        self._lock = threading.Lock()

    def start_task(self, coro: Coroutine[Any, Any, Any], *, name: str) -> asyncio.Task:
        """Schedule a training coroutine; shutdown cancels it."""
        task = asyncio.create_task(coro, name=name)
        self._tasks = [t for t in self._tasks if not t.done()]
        self._tasks.append(task)
        return task

    async def run(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run ``fn`` on a dedicated daemon thread and await its result.

        Unlike ``asyncio.to_thread`` this never uses the loop's default
        executor, so closing the loop never waits for it.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()

        def _settle(setter: Callable[[Any], None], value: Any) -> None:
            if not future.done():  # the awaiting task may have been cancelled
                setter(value)

        def _post(setter: Callable[[Any], None], value: Any) -> None:
            try:
                loop.call_soon_threadsafe(_settle, setter, value)
            except RuntimeError:
                # Loop already closed: the app shut down and nobody awaits this run.
                logger.debug("Training run finished after its event loop closed")

        def _target() -> None:
            try:
                result = fn(*args, **kwargs)
            except BaseException as exc:  # noqa: BLE001 - forwarded to the awaiting task
                _post(future.set_exception, exc)
            else:
                _post(future.set_result, result)
            finally:
                with self._lock:
                    if threading.current_thread() in self._threads:
                        self._threads.remove(threading.current_thread())

        thread = threading.Thread(target=_target, name="datawrap-training", daemon=True)
        with self._lock:
            self._threads.append(thread)
        thread.start()
        return await future

    async def shutdown(self, join_seconds: float = SHUTDOWN_JOIN_SECONDS) -> None:
        """Ask every run to stop, cancel the tasks, wait at most ``join_seconds``."""
        self.stop_event.set()
        tasks = [t for t in self._tasks if not t.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        deadline = time.monotonic() + join_seconds
        while time.monotonic() < deadline:
            with self._lock:
                alive = [t for t in self._threads if t.is_alive()]
            if not alive:
                return
            await asyncio.sleep(0.02)
        logger.warning(
            "Training run still stopping after %.1fs; leaving its daemon thread", join_seconds,
        )


def app_training_runtime(app: Any) -> TrainingRuntime:
    """The app's ``TrainingRuntime``, which its lifespan shuts down.

    The ``src.main`` lifespan always creates one. An app without that
    lifespan (a bare router in a test or probe) gets one on first use; its
    threads are daemons, so even unstopped they never block process exit.
    """
    runtime = getattr(app.state, "training_runtime", None)
    if runtime is None:
        runtime = TrainingRuntime()
        app.state.training_runtime = runtime
    return runtime
