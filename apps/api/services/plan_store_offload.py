"""Transfer-plan writes off the event loop, on a thread pool of their own.

A plan write holds the plan store's cross-process lock and can wait up to
``transfer_plan_store.LOCK_TIMEOUT_SECONDS`` (30 s) for another process to
release it. Run on the loop's default executor, a handful of such writers
parked on a held lock would use up the threads every other ``to_thread``
caller (and read-only plan routes) shares. :func:`run_plan_write` runs them on
:data:`PLAN_WRITE_WORKERS` dedicated threads instead: writers queue among
themselves, nothing else waits behind them. Reads take no lock and stay on
the default executor.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

T = TypeVar("T")

#: Set only inside the worker thread of :func:`compute_then_commit`.
_DEFER_PLAN_WRITES: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "defer_plan_writes", default=False
)

#: Plan writes serialize on one store lock, so more threads would only wait.
PLAN_WRITE_WORKERS = 4


@functools.lru_cache(maxsize=1)
def _pool() -> ThreadPoolExecutor:
    return ThreadPoolExecutor(max_workers=PLAN_WRITE_WORKERS, thread_name_prefix="plan-store-write")


async def run_plan_write(fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """Await ``fn(*args, **kwargs)`` on the plan-write pool, in a copy of this context.

    The copied context carries the request's workspace scope, as
    ``asyncio.to_thread`` would.
    """
    call = functools.partial(contextvars.copy_context().run, fn, *args, **kwargs)
    return await asyncio.get_running_loop().run_in_executor(_pool(), call)


def take_plan_write_deferral() -> bool:
    """Whether the caller asked this service call to return its plan writes.

    Read once at the top of a service function that supports it (Map, sync
    Validate): it returns ``commit()`` instead of writing. Cleared on read, so
    a nested service call in the same thread writes normally.
    """
    deferred = _DEFER_PLAN_WRITES.get()
    if deferred:
        _DEFER_PLAN_WRITES.set(False)
    return deferred


def _deferring(fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
    _DEFER_PLAN_WRITES.set(True)  # this worker's own copy of the context
    return fn(*args, **kwargs)


async def compute_then_commit(fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
    """Run a long plan operation's compute on the default executor, its write on the plan pool.

    ``fn`` runs via ``asyncio.to_thread`` with plan writes deferred: a service
    that supports deferral returns ``commit()`` (its plan writes, which can
    wait on the store lock), which then runs on the plan-write pool. A
    function that returns a plain result (e.g. a test double) is passed
    through.
    """
    outcome = await asyncio.to_thread(_deferring, fn, *args, **kwargs)
    if callable(outcome):
        return await run_plan_write(outcome)
    return outcome
