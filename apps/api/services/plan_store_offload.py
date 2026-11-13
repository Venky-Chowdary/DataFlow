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
