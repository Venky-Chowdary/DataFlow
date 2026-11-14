"""Run a blocking route handler off the event loop (audit M-8).

The API runs one uvicorn worker. A handler declared ``async`` that calls the
synchronous job store (pymongo) directly holds the only event loop for the
whole round trip, so every other request — ``/health`` included — waits.

``off_event_loop`` keeps such a handler awaitable (other routers call these
handlers directly with ``await``) and keeps its signature for FastAPI, but
runs its body on the server's thread pool through Starlette's
``run_in_threadpool``, which carries the request's context variables along.
"""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

from starlette.concurrency import run_in_threadpool

P = ParamSpec("P")
R = TypeVar("R")


def off_event_loop(handler: Callable[P, R]) -> Callable[P, Awaitable[R]]:
    """An awaitable route handler whose synchronous body runs on the thread pool."""

    @functools.wraps(handler)
    async def offloaded(*args: P.args, **kwargs: P.kwargs) -> R:
        return await run_in_threadpool(handler, *args, **kwargs)

    return offloaded
