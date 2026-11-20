"""Bounded thread lanes for calls that wait on a customer's system (D321).

``asyncio.to_thread`` runs on the event loop's *default* executor, which has
``min(32, cpus + 4)`` threads and is shared by every ``/api/v1`` route that
offloads a store read. A connector test or an MCP tool call held one of those
threads for its whole deadline (15-45 s per call, minutes for a plan through a
dead tunnel). A handful of them in flight starved the metadata routes: with 16
connector tests against a host that accepts and never answers, ``GET
/api/v1/jobs`` took 28.5 s and the tests themselves queued past their own
15 s budget (measured on this branch, 4 CPUs, 8 default threads), while
``/health`` (no thread) still answered.

Each lane is its own bounded pool, so waiting on external systems can only
queue behind other waits of the same kind, never in front of the short routes.

Settings (``DATAWRAP_*`` is read before ``DATAFLOW_*``):

``DATAFLOW_LANE_CONNECTOR_THREADS``  connector calls awaited by async routes
                                     (default 48: the connector executor's
                                     workers plus its replacement workers,
                                     with room for fast capacity refusals)
``DATAFLOW_LANE_MCP_TOOL_THREADS``   MCP tool executions (default 32)
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import functools
import threading
from collections.abc import Callable
from typing import Any

from services.brand_env import getenv_brand

_DEFAULTS = {"connector": 48, "mcp_tool": 32}
_LOCK = threading.Lock()
_POOLS: dict[str, concurrent.futures.ThreadPoolExecutor] = {}
_BUSY: dict[str, int] = {}


def lane_size(name: str) -> int:
    raw = str(getenv_brand(f"DATAFLOW_LANE_{name.upper()}_THREADS", "") or "").strip()
    try:
        value = int(float(raw)) if raw else _DEFAULTS.get(name, 16)
    except ValueError:
        value = _DEFAULTS.get(name, 16)
    return value if value > 0 else _DEFAULTS.get(name, 16)


def _pool(name: str) -> concurrent.futures.ThreadPoolExecutor:
    with _LOCK:
        pool = _POOLS.get(name)
        if pool is None:
            pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=lane_size(name), thread_name_prefix=f"lane-{name}"
            )
            _POOLS[name] = pool
        return pool


def _counted(name: str, fn: Callable[[], Any]) -> Any:
    with _LOCK:
        _BUSY[name] = _BUSY.get(name, 0) + 1
    try:
        return fn()
    finally:
        with _LOCK:
            _BUSY[name] = _BUSY.get(name, 1) - 1


async def run_in_lane(name: str, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
    """``asyncio.to_thread`` on lane ``name`` (context variables carried over)."""
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    call = functools.partial(ctx.run, fn, *args, **kwargs)
    return await loop.run_in_executor(_pool(name), _counted, name, call)


def snapshot() -> dict[str, dict[str, int]]:
    """Busy and maximum threads per lane, for ``/health``."""
    with _LOCK:
        return {
            name: {"busy": _BUSY.get(name, 0), "max": lane_size(name)}
            for name in sorted(set(_DEFAULTS) | set(_POOLS))
        }
