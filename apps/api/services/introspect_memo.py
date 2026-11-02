"""Share one catalog read between the steps of a single read-only plan.

``plan_transfer`` introspects the same source and destination table from
several steps (Map, destination probe, FK/CHECK/index catalogs, preflight,
sample resolution) — seven separate ``introspect_schema`` calls for one
PostgreSQL pair. On PostgreSQL that costs milliseconds each; on Oracle every
call walks ``ALL_*`` dictionary views for columns, constraints, indexes and
partitions, so the plan outlasted the MCP client's request timeout and
``start_transfer`` died with JSON-RPC -32001 (D145).

A plan never changes a catalog, so within the scope the identical request can
share its answer. Outside a scope nothing is memoised: Execute and writers
always read the live catalog.
"""

from __future__ import annotations

import contextlib
import contextvars
import copy
from collections.abc import Callable, Iterator
from typing import Any

_memo: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "introspect_memo", default=None
)


@contextlib.contextmanager
def introspect_memo_scope() -> Iterator[None]:
    """Memoise catalog reads for the duration of one read-only plan."""
    if _memo.get() is not None:
        yield
        return
    token = _memo.set({})
    try:
        yield
    finally:
        _memo.reset(token)


def memoized_introspection(
    db_type: str, kwargs: dict[str, Any], read: Callable[[], dict[str, Any]]
) -> dict[str, Any]:
    """``read()`` once per identical request inside a scope; a copy each time."""
    memo = _memo.get()
    if memo is None:
        return read()
    key = repr((db_type, sorted((str(k), repr(v)) for k, v in kwargs.items())))
    if key not in memo:
        memo[key] = read()
    return copy.deepcopy(memo[key])
