"""Build the route table before the first request needs it.

FastAPI materialises the routes of an included router lazily: the first
request that has to look past the included routers builds the dependency
model of every route in them. That is CPU work on the event loop — about half
a second here, over a second on a loaded host — and in production the request
that pays it is the first health probe after a start (D321 follow-up: a
``/api/v1/health`` that took 1.1s while every later one took 4ms).

Matching a path no route serves walks the whole table once, so the work
happens at startup instead. It uses only the public ``route.matches`` call;
on a FastAPI that builds routes eagerly it is a cheap no-op.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

#: Paths that match no route: one under the API prefix, one at the root (some
#: routers are also included without a prefix).
_PROBE_PATHS = ("/api/v1/__route_table_warmup__", "/__route_table_warmup__")


def warm_route_table(app: Any) -> float:
    """Walk every route once. Returns the seconds it took (0.0 on failure)."""
    started = time.perf_counter()
    try:
        for path in _PROBE_PATHS:
            for method in ("GET", "POST"):
                scope = {
                    "type": "http",
                    "method": method,
                    "path": path,
                    "root_path": "",
                    "headers": [],
                    "query_string": b"",
                    "path_params": {},
                    "app": app,
                }
                for route in list(app.router.routes):
                    route.matches(scope)
    except Exception:  # noqa: BLE001 - a warm-up must never stop the app from starting
        logger.warning("route table warm-up failed; the first request will build it", exc_info=True)
        return 0.0
    return time.perf_counter() - started
