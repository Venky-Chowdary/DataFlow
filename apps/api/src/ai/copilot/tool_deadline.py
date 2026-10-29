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

import contextvars
import logging
import os
import threading
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
class _Progress:
    """What the running tool is waiting on; read by the deadline watcher."""

    step: str = ""
    role: str = ""
    target: str = ""
    #: ``(role, host:port)`` of every other system the same step also reaches.
    also: tuple[tuple[str, str], ...] = ()
    lock: threading.Lock = field(default_factory=threading.Lock)


_progress: contextvars.ContextVar[_Progress | None] = contextvars.ContextVar(
    "df_tool_progress", default=None
)


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
) -> None:
    """Record the step about to block on a customer system (cheap, thread-safe).

    A step that reaches several systems (the preflight reads the source and the
    destination) passes the others as ``also=((role, conn), ...)`` so a timeout
    names every system it may have been waiting on, not just the first.
    """
    progress = _progress.get()
    if progress is None:
        return
    with progress.lock:
        progress.step = step
        progress.role = role
        progress.target = describe_target(conn) if conn is not None else ""
        progress.also = tuple(
            (other_role, target)
            for other_role, other in also
            if (target := describe_target(other))
        )


def _timeout_message(tool: str, seconds: float, progress: _Progress) -> str:
    with progress.lock:
        step, role, target = progress.step, progress.role, progress.target
        also = progress.also
    waited = f"{seconds:g}s"
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
    where = f" at step: {step}" if step else ""
    return (
        f"{tool} did not finish within {waited}{where}. A system it reads from "
        "is unreachable or slow; nothing was staged."
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

    limit = deadline_seconds(tool) if seconds is None else seconds
    progress = _Progress()

    def work() -> Any:
        # ``_progress`` is set on the worker's copy of the caller's context, so
        # only this call sees it.
        _progress.set(progress)
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
