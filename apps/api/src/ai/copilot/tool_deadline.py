"""A deadline for pilot/MCP tools that reach out to a customer's database (D169).

``start_transfer`` / ``plan_transfer`` introspect the source and destination and
run a preflight on the request path. Against a source that accepts the TCP
connection and never answers, each driver step waits out its own socket timeout
and the steps stack, so the MCP client gave up first with a bare
``-32001 Request timed out`` that names nothing and staged nothing.

``run_with_deadline`` bounds the whole call. When the deadline passes it returns
a failure that names the host:port and the step that was waiting. The abandoned
worker keeps running only until its driver's own (bounded) timeout fires; it
cannot hold a request, the event loop or a pool slot of the caller.
"""

from __future__ import annotations

import contextvars
import logging
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

#: Under the MCP client's own request timeout, so the caller gets our answer.
DEFAULT_DEADLINE_SECONDS = 45.0

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


def deadline_seconds() -> float:
    raw = os.environ.get("DATAFLOW_TOOL_DEADLINE_SECONDS", "")
    try:
        value = float(raw) if raw.strip() else DEFAULT_DEADLINE_SECONDS
    except ValueError:
        value = DEFAULT_DEADLINE_SECONDS
    return value if value > 0 else DEFAULT_DEADLINE_SECONDS


@dataclass
class _Progress:
    """What the running tool is waiting on; read by the deadline watcher."""

    step: str = ""
    role: str = ""
    target: str = ""
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


def note_step(step: str, conn: Any = None, *, role: str = "") -> None:
    """Record the step about to block on a customer system (cheap, thread-safe)."""
    progress = _progress.get()
    if progress is None:
        return
    with progress.lock:
        progress.step = step
        progress.role = role
        progress.target = describe_target(conn) if conn is not None else ""


def _timeout_message(tool: str, seconds: float, progress: _Progress) -> str:
    with progress.lock:
        step, role, target = progress.step, progress.role, progress.target
    waited = f"{seconds:g}s"
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
    """Run ``fn`` and return its value, or ``on_timeout(message)`` at the deadline."""
    limit = deadline_seconds() if seconds is None else seconds
    progress = _Progress()
    outcome: dict[str, Any] = {}
    done = threading.Event()
    ctx = contextvars.copy_context()

    def work() -> None:
        _progress.set(progress)
        try:
            outcome["value"] = fn()
        except BaseException as exc:  # noqa: BLE001 - re-raised in the caller
            outcome["error"] = exc
        finally:
            done.set()

    # ``ctx.run`` carries the caller's principal / tenant scope / MCP request
    # into the worker; ``_progress`` is set inside it so only this call sees it.
    thread = threading.Thread(
        target=lambda: ctx.run(work), name=f"tool-{tool}", daemon=True
    )
    thread.start()
    if not done.wait(limit):
        message = _timeout_message(tool, limit, progress)
        logger.warning("tool %s exceeded its %.0fs deadline: %s", tool, limit, message)
        return on_timeout(message)
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]
