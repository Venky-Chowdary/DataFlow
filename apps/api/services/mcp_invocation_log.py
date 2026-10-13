"""Persisted MCP tool invocation log — latency, status, and safe error type."""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from services.audit_log import append_audit_event
from services.ops_events import emit_event
from services.platform_config import data_dir
from services.value_serializer import json_default

STORE_PATH = data_dir() / "mcp_invocations.jsonl"
MAX_ROWS = int(__import__("os").getenv("DATAFLOW_MCP_LOG_MAX", "2000"))
logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_mcp_invocation(
    *,
    tool: str,
    client: str = "unknown",
    arguments: dict[str, Any] | None = None,
    status: str = "ok",
    error: str | BaseException | None = None,
    duration_ms: float = 0.0,
    correlation_id: str | None = None,
    actor: str = "mcp-agent",
    error_kind: str | None = None,
    error_type: str | None = None,
    event_error: BaseException | None = None,
) -> dict[str, Any]:
    error_text = str(error) if error is not None else ""
    if error_kind is None:
        if status == "ok":
            error_kind = "ok"
        else:
            from src.ai.copilot.tool_permissions import is_permission_denial

            if is_permission_denial(error_text):
                error_kind = "permission_denied"
            else:
                error_kind = "tool_error"
    if error_type is None and status != "ok":
        error_type = (
            type(event_error).__name__
            if event_error is not None
            else type(error).__name__
            if isinstance(error, BaseException)
            else "ToolError"
        )
    event_error = (
        event_error
        if event_error is not None
        else error
        if isinstance(error, BaseException)
        else RuntimeError()
        if status != "ok"
        else None
    )
    if status == "ok":
        emit_event(
            logger,
            "mcp.tool.call",
            outcome="succeeded",
            tool=tool,
            client=client,
            duration_ms=duration_ms,
            correlation_id=correlation_id,
        )
    elif error_kind == "permission_denied":
        emit_event(
            logger,
            "mcp.tool.call",
            outcome="failed",
            reason_code="permission_denied",
            tool=tool,
            client=client,
            duration_ms=duration_ms,
            correlation_id=correlation_id,
            error=event_error,
        )
    else:
        emit_event(
            logger,
            "mcp.tool.call",
            outcome="errored",
            tool=tool,
            client=client,
            duration_ms=duration_ms,
            correlation_id=correlation_id,
            error=event_error,
        )
    row = {
        "id": str(uuid.uuid4()),
        "time": _now(),
        "tool": tool,
        "client": client,
        "actor": actor,
        "status": status,
        "error_type": error_type,
        "ms": round(duration_ms, 1),
        "correlation_id": correlation_id,
        "error_kind": error_kind,
    }
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STORE_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False, default=json_default) + "\n")
    _trim_if_needed()
    append_audit_event(
        action=f"mcp.{tool}",
        resource=tool,
        actor=actor,
        level="error" if status != "ok" else "info",
        correlation_id=correlation_id,
        details={"client": client, "ms": row["ms"], "status": status},
    )
    return row


def list_mcp_invocations(*, limit: int = 50) -> list[dict[str, Any]]:
    if not STORE_PATH.exists():
        return []
    lines = STORE_PATH.read_text(encoding="utf-8").strip().splitlines()
    rows: list[dict[str, Any]] = []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        rows.append({
            "id": row.get("id"),
            "time": row.get("time"),
            "tool": row.get("tool"),
            "client": row.get("client"),
            "actor": row.get("actor"),
            "status": row.get("status"),
            "error_type": row.get("error_type"),
            "ms": row.get("ms"),
            "correlation_id": row.get("correlation_id"),
            "error_kind": row.get("error_kind"),
        })
        if len(rows) >= limit:
            break
    return rows


def _trim_if_needed() -> None:
    if not STORE_PATH.exists():
        return
    lines = STORE_PATH.read_text(encoding="utf-8").splitlines()
    if len(lines) <= MAX_ROWS:
        return
    STORE_PATH.write_text("\n".join(lines[-MAX_ROWS:]) + "\n", encoding="utf-8")


class McpInvocationTimer:
    """Context manager for timed MCP tool execution."""

    def __init__(
        self,
        tool: str,
        *,
        client: str = "unknown",
        arguments: dict | None = None,
        correlation_id: str | None = None,
    ):
        self.tool = tool
        self.client = client
        self.arguments = arguments or {}
        self.correlation_id = correlation_id
        self._start = 0.0

    def __enter__(self) -> McpInvocationTimer:
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        ms = (time.perf_counter() - self._start) * 1000
        if exc_type is not None:
            from src.ai.copilot.tool_permissions import is_permission_denial

            error = str(exc)[:500]
            log_mcp_invocation(
                tool=self.tool,
                client=self.client,
                arguments=self.arguments,
                status="error",
                error=error,
                duration_ms=ms,
                correlation_id=self.correlation_id,
                error_kind=(
                    "permission_denied" if is_permission_denial(error) else "tool_error"
                ),
                error_type=type(exc).__name__,
                event_error=exc,
            )
        else:
            log_mcp_invocation(
                tool=self.tool,
                client=self.client,
                arguments=self.arguments,
                status="ok",
                duration_ms=ms,
                correlation_id=self.correlation_id,
                error_kind="ok",
            )
