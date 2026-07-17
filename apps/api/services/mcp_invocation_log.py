"""Persisted MCP tool invocation log — latency, status, redacted args."""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from services.audit_log import _redact, append_audit_event
from services.platform_config import data_dir

STORE_PATH = data_dir() / "mcp_invocations.jsonl"
MAX_ROWS = int(__import__("os").getenv("DATAFLOW_MCP_LOG_MAX", "2000"))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_mcp_invocation(
    *,
    tool: str,
    client: str = "unknown",
    arguments: dict[str, Any] | None = None,
    status: str = "ok",
    error: str | None = None,
    duration_ms: float = 0.0,
    correlation_id: str | None = None,
    actor: str = "mcp-agent",
) -> dict[str, Any]:
    row = {
        "id": str(uuid.uuid4()),
        "time": _now(),
        "tool": tool,
        "client": client,
        "status": status,
        "error": error,
        "ms": round(duration_ms, 1),
        "arguments": _redact(arguments or {}),
        "correlation_id": correlation_id,
    }
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STORE_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")
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
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
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
            log_mcp_invocation(
                tool=self.tool,
                client=self.client,
                arguments=self.arguments,
                status="error",
                error=str(exc)[:500],
                duration_ms=ms,
                correlation_id=self.correlation_id,
            )
        else:
            log_mcp_invocation(
                tool=self.tool,
                client=self.client,
                arguments=self.arguments,
                status="ok",
                duration_ms=ms,
                correlation_id=self.correlation_id,
            )
