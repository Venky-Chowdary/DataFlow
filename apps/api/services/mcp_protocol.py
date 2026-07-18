"""Native MCP Streamable HTTP (JSON-RPC) — Cursor/Claude compatible transport.

The legacy REST bridge (`/mcp/manifest`, `/mcp/tools/call`) remains for custom
integrations. This module speaks the protocol Cursor expects when configured as:

    {"mcpServers": {"dataflow": {"url": "https://…/api/v1/mcp"}}}
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from services.value_serializer import json_default

logger = logging.getLogger(__name__)

PROTOCOL_VERSION = "2024-11-05"
SERVER_INFO = {
    "name": "dataflow",
    "title": "DataFlow MCP Server",
    "version": "2.0.0",
}


def _jsonrpc_result(req_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _jsonrpc_error(req_id: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
    err: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": err}


def _tool_descriptors() -> list[dict[str, Any]]:
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    tools: list[dict[str, Any]] = []
    for tool in TOOL_DEFINITIONS:
        schema = tool.get("input_schema") or tool.get("inputSchema") or {"type": "object", "properties": {}}
        tools.append(
            {
                "name": tool["name"],
                "description": tool.get("description") or "",
                "inputSchema": schema,
            }
        )
    return tools


def _execute_tool(name: str, arguments: dict[str, Any] | None) -> dict[str, Any]:
    from services.mcp_invocation_log import log_mcp_invocation
    from src.ai.copilot.tools import get_pilot_tools

    start = time.perf_counter()
    try:
        result = get_pilot_tools().execute(name, arguments or {})
    except Exception as exc:
        log_mcp_invocation(
            tool=name,
            client="mcp-streamable",
            arguments=arguments or {},
            status="error",
            error=str(exc),
            duration_ms=(time.perf_counter() - start) * 1000,
        )
        return {
            "content": [{"type": "text", "text": f"Tool error: {exc}"}],
            "isError": True,
        }

    ms = (time.perf_counter() - start) * 1000
    if not result.success:
        log_mcp_invocation(
            tool=name,
            client="mcp-streamable",
            arguments=arguments or {},
            status="error",
            error=result.error or "tool failed",
            duration_ms=ms,
        )
        return {
            "content": [{"type": "text", "text": result.error or "tool failed"}],
            "isError": True,
        }

    log_mcp_invocation(
        tool=name,
        client="mcp-streamable",
        arguments=arguments or {},
        status="ok",
        duration_ms=ms,
    )
    text = result.output
    if not isinstance(text, str):
        text = json.dumps(text, default=json_default, indent=2)
    return {
        "content": [{"type": "text", "text": text}],
        "isError": False,
    }


def handle_jsonrpc(
    message: dict[str, Any],
    *,
    authenticated: bool,
    allow_unauth_tools: bool = False,
) -> dict[str, Any] | None:
    """Handle one JSON-RPC request/notification. Returns None for notifications."""
    if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
        return _jsonrpc_error(message.get("id") if isinstance(message, dict) else None, -32600, "Invalid Request")

    method = message.get("method")
    req_id = message.get("id")
    params = message.get("params") or {}
    is_notification = "id" not in message

    if method == "initialize":
        return _jsonrpc_result(
            req_id,
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {
                    "tools": {"listChanged": False},
                },
                "serverInfo": SERVER_INFO,
                "instructions": (
                    "DataFlow universal data-movement tools. "
                    "Use Authorization: Bearer <workspace API key or JWT> for tools/call."
                ),
            },
        )

    if method == "notifications/initialized" or method == "initialized":
        return None

    if method == "ping":
        return _jsonrpc_result(req_id, {})

    if method == "tools/list":
        return _jsonrpc_result(req_id, {"tools": _tool_descriptors()})

    if method == "resources/list":
        return _jsonrpc_result(req_id, {"resources": []})

    if method == "prompts/list":
        return _jsonrpc_result(req_id, {"prompts": []})

    if method == "tools/call":
        if not authenticated and not allow_unauth_tools:
            return _jsonrpc_error(
                req_id,
                -32001,
                "Authentication required",
                {"hint": "Pass Authorization: Bearer <token> in MCP headers"},
            )
        name = params.get("name") if isinstance(params, dict) else None
        arguments = params.get("arguments") if isinstance(params, dict) else {}
        if not name or not isinstance(name, str):
            return _jsonrpc_error(req_id, -32602, "Invalid params: name required")
        if not isinstance(arguments, dict):
            arguments = {}
        return _jsonrpc_result(req_id, _execute_tool(name, arguments))

    if is_notification:
        return None

    return _jsonrpc_error(req_id, -32601, f"Method not found: {method}")


def new_session_id() -> str:
    return str(uuid.uuid4())
