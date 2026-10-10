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
from dataclasses import dataclass
from typing import Any

from services.value_serializer import json_default

logger = logging.getLogger(__name__)

SUPPORTED_PROTOCOL_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")
PROTOCOL_VERSION = SUPPORTED_PROTOCOL_VERSIONS[0]
SERVER_INFO = {
    "name": "dataflow",
    "title": "Datawrap MCP Server",
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
    from src.ai.copilot.tool_permissions import PLAN, READ, tool_requirement

    tools: list[dict[str, Any]] = []
    for tool in TOOL_DEFINITIONS:
        schema = tool.get("input_schema") or tool.get("inputSchema") or {"type": "object", "properties": {}}
        name = tool["name"]
        effect = tool_requirement(name)[1]
        read_only = effect in (READ, PLAN) and name != "confirm_action"
        tools.append(
            {
                "name": name,
                "description": tool.get("description") or "",
                "inputSchema": schema,
                "annotations": {
                    "readOnlyHint": read_only,
                    "destructiveHint": not read_only,
                },
            }
        )
    return tools


@dataclass(frozen=True)
class McpCallContext:
    actor: str
    client: str
    correlation_id: str | None


def _execute_tool(
    name: str,
    arguments: dict[str, Any] | None,
    context: McpCallContext,
) -> dict[str, Any]:
    from services.mcp_invocation_log import log_mcp_invocation
    from services.secret_config import mask_secrets_in_text
    from src.ai.copilot.tool_permissions import is_permission_denial
    from src.ai.copilot.tools import get_pilot_tools

    start = time.perf_counter()
    try:
        result = get_pilot_tools().execute(name, arguments or {})
    except Exception as exc:
        logger.exception("MCP tool %s raised", name)
        masked = mask_secrets_in_text(str(exc))
        log_mcp_invocation(
            tool=name,
            client=context.client,
            arguments=arguments or {},
            status="error",
            error=masked,
            duration_ms=(time.perf_counter() - start) * 1000,
            actor=context.actor,
            correlation_id=context.correlation_id,
            error_kind="tool_error",
        )
        return {
            "content": [
                {
                    "type": "text",
                    "text": f"Tool error: {type(exc).__name__}: {masked}",
                }
            ],
            "isError": True,
        }

    ms = (time.perf_counter() - start) * 1000
    if not result.success:
        error = mask_secrets_in_text(str(result.error or "tool failed"))
        log_mcp_invocation(
            tool=name,
            client=context.client,
            arguments=arguments or {},
            status="error",
            error=error,
            duration_ms=ms,
            actor=context.actor,
            correlation_id=context.correlation_id,
            error_kind="permission_denied" if is_permission_denial(error) else "tool_error",
        )
        return {
            "content": [{"type": "text", "text": error}],
            "isError": True,
        }

    log_mcp_invocation(
        tool=name,
        client=context.client,
        arguments=arguments or {},
        status="ok",
        duration_ms=ms,
        actor=context.actor,
        correlation_id=context.correlation_id,
        error_kind="ok",
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
    context: McpCallContext | None = None,
) -> dict[str, Any] | None:
    """Handle one JSON-RPC request/notification. Returns None for notifications."""
    context = context or McpCallContext(
        actor="mcp-streamable",
        client="mcp-streamable",
        correlation_id=None,
    )
    if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
        return _jsonrpc_error(message.get("id") if isinstance(message, dict) else None, -32600, "Invalid Request")

    method = message.get("method")
    req_id = message.get("id")
    params = message.get("params") or {}
    is_notification = "id" not in message

    if method == "initialize":
        requested_version = (
            params.get("protocolVersion") if isinstance(params, dict) else None
        )
        protocol_version = (
            requested_version
            if requested_version in SUPPORTED_PROTOCOL_VERSIONS
            else PROTOCOL_VERSION
        )
        return _jsonrpc_result(
            req_id,
            {
                "protocolVersion": protocol_version,
                "capabilities": {
                    "tools": {"listChanged": False},
                },
                "serverInfo": SERVER_INFO,
                "instructions": (
                    "Datawrap universal data-movement tools. "
                    "Use Authorization: Bearer <workspace API key or JWT> for tools/call. "
                    "create_connector, start_transfer, start_dataset_transfer, "
                    "create_schedule, and lifecycle changes return an ack_id and do not "
                    "mutate until confirm_action."
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
            from services.mcp_invocation_log import log_mcp_invocation

            log_mcp_invocation(
                tool=str((params or {}).get("name") or "unknown"),
                status="error",
                error="Authentication required",
                actor=context.actor,
                client=context.client,
                correlation_id=context.correlation_id,
                error_kind="auth",
            )
            return _jsonrpc_error(
                req_id,
                -32001,
                "Authentication required",
                {"hint": "Pass Authorization: Bearer <token> in MCP headers"},
            )
        from services.mcp_rate_limit import check_mcp_rate_limit

        limit = check_mcp_rate_limit(context.actor)
        if not limit.get("allowed"):
            from services.mcp_invocation_log import log_mcp_invocation

            log_mcp_invocation(
                tool=str((params or {}).get("name") or "unknown"),
                status="error",
                error="MCP rate limit exceeded",
                actor=context.actor,
                client=context.client,
                correlation_id=context.correlation_id,
                error_kind="rate_limited",
            )
            return _jsonrpc_error(
                req_id,
                -32029,
                "MCP rate limit exceeded",
                {"retry_after_sec": limit.get("retry_after_sec")},
            )
        name = params.get("name") if isinstance(params, dict) else None
        arguments = params.get("arguments") if isinstance(params, dict) else {}
        if not name or not isinstance(name, str):
            return _jsonrpc_error(req_id, -32602, "Invalid params: name required")
        if not isinstance(arguments, dict):
            arguments = {}
        return _jsonrpc_result(req_id, _execute_tool(name, arguments, context))

    if is_notification:
        return None

    return _jsonrpc_error(req_id, -32601, f"Method not found: {method}")


def new_session_id() -> str:
    return str(uuid.uuid4())
