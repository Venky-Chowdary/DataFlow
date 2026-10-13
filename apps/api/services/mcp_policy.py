"""Shared organization-wide policy for MCP tool access."""

from __future__ import annotations

from services.integrations_store import get_mcp_policy


def policy_denial(tool_name: str | None) -> str | None:
    policy = get_mcp_policy()
    if not policy["enabled"]:
        return "MCP is disabled by an administrator"
    allowed = policy.get("allowed_tools")
    if tool_name and allowed is not None and tool_name not in allowed:
        return f"MCP tool is not allowed by administrator: {tool_name}"
    return None


def filter_tools(tools: list[dict]) -> list[dict]:
    allowed = get_mcp_policy().get("allowed_tools")
    if allowed is None:
        return tools
    allowed_names = set(allowed)
    return [tool for tool in tools if tool.get("name") in allowed_names]
