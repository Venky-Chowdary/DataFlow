"""MCP Server — expose Data Pilot tools to Cursor, Claude, VS Code, and external agents."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Optional

router = APIRouter(prefix="/mcp", tags=["MCP Server"])


class ToolCallRequest(BaseModel):
    name: str
    arguments: dict = Field(default_factory=dict)


@router.get("/manifest")
async def mcp_manifest():
    """MCP-compatible manifest for IDE and agent integrations."""
    from ..ai.copilot.tools import TOOL_DEFINITIONS

    base = "http://localhost:8001/api/v1/mcp"
    return {
        "name": "datatransfer",
        "title": "DataTransfer.space MCP Server",
        "version": "1.0.0",
        "description": "Universal data movement — analyze, transfer, and query any dataset via AI agents.",
        "protocol": "rest-bridge",
        "endpoints": {
            "manifest": f"{base}/manifest",
            "tools": f"{base}/tools",
            "call": f"{base}/tools/call",
            "status": f"{base}/status",
        },
        "tools": TOOL_DEFINITIONS,
        "integrations": [
            {"id": "cursor", "label": "Cursor", "install_hint": "Add MCP URL in Cursor Settings → MCP"},
            {"id": "claude", "label": "Claude Desktop", "install_hint": "Add server URL to claude_desktop_config.json"},
            {"id": "vscode", "label": "VS Code", "install_hint": "Use MCP extension with server URL"},
            {"id": "chatgpt", "label": "ChatGPT", "install_hint": "Custom GPT action pointing to /mcp/tools/call"},
        ],
        "capabilities": [
            "list_datasets",
            "analyze_dataset",
            "search_data",
            "list_connectors",
            "list_jobs",
            "universal_transfer",
            "navigate_app",
        ],
    }


@router.get("/tools")
async def list_mcp_tools():
    from ..ai.copilot.tools import TOOL_DEFINITIONS
    return {"tools": TOOL_DEFINITIONS}


@router.post("/tools/call")
async def call_mcp_tool(request: ToolCallRequest):
    """Execute a Data Pilot tool — same surface external agents use."""
    from ..ai.copilot.tools import get_pilot_tools

    result = get_pilot_tools().execute(request.name, request.arguments)
    if not result.success:
        raise HTTPException(status_code=422, detail={"error": result.error, "tool": request.name})
    return {"tool": result.name, "success": True, "output": result.output}


@router.get("/status")
async def mcp_status():
    from ..ai.copilot.pilot_agent import get_pilot_agent
    from ..services.mongodb_service import get_mongodb_service

    pilot = get_pilot_agent()
    mongo = get_mongodb_service()
    return {
        "status": "online",
        "agent_mode": "anthropic_tools" if pilot.anthropic.is_available() else "local_tools",
        "datasets_indexed": len(pilot.analyst.list_datasets()),
        "connectors": len(mongo.list_connectors()),
        "jobs": len(mongo.list_jobs(limit=100)),
    }
