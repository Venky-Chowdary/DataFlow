"""MCP Server — expose Data Pilot tools to Cursor, Claude, VS Code, and external agents."""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from typing import Any, Optional
import time

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
async def call_mcp_tool(request: ToolCallRequest, http_request: Request):
    """Execute a Data Pilot tool — same surface external agents use."""
    from ..ai.copilot.tools import get_pilot_tools
    from services.mcp_invocation_log import log_mcp_invocation

    client = http_request.headers.get("X-MCP-Client", "unknown")
    correlation_id = getattr(http_request.state, "correlation_id", None)
    start = time.perf_counter()
    try:
        result = get_pilot_tools().execute(request.name, request.arguments)
    except Exception as exc:
        log_mcp_invocation(
            tool=request.name,
            client=client,
            arguments=request.arguments,
            status="error",
            error=str(exc),
            duration_ms=(time.perf_counter() - start) * 1000,
            correlation_id=correlation_id,
        )
        raise HTTPException(status_code=500, detail={"error": str(exc), "tool": request.name}) from exc

    ms = (time.perf_counter() - start) * 1000
    if not result.success:
        log_mcp_invocation(
            tool=request.name,
            client=client,
            arguments=request.arguments,
            status="error",
            error=result.error or "tool failed",
            duration_ms=ms,
            correlation_id=correlation_id,
        )
        raise HTTPException(status_code=422, detail={"error": result.error, "tool": request.name})

    log_mcp_invocation(
        tool=request.name,
        client=client,
        arguments=request.arguments,
        status="ok",
        duration_ms=ms,
        correlation_id=correlation_id,
    )
    return {"tool": result.name, "success": True, "output": result.output}


@router.get("/logs")
async def mcp_request_logs(limit: int = 50):
    """Recent MCP tool invocations from persistent log."""
    from services.mcp_invocation_log import list_mcp_invocations

    rows = list_mcp_invocations(limit=min(limit, 200))
    return {"logs": rows, "count": len(rows)}


@router.get("/status")
async def mcp_status():
    from ..ai.copilot.pilot_agent import get_pilot_agent
    from services.connector_store import list_connectors
    from services.mcp_invocation_log import list_mcp_invocations
    from ..ai.copilot.tools import TOOL_DEFINITIONS

    pilot = get_pilot_agent()
    try:
        from ..services.mongodb_service import get_mongodb_service
        jobs = len(get_mongodb_service().list_jobs(limit=100))
    except Exception:
        jobs = 0

    recent = list_mcp_invocations(limit=1)
    online = True
    return {
        "status": "online" if online else "offline",
        "agent_mode": "anthropic_tools" if pilot.anthropic.is_available() else "local_tools",
        "datasets_indexed": len(pilot.analyst.list_datasets()),
        "connectors": len(list_connectors()),
        "jobs": jobs,
        "tools_registered": len(TOOL_DEFINITIONS),
        "last_invocation_ms": recent[0]["ms"] if recent else None,
    }
