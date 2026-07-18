"""MCP Server — expose Data Pilot tools to Cursor, Claude, VS Code, and external agents.

Supports:
  - Native Streamable HTTP at ``POST/GET /api/v1/mcp`` (Cursor ``url`` config)
  - Legacy REST bridge at ``/manifest``, ``/tools``, ``/tools/call``
"""

import json
import time

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

router = APIRouter(prefix="/mcp", tags=["MCP Server"])


class ToolCallRequest(BaseModel):
    name: str
    arguments: dict = Field(default_factory=dict)


def _mcp_authenticated(http_request: Request) -> bool:
    return bool(getattr(http_request.state, "user", None) or getattr(http_request.state, "api_key_auth", False))


@router.api_route("", methods=["GET", "POST", "DELETE"], include_in_schema=True)
@router.api_route("/", methods=["GET", "POST", "DELETE"], include_in_schema=False)
async def mcp_streamable(http_request: Request):
    """Cursor-native MCP Streamable HTTP endpoint."""
    from services.mcp_protocol import handle_jsonrpc, new_session_id

    if http_request.method == "DELETE":
        return Response(status_code=204)

    if http_request.method == "GET":
        # Optional SSE stream — keep-alive so clients that open GET succeed.
        async def _sse():
            yield ": dataflow-mcp ready\n\n"

        return StreamingResponse(
            _sse(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )

    try:
        payload = await http_request.json()
    except Exception as exc:
        return JSONResponse(
            status_code=400,
            content={"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": f"Parse error: {exc}"}},
        )

    from src.services.auth_service import auth_required

    authenticated = _mcp_authenticated(http_request)
    # When platform auth is off (local/dev), tools are callable without a Bearer token.
    allow_unauth_tools = not auth_required()
    session_id = http_request.headers.get("mcp-session-id") or new_session_id()

    messages = payload if isinstance(payload, list) else [payload]
    results: list[dict] = []
    for message in messages:
        if not isinstance(message, dict):
            results.append({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"}})
            continue
        out = handle_jsonrpc(
            message,
            authenticated=authenticated,
            allow_unauth_tools=allow_unauth_tools,
        )
        if out is not None:
            results.append(out)

    headers = {"Mcp-Session-Id": session_id}

    # Notifications-only → 202 Accepted with empty body
    if not results:
        return Response(status_code=202, headers=headers)

    body = results if isinstance(payload, list) else results[0]
    accept = http_request.headers.get("accept", "")
    if "text/event-stream" in accept and "application/json" not in accept:
        data = json.dumps(body, default=str)
        return StreamingResponse(
            iter([f"event: message\ndata: {data}\n\n"]),
            media_type="text/event-stream",
            headers=headers,
        )
    return JSONResponse(content=body, headers=headers)


@router.get("/manifest")
async def mcp_manifest(http_request: Request):
    """MCP-compatible manifest for IDE and agent integrations."""
    from ..ai.copilot.tools import TOOL_DEFINITIONS

    base = f"{str(http_request.base_url).rstrip('/')}/api/v1/mcp"
    return {
        "name": "dataflow",
        "title": "DataFlow MCP Server",
        "version": "2.0.0",
        "description": "Universal data movement — analyze, transfer, and query any dataset via AI agents.",
        "protocol": "streamable-http",
        "legacy_protocol": "rest-bridge",
        "endpoints": {
            "mcp": base,
            "manifest": f"{base}/manifest",
            "tools": f"{base}/tools",
            "call": f"{base}/tools/call",
            "status": f"{base}/status",
        },
        "tools": TOOL_DEFINITIONS,
        "integrations": [
            {
                "id": "cursor",
                "label": "Cursor",
                "install_hint": (
                    'Add to mcp.json: {"url": "' + base + '", '
                    '"headers": {"Authorization": "Bearer <workspace-api-key>"}}'
                ),
            },
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
    from services.mcp_invocation_log import log_mcp_invocation

    from ..ai.copilot.tools import get_pilot_tools

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
async def mcp_status(http_request: Request):
    from services.connector_store import list_connectors
    from services.mcp_invocation_log import list_mcp_invocations

    from ..ai.copilot.pilot_agent import get_pilot_agent
    from ..ai.copilot.tools import TOOL_DEFINITIONS

    try:
        pilot = get_pilot_agent()
        anthropic_available = pilot.anthropic.is_available()
        datasets = len(pilot.analyst.list_datasets())
    except Exception:
        pilot = None
        anthropic_available = False
        datasets = 0

    try:
        jobs_service = None
        try:
            from ..services.mongodb_service import get_mongodb_service
            jobs_service = get_mongodb_service()
        except Exception:
            pass
        if jobs_service is None:
            from services.jobs import job_store
            jobs_service = job_store
        jobs = len(jobs_service.list_jobs(limit=100))
    except Exception:
        try:
            from services.jobs import job_store
            jobs = len(job_store.list_recent(limit=100))
        except Exception:
            jobs = 0

    try:
        connectors = len(list_connectors())
    except Exception:
        connectors = 0

    recent = list_mcp_invocations(limit=1)
    return {
        "status": "online",
        "agent_mode": "anthropic_tools" if anthropic_available else "local_tools",
        "datasets_indexed": datasets,
        "connectors": connectors,
        "jobs": jobs,
        "tools_registered": len(TOOL_DEFINITIONS),
        "last_invocation_ms": recent[0]["ms"] if recent else None,
        "manifest_url": f"{str(http_request.base_url).rstrip('/')}/api/v1/mcp/manifest",
    }
