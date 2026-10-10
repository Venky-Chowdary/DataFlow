"""MCP Server — expose Datawrap Pilot tools to Cursor, Claude, VS Code, and external agents.

Supports:
  - Native Streamable HTTP at ``POST/GET /api/v1/mcp`` (Cursor ``url`` config)
  - Legacy REST bridge at ``/manifest``, ``/tools``, ``/tools/call``
"""

import asyncio
import json
import logging
import time

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

router = APIRouter(prefix="/mcp", tags=["MCP Server"])
oauth_resource_router = APIRouter(tags=["MCP OAuth"])
logger = logging.getLogger(__name__)


def _mcp_origin_allowed(http_request: Request, origin: str) -> bool:
    from services.cors_policy import TenantAwareCORSMiddleware

    middleware = http_request.app.middleware_stack
    while middleware is not None:
        if isinstance(middleware, TenantAwareCORSMiddleware):
            return middleware.is_allowed_origin(origin)
        middleware = getattr(middleware, "app", None)
    return False


class ToolCallRequest(BaseModel):
    name: str
    arguments: dict = Field(default_factory=dict)


class McpPolicyRequest(BaseModel):
    enabled: bool = True
    allowed_tools: list[str] | None = None


def _mcp_authenticated(http_request: Request) -> bool:
    return bool(getattr(http_request.state, "user", None) or getattr(http_request.state, "api_key_auth", False))


def _mcp_policy_denial(tool_name: str | None = None) -> str | None:
    from services.integrations_store import get_mcp_policy

    policy = get_mcp_policy()
    if not policy["enabled"]:
        return "MCP is disabled by an administrator"
    allowed = policy.get("allowed_tools")
    if tool_name and allowed is not None and tool_name not in allowed:
        return f"MCP tool is not allowed by administrator: {tool_name}"
    return None


def _require_mcp_tool_auth(http_request: Request, tool_name: str | None = None) -> None:
    """Refuse tool execution unless a Bearer JWT / workspace API key is present.

    When platform auth is off (local/dev), tools remain callable without a token
    so developers can exercise the MCP surface without spinning up users.
    """
    from src.services.auth_service import auth_required

    if not auth_required():
        return
    if _mcp_authenticated(http_request):
        return
    from services.mcp_oauth import bearer_challenge

    raise HTTPException(
        status_code=401,
        detail={
            "error": "Authentication required",
            "hint": "Pass Authorization: Bearer <workspace-api-key-or-jwt>",
        },
        headers={"WWW-Authenticate": bearer_challenge()},
    )


@router.api_route("", methods=["GET", "POST", "DELETE"], include_in_schema=True)
@router.api_route("/", methods=["GET", "POST", "DELETE"], include_in_schema=False)
async def mcp_streamable(http_request: Request):
    """Cursor-native MCP Streamable HTTP endpoint."""
    from services.mcp_protocol import McpCallContext, _jsonrpc_error, handle_jsonrpc, new_session_id

    origin = http_request.headers.get("origin")
    if origin is not None and not _mcp_origin_allowed(http_request, origin):
        return JSONResponse(
            status_code=403,
            content=_jsonrpc_error(None, -32600, "Origin not allowed"),
        )

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
    except Exception:
        logger.warning("MCP JSON-RPC parse error")
        return JSONResponse(
            status_code=400,
            content={"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}},
        )

    from src.ai.copilot.confirm_ack import reset_mcp_request, set_mcp_request
    from src.ai.copilot.tool_permissions import bind_request_principal
    from src.services.auth_service import auth_required

    authenticated = _mcp_authenticated(http_request)
    client = http_request.headers.get("X-MCP-Client") or "mcp-streamable"
    actor = (
        getattr(http_request.state, "user_email", None)
        or http_request.headers.get("X-MCP-Client")
        or "mcp-streamable"
    )
    context = McpCallContext(
        actor=str(actor),
        client=client,
        correlation_id=getattr(http_request.state, "correlation_id", None),
    )
    # When platform auth is off (local/dev), tools are callable without a Bearer token.
    allow_unauth_tools = not auth_required()
    mcp_role = ""
    if auth_required():
        mcp_user = getattr(http_request.state, "user", None) or {}
        mcp_role = str(mcp_user.get("role") or "viewer")
    session_id = http_request.headers.get("mcp-session-id") or new_session_id()

    messages = payload if isinstance(payload, list) else [payload]
    results: list[dict] = []
    request_token = set_mcp_request(http_request)
    try:
        with bind_request_principal(http_request, mcp_role):
            for message in messages:
                if not isinstance(message, dict):
                    results.append({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"}})
                    continue
                if message.get("method") == "tools/call":
                    params = message.get("params") or {}
                    denial = _mcp_policy_denial(params.get("name"))
                    if denial:
                        results.append(_jsonrpc_error(message.get("id"), -32003, denial))
                        continue
                # tools/call runs plan/preflight synchronously. Doing that on
                # the event loop made ping and tools/list wait out the client
                # timeout (JSON-RPC -32001 Request timed out). A thread keeps
                # the listener free. A process restart during deploy still
                # drops the in-flight call.
                out = await asyncio.to_thread(
                    handle_jsonrpc,
                    message,
                    authenticated=authenticated,
                    allow_unauth_tools=allow_unauth_tools,
                    context=context,
                )
                if out is not None:
                    if message.get("method") == "tools/list":
                        from services.integrations_store import get_mcp_policy

                        allowed = get_mcp_policy().get("allowed_tools")
                        if allowed is not None and isinstance(out.get("result"), dict):
                            out["result"]["tools"] = [
                                tool for tool in out["result"].get("tools", [])
                                if tool.get("name") in allowed
                            ]
                    results.append(out)
    finally:
        reset_mcp_request(request_token)

    headers = {"Mcp-Session-Id": session_id}

    # Notifications-only → 202 Accepted with empty body
    if not results:
        return Response(status_code=202, headers=headers)

    body = results if isinstance(payload, list) else results[0]
    auth_failed = any(
        isinstance(result, dict)
        and isinstance(result.get("error"), dict)
        and result["error"].get("code") == -32001
        for result in results
    )
    if auth_failed:
        from services.mcp_oauth import bearer_challenge

        headers["WWW-Authenticate"] = bearer_challenge()
    accept = http_request.headers.get("accept", "")
    if "text/event-stream" in accept and "application/json" not in accept:
        data = json.dumps(body, default=str)
        return StreamingResponse(
            iter([f"event: message\ndata: {data}\n\n"]),
            media_type="text/event-stream",
            headers=headers,
            status_code=401 if auth_failed else 200,
        )
    return JSONResponse(
        content=body,
        headers=headers,
        status_code=401 if auth_failed else 200,
    )


def _protected_resource_metadata() -> dict[str, object]:
    from services.mcp_oauth import get_mcp_oauth_config
    from services.rbac import all_permissions

    config = get_mcp_oauth_config()
    if not config.enabled:
        raise HTTPException(status_code=404, detail="MCP OAuth is not configured")
    return {
        "resource": config.audience,
        "authorization_servers": [config.issuer],
        "bearer_methods_supported": ["header"],
        "scopes_supported": sorted(all_permissions()),
    }


@oauth_resource_router.get("/.well-known/oauth-protected-resource")
@oauth_resource_router.get("/.well-known/oauth-protected-resource/api/v1/mcp")
async def mcp_protected_resource_metadata():
    return _protected_resource_metadata()


@router.get("/policy")
async def get_mcp_policy_route(http_request: Request):
    from services.integrations_store import get_mcp_policy

    _require_mcp_tool_auth(http_request)
    return get_mcp_policy()


@router.put("/policy")
async def set_mcp_policy_route(request: McpPolicyRequest, http_request: Request):
    from services.audit_log import append_audit_event
    from services.integrations_store import get_mcp_policy, set_mcp_policy

    _require_mcp_tool_auth(http_request)
    old = get_mcp_policy()
    try:
        new = set_mcp_policy(request.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    append_audit_event(
        action="mcp.policy.updated",
        resource="/api/v1/mcp/policy",
        actor=getattr(http_request.state, "user_email", None) or "unknown",
        details={"old": old, "new": new},
    )
    return new


@router.get("/manifest")
async def mcp_manifest(http_request: Request):
    """MCP-compatible manifest for IDE and agent integrations."""
    from ..ai.copilot.tools import TOOL_DEFINITIONS

    base = f"{str(http_request.base_url).rstrip('/')}/api/v1/mcp"
    return {
        "name": "dataflow",
        "title": "Datawrap MCP Server",
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
    """Execute a Datawrap Pilot tool — same surface external agents use."""
    _require_mcp_tool_auth(http_request, request.name)
    denial = _mcp_policy_denial(request.name)
    if denial:
        raise HTTPException(status_code=403, detail=denial)
    from services.mcp_invocation_log import log_mcp_invocation
    from services.mcp_rate_limit import check_mcp_rate_limit
    from services.secret_config import mask_secrets_in_text
    from src.services.auth_service import auth_required as mcp_auth_required

    from ..ai.copilot.confirm_ack import reset_mcp_request, set_mcp_request
    from ..ai.copilot.tool_permissions import bind_request_principal
    from ..ai.copilot.tools import get_pilot_tools

    client = http_request.headers.get("X-MCP-Client", "unknown")
    # MCP is a second door into the same tools, so it carries the same role gate:
    # an API key issued as viewer cannot start a transfer through an agent either.
    # An enforced deployment with an unknown role falls back to viewer, never to
    # the open posture — a token we cannot place must not get write reach.
    mcp_role = ""
    if mcp_auth_required():
        mcp_user = getattr(http_request.state, "user", None) or {}
        mcp_role = str(mcp_user.get("role") or "viewer")
    actor = getattr(getattr(http_request, "state", None), "user_email", None) or client
    correlation_id = getattr(http_request.state, "correlation_id", None)
    limit = check_mcp_rate_limit(str(actor or client))
    if not limit.get("allowed"):
        raise HTTPException(
            status_code=429,
            detail={
                "error": "MCP rate limit exceeded",
                "retry_after_sec": limit.get("retry_after_sec"),
                "honesty": "MCP is a production API — rate limits protect the control plane.",
            },
            headers={"Retry-After": str(int(float(limit.get("retry_after_sec") or 1)))},
        )
    start = time.perf_counter()
    try:
        request_token = set_mcp_request(http_request)
        try:
            with bind_request_principal(http_request, mcp_role):
                result = await asyncio.to_thread(
                    get_pilot_tools().execute, request.name, request.arguments
                )
        finally:
            reset_mcp_request(request_token)
    except Exception as exc:
        masked = mask_secrets_in_text(str(exc))
        receipt = log_mcp_invocation(
            tool=request.name,
            client=client,
            arguments=request.arguments,
            status="error",
            error=masked,
            duration_ms=(time.perf_counter() - start) * 1000,
            correlation_id=correlation_id,
            actor=str(actor or "mcp-agent"),
        )
        raise HTTPException(
            status_code=500,
            detail={"error": masked, "tool": request.name, "receipt_id": receipt.get("id")},
        ) from exc

    ms = (time.perf_counter() - start) * 1000
    if not result.success:
        error = mask_secrets_in_text(str(result.error or "tool failed"))
        receipt = log_mcp_invocation(
            tool=request.name,
            client=client,
            arguments=request.arguments,
            status="error",
            error=error,
            duration_ms=ms,
            correlation_id=correlation_id,
            actor=str(actor or "mcp-agent"),
        )
        raise HTTPException(
            status_code=422,
            detail={"error": error, "tool": request.name, "receipt_id": receipt.get("id")},
        )

    receipt = log_mcp_invocation(
        tool=request.name,
        client=client,
        arguments=request.arguments,
        status="ok",
        duration_ms=ms,
        correlation_id=correlation_id,
        actor=str(actor or "mcp-agent"),
    )
    return {
        "tool": result.name,
        "success": True,
        "output": result.output,
        "receipt_id": receipt.get("id"),
        "ms": receipt.get("ms"),
        "status": "ok",
    }


@router.get("/logs")
async def mcp_request_logs(http_request: Request, limit: int = 50):
    """Recent MCP tool invocations from persistent log."""
    _require_mcp_tool_auth(http_request)
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
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
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
