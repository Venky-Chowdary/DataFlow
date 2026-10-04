"""Spend a Pilot approval from MCP or from the Confirm route.

Staging tools return an ack_id and do not mutate. This module is the one
place that turns that ack into a connector, a transfer, a schedule, or a
lifecycle change. MCP and ``POST /copilot/confirm`` both call it.
"""

from __future__ import annotations

import asyncio
import contextvars
import threading
from typing import Any

_request: contextvars.ContextVar[Any] = contextvars.ContextVar("pilot_http_request", default=None)


def set_mcp_request(request: Any) -> contextvars.Token[Any]:
    return _request.set(request)


def reset_mcp_request(token: contextvars.Token[Any]) -> None:
    _request.reset(token)


def current_mcp_request() -> Any:
    return _request.get()


def _detail(exc: Any) -> str:
    detail = getattr(exc, "detail", None)
    if isinstance(detail, dict):
        return str(detail.get("error") or detail.get("detail") or detail)
    if detail:
        return str(detail)
    return str(exc)


async def apply_confirmed_ack(
    ack_id: str,
    *,
    reason: str = "",
    http_request: Any,
    role: str = "",
) -> dict[str, Any]:
    """Run the shared confirm route. Raises nothing; returns a tool-shaped dict."""
    from fastapi import BackgroundTasks, HTTPException

    from src.routers.copilot_router import ConfirmActionRequest, copilot_confirm

    aid = (ack_id or "").strip()
    if not aid:
        return {"ok": False, "error": "ack_id required"}

    # Permission is decided inside copilot_confirm from the request identity,
    # the same gate the Confirm button uses. ``role`` is accepted so callers
    # can pass it; the route does not trust a tool argument for authority.
    del role
    tasks = BackgroundTasks()
    try:
        body = await copilot_confirm(
            ConfirmActionRequest(ack_id=aid, reason=reason or "confirmed"),
            http_request,
            tasks,
        )
    except HTTPException as exc:
        return {"ok": False, "error": _detail(exc)}
    except Exception as exc:  # noqa: BLE001 — MCP must get a tool error, not a 500
        return {"ok": False, "error": _detail(exc)}
    await tasks()
    if isinstance(body, dict):
        return body
    return {"ok": True, "result": body}


def confirm_action_blocking(ack_id: str, reason: str, http_request: Any, role: str) -> dict[str, Any]:
    """Run confirm from a synchronous tool handler.

    The MCP and chat routes are already inside an event loop, so this hops to
    a private loop. The request object and role are passed in; the worker does
    not inherit the caller's context variables.
    """
    coro = apply_confirmed_ack(ack_id, reason=reason, http_request=http_request, role=role)

    def _run() -> dict[str, Any]:
        return asyncio.run(coro)

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _run()

    box: dict[str, Any] = {}
    error: list[BaseException] = []

    def _target() -> None:
        try:
            box["value"] = _run()
        except BaseException as exc:  # noqa: BLE001 — surfaced to the caller
            error.append(exc)

    thread = threading.Thread(target=_target, name="pilot-confirm")
    thread.start()
    thread.join()
    if error:
        raise error[0]
    return box.get("value") or {"ok": False, "error": "Confirm returned no result"}


def confirm_from_tool(ack_id: str, reason: str = "") -> dict[str, Any]:
    request = current_mcp_request()
    if request is None:
        return {
            "ok": False,
            "error": (
                "confirm_action needs the active request. "
                "Call it through MCP, or POST /api/v1/copilot/confirm with the same ack_id."
            ),
        }
    from .tool_permissions import current_caller_role

    return confirm_action_blocking(ack_id, reason, request, current_caller_role())
