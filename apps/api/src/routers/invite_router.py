"""Public, token-authenticated half of workspace invitations.

The invitee holds no session yet (or holds one for an existing account), so
these routes are on the public lists in ``auth_middleware``, ``rbac`` and
``tenant_middleware``; the single-use link is the credential. When a Bearer
token is present the middleware still attaches its identity, which is how an
existing account accepts. Admin operations live in ``team_router``.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

router = APIRouter(prefix="/invites", tags=["Team"])


class InviteTokenBody(BaseModel):
    # The token travels in the JSON body, never the query string, so it
    # can't land in an access log.
    token: str = Field(min_length=16, max_length=256)


class InviteAcceptBody(InviteTokenBody):
    name: str = Field(default="", max_length=128)
    password: str = Field(default="", max_length=1024)
    device_id: str = Field(default="", max_length=128)


def _client_ip(request: Request) -> str:
    from services.client_ip import client_ip_from_request

    return client_ip_from_request(request) or ""


def _signed_in_email(request: Request) -> str:
    return str(getattr(request.state, "user_email", None) or "")


@router.post("/inspect")
async def inspect(body: InviteTokenBody, request: Request) -> dict[str, Any]:
    from services.workspace_invites import inspect_invite

    return inspect_invite(token=body.token, signed_in_email=_signed_in_email(request),
                          ip=_client_ip(request))


@router.post("/accept")
async def accept(body: InviteAcceptBody, request: Request, response: Response) -> dict[str, Any]:
    from services.workspace_invites import accept_invite

    # argon2 hashing for a new account runs off the event loop.
    result = await run_in_threadpool(
        accept_invite, token=body.token, signed_in_email=_signed_in_email(request),
        name=body.name, password=body.password, ip=_client_ip(request),
        ua=request.headers.get("user-agent", "")[:512], device_id=body.device_id)
    session = result.pop("session", None)
    if session is not None:
        from src.services.browser_session import issue_browser_session

        result.update(issue_browser_session(response, session, result["user"]["email"]))
    return result
