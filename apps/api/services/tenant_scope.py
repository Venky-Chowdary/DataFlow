"""Which workspaces one request may read — the single tenant-scope resolver.

Every list, count, stat and id-addressed read answers the same question: may
this actor see a resource stamped with workspace ``W``? Routers used to answer
it themselves from the ``X-Workspace-Id`` header, so a request with no header
saw every unscoped (legacy) row and the job store OR-ed those rows into every
query (D154: a fresh signup account saw the owner's connectors, jobs and
stats). The answer now comes from the actor, not the header:

* a platform admin (or the single operator of an auth-disabled deployment) is
  *unrestricted* and sees every workspace;
* anyone else sees the workspaces they are a member of;
* ``X-Workspace-Id`` only narrows that set — naming a workspace the actor
  cannot read is a 403, never a widening;
* unscoped rows (``workspace_id`` ``""`` / null / missing) are legacy data with
  no owner on record. While isolation is enforced only unrestricted actors see
  them; ``scripts/backfill_legacy_workspaces.py`` assigns them an owner.

Code that runs without a request (schedulers, CDC workers, scripts) passes no
filter and keeps system scope.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from typing import Any, Iterator

from fastapi import HTTPException

from services.workspace_filter import (
    WorkspaceFilter,
    bound_workspace_filter,
    norm_workspace_id as _norm,
)
from services.team_store import (
    list_memberships_for_user,
    list_workspaces_for_user,
    normalize_email,
    require_workspace_isolation,
)

_WRITE_ROLES = frozenset({"admin", "editor"})
_STATE_KEY = "_tenant_scope_base"


@dataclass(frozen=True)
class TenantScope:
    """What one actor may read on one request (see the module docstring)."""

    actor: str
    unrestricted: bool
    enforced: bool
    # Workspace id -> the actor's role there, in membership order (personal first).
    memberships: tuple[tuple[str, str], ...]
    # The header scope, already authorized ("" = no header).
    requested_workspace_id: str = ""

    @property
    def member_workspace_ids(self) -> frozenset[str]:
        return frozenset(ws for ws, _role in self.memberships)

    @property
    def default_workspace_id(self) -> str:
        """The actor's personal workspace, else their earliest membership."""
        return self.memberships[0][0] if self.memberships else ""

    @property
    def workspace_id(self) -> str:
        """The single workspace this request acts in.

        The header when one was sent; otherwise the actor's default workspace
        for a restricted actor. An unrestricted actor with no header gets ``""``
        (every workspace) — callers that need one workspace use
        :meth:`target_workspace_id`.
        """
        if self.requested_workspace_id:
            return self.requested_workspace_id
        return "" if self.unrestricted else self.default_workspace_id

    @property
    def filter(self) -> WorkspaceFilter:
        """Rows a list or count may return."""
        legacy = not self.enforced
        if self.requested_workspace_id:
            return WorkspaceFilter(frozenset({self.requested_workspace_id}), legacy)
        if self.unrestricted:
            return WorkspaceFilter.everything()
        return WorkspaceFilter(self.member_workspace_ids, legacy)

    def strict_filter(self) -> WorkspaceFilter:
        """Like :attr:`filter` but never legacy unscoped rows for a restricted actor.

        For platform records that were never tenant data (the audit trail):
        an unscoped audit row is another account's login or admin action.
        """
        if self.requested_workspace_id:
            return WorkspaceFilter.only(self.requested_workspace_id)
        if self.unrestricted:
            return WorkspaceFilter.everything()
        return WorkspaceFilter(self.member_workspace_ids, False)

    def single_workspace_filter(self) -> WorkspaceFilter:
        """Rows for an endpoint that reports one workspace (usage, ops tiles).

        Exact match, like those stores always were; a restricted actor with no
        workspace gets nothing rather than the platform-wide figure.
        """
        if self.requested_workspace_id:
            return WorkspaceFilter.only(self.requested_workspace_id)
        if self.unrestricted:
            return WorkspaceFilter.everything()
        ws = self.default_workspace_id
        return WorkspaceFilter.only(ws) if ws else WorkspaceFilter.nothing()

    def allows(self, resource_workspace_id: Any) -> bool:
        return self.filter.matches(resource_workspace_id)

    def can_write(self, resource_workspace_id: Any) -> bool:
        """Read visibility plus an admin/editor role in the resource's workspace."""
        if not self.allows(resource_workspace_id):
            return False
        ws = _norm(resource_workspace_id)
        if self.unrestricted or not ws:
            return True
        return dict(self.memberships).get(ws, "") in _WRITE_ROLES

    def target_workspace_id(self, *, write: bool = False) -> str:
        """The one workspace a create/export lands in; never a silent "all".

        With no header this is the actor's default workspace instead of the old
        400. A header-less write that has no workspace to land in keeps the
        legacy unscoped stamp only while isolation is off, and only for
        callers with no membership to prefer (a member's write must not become
        an unscoped row every legacy caller can see).
        """
        ws = self.requested_workspace_id
        if not ws and (self.enforced or not write or (self.memberships and not self.unrestricted)):
            ws = self.default_workspace_id
        if not ws:
            if not self.enforced and write:
                return ""
            if self.unrestricted:
                raise HTTPException(status_code=400, detail="X-Workspace-Id required")
            raise HTTPException(status_code=403, detail="No workspace membership")
        if write and not self.can_write(ws):
            raise HTTPException(status_code=403, detail="Write access to workspace denied")
        return ws

    def narrowed(self, workspace_id: str) -> "TenantScope":
        """This scope narrowed to ``workspace_id`` (403 when it is not readable)."""
        ws = _norm(workspace_id)
        if not ws:
            return self
        if not self.unrestricted and ws not in self.member_workspace_ids:
            raise HTTPException(status_code=403, detail="Access to workspace denied")
        if self.requested_workspace_id and ws != self.requested_workspace_id:
            raise HTTPException(status_code=403, detail="workspace_id does not match X-Workspace-Id")
        return TenantScope(
            actor=self.actor,
            unrestricted=self.unrestricted,
            enforced=self.enforced,
            memberships=self.memberships,
            requested_workspace_id=ws,
        )


def ordered_memberships(email: str) -> tuple[tuple[str, str], ...]:
    """Memberships with the personal workspace first, then by workspace age.

    The personal workspace is the one signup provisioned for this account
    (``signup_service._provision_verified_user``): created by the actor under
    its own organization.
    """
    roles = {
        str(m.get("workspace_id") or ""): str(m.get("role") or "")
        for m in list_memberships_for_user(email)
        if m.get("workspace_id")
    }
    if not roles:
        return ()
    workspaces = [ws for ws in list_workspaces_for_user(email) if ws.id in roles]
    personal = [ws for ws in workspaces if ws.created_by == email and ws.org_id]
    ordered = personal + [ws for ws in workspaces if ws not in personal]
    seen = [ws.id for ws in ordered]
    # A membership whose workspace row is gone still names a scope the actor holds.
    seen += sorted(ws for ws in roles if ws not in seen)
    return tuple((ws, roles[ws]) for ws in seen)


def actor_is_unrestricted(user: dict[str, Any] | None) -> bool:
    """Platform admin, the one definition of a platform admin (``team_router`` resolves a user by email and asks it).

    A workspace API key carrying ``admin`` is unrestricted only when the account
    that minted it is a platform admin — the key's role is a workspace role.
    """
    from src.services.auth_service import auth_required, lookup_user

    if not auth_required():
        return True
    if not isinstance(user, dict):
        return False
    if str(user.get("role") or "").strip().lower() != "admin":
        return False
    if str(user.get("auth_kind") or "") != "api_key":
        return True
    # A key minted in a workspace belongs to that workspace, whoever minted it.
    if str(user.get("workspace_id") or "").strip():
        return False
    creator = lookup_user(str(user.get("email") or ""))
    return bool(creator) and str(creator.get("role") or "").strip().lower() == "admin"


def _base_scope(request: Any) -> TenantScope:
    state = getattr(request, "state", None)
    cached = getattr(state, _STATE_KEY, None) if state is not None else None
    if isinstance(cached, TenantScope):
        return cached
    raw_email = getattr(state, "user_email", None)
    email = normalize_email(raw_email) if isinstance(raw_email, str) else ""
    user = getattr(state, "user", None)
    if isinstance(user, dict) and str(user.get("auth_kind") or "") == "api_key":
        # A key reads its own workspace, not its creator's memberships
        # (services.effective_principal — the resolver every route uses).
        from services.effective_principal import api_key_memberships

        memberships = api_key_memberships(user)
    else:
        memberships = ordered_memberships(email) if email else ()
    scope = TenantScope(
        actor=email or "anonymous",
        unrestricted=actor_is_unrestricted(user),
        enforced=require_workspace_isolation(),
        memberships=memberships,
    )
    if state is not None:
        try:
            setattr(state, _STATE_KEY, scope)
        except Exception:  # noqa: BLE001 - a read-only state still resolves
            pass
    return scope


def requested_header(request: Any) -> str:
    try:
        value = request.headers.get("X-Workspace-Id")
    except Exception:  # noqa: BLE001
        return ""
    return _norm(value) if isinstance(value, str) else ""


def resolve_tenant_scope(request: Any, requested_workspace_id: str | None = None) -> TenantScope:
    """The scope for ``request``; ``requested_workspace_id`` defaults to the header."""
    requested = requested_header(request) if requested_workspace_id is None else requested_workspace_id
    return _base_scope(request).narrowed(requested)


@contextlib.contextmanager
def bind_tenant_scope(scope: TenantScope | None) -> Iterator[None]:
    """Bind ``scope`` for store-level reads in this request/turn (see
    ``services.workspace_filter.bound_workspace_filter``)."""
    stamp = ""
    if scope is not None:
        try:
            stamp = scope.target_workspace_id(write=True)
        except HTTPException:
            stamp = ""
    with bound_workspace_filter(
        scope.filter if scope is not None else None,
        stamp,
        scope.actor if scope is not None else "",
    ):
        yield


def scope_for_binding(request: Any) -> TenantScope | None:
    """The scope to bind for ``request``; None leaves system scope.

    Auth off means the single local operator, who sees everything. A request
    whose header names a workspace the actor cannot read binds a scope that
    sees nothing, so a store read in that request cannot fall back to system
    scope; the router still answers the 403.
    """
    from src.services.auth_service import auth_required

    if not auth_required() or not getattr(request.state, "user_email", None):
        return None
    try:
        return resolve_tenant_scope(request)
    except HTTPException:
        return TenantScope(actor="", unrestricted=False, enforced=True, memberships=())

