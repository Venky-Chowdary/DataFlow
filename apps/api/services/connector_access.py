"""Caller scope for saved-connector resolution (tenant boundary).

Saved connectors carry another tenant's live credentials, so "which workspace"
must never default to "every workspace". Each authenticated HTTP request binds
its caller here (``src/middleware/connector_access_middleware.py``; Pilot and
MCP tool turns re-bind it in ``bind_request_principal``). The connector stores
then check a stored document's ``workspace_id`` against that caller on every
get / list / update / delete, whether or not the call site passed a workspace.

Code that runs outside a request (schedulers, CDC workers, CLI scripts) binds
nothing and keeps system scope, as does the unauthenticated single-operator
deployment, which has no identity to scope by.
"""

from __future__ import annotations

import contextlib
import contextvars
import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class ConnectorCaller:
    """Who is resolving connectors, and in which (requested) workspace."""

    actor: str
    requested_workspace_id: str = ""
    platform_admin: bool = False
    _header_readable: bool | None = field(default=None, repr=False)
    _read_cache: dict[str, bool] = field(default_factory=dict, repr=False)
    _write_cache: dict[str, bool] = field(default_factory=dict, repr=False)

    def _can_read(self, workspace_id: str) -> bool:
        if workspace_id not in self._read_cache:
            from services.team_store import can_read_workspace

            try:
                self._read_cache[workspace_id] = bool(can_read_workspace(workspace_id, self.actor))
            except Exception as exc:
                logger.warning("workspace read check failed closed: %s", type(exc).__name__)
                self._read_cache[workspace_id] = False
        return self._read_cache[workspace_id]

    def _can_write(self, workspace_id: str) -> bool:
        if workspace_id not in self._write_cache:
            from services.team_store import can_write_workspace

            try:
                self._write_cache[workspace_id] = bool(can_write_workspace(workspace_id, self.actor))
            except Exception as exc:
                logger.warning("workspace write check failed closed: %s", type(exc).__name__)
                self._write_cache[workspace_id] = False
        return self._write_cache[workspace_id]

    def authorized_workspace_id(self) -> str:
        """The requested workspace when the caller may read it, else ``""``."""
        ws = (self.requested_workspace_id or "").strip()
        if not ws:
            return ""
        if self.platform_admin:
            return ws
        if self._header_readable is None:
            self._header_readable = self._can_read(ws)
        return ws if self._header_readable else ""

    def may_access(self, resource_workspace_id: str | None, *, write: bool = False) -> bool:
        if self.platform_admin:
            return True
        from services.team_store import require_workspace_isolation

        ws = (resource_workspace_id or "").strip()
        if not ws:
            # Unscoped (legacy/demo) connectors stay shared unless hard isolation is on.
            return not require_workspace_isolation()
        requested = (self.requested_workspace_id or "").strip()
        if requested:
            # The declared scope is the tenant boundary: a member of two
            # workspaces operating in A must not reach B's connector by id.
            if ws != requested or not self.authorized_workspace_id():
                return False
        return self._can_write(ws) if write else self._can_read(ws)


_CALLER: contextvars.ContextVar[ConnectorCaller | None] = contextvars.ContextVar(
    "connector_access_caller", default=None
)


def current_caller() -> ConnectorCaller | None:
    return _CALLER.get()


@contextlib.contextmanager
def bind_connector_caller(caller: ConnectorCaller | None) -> Iterator[None]:
    """Bind ``caller`` for connector resolution; ``None`` keeps the current binding."""
    if caller is None:
        yield
        return
    token = _CALLER.set(caller)
    try:
        yield
    finally:
        _CALLER.reset(token)


@contextlib.contextmanager
def system_connector_scope() -> Iterator[None]:
    """Resolve without a caller — only to read a doc's owner before authorizing it."""
    token = _CALLER.set(None)
    try:
        yield
    finally:
        _CALLER.reset(token)


def caller_workspace_id() -> str:
    """The bound caller's authorized workspace, or ``""``."""
    caller = _CALLER.get()
    return caller.authorized_workspace_id() if caller is not None else ""


def may_read_connector(resource_workspace_id: str | None) -> bool:
    caller = _CALLER.get()
    return True if caller is None else caller.may_access(resource_workspace_id)


def may_write_connector(resource_workspace_id: str | None) -> bool:
    caller = _CALLER.get()
    return True if caller is None else caller.may_access(resource_workspace_id, write=True)


def _doc_workspace(item: Any) -> str:
    if isinstance(item, dict):
        return str(item.get("workspace_id") or "")
    return str(getattr(item, "workspace_id", "") or "")


def visible(item: T | None) -> T | None:
    """``item`` when the bound caller may read it, else ``None``."""
    if item is None or may_read_connector(_doc_workspace(item)):
        return item
    logger.info("connector lookup refused outside the caller's workspace")
    return None


def visible_items(items: Iterable[T]) -> list[T]:
    caller = _CALLER.get()
    if caller is None:
        return list(items)
    return [i for i in items if caller.may_access(_doc_workspace(i))]


def writable(item: Any) -> bool:
    return item is not None and may_write_connector(_doc_workspace(item))


def _is_platform_admin(user: dict[str, Any], email: str) -> bool:
    if str(user.get("role") or "").lower() != "admin":
        return False
    if str(user.get("auth_kind") or "") != "api_key":
        return True
    # A workspace API key's "admin" role is granted by whoever minted it. It is
    # platform-wide only when its creator is a platform admin.
    try:
        from src.services.auth_service import lookup_user

        creator = lookup_user(email) or {}
    except Exception:
        return False
    return str(creator.get("role") or "").lower() == "admin"


def caller_from_request(request: Any) -> ConnectorCaller | None:
    """The connector caller for an HTTP request, or ``None`` when unauthenticated."""
    state = getattr(request, "state", None)
    user = getattr(state, "user", None) if state is not None else None
    email = getattr(state, "user_email", None) if state is not None else None
    if not user and not email:
        return None
    user = user if isinstance(user, dict) else {}
    actor = str(email or user.get("email") or "anonymous")
    try:
        requested = str(request.headers.get("x-workspace-id") or "").strip()
    except Exception:
        requested = ""
    return ConnectorCaller(
        actor=actor,
        requested_workspace_id=requested,
        platform_admin=_is_platform_admin(user, actor),
    )
