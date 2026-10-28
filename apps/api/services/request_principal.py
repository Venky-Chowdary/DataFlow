"""Request-principal binding for sinks that run outside the router layer.

:class:`services.rbac.RBACMiddleware` binds the authenticated role for the
duration of a request so connectors and stores can refuse admin-only work
without importing the auth stack. Scheduler and worker threads have no bound
principal; they rely on creation-time gates.
"""

from __future__ import annotations

from collections.abc import Iterable
from contextvars import ContextVar
from typing import Any

# Connector types that execute operator-installed programs on the API host.
# Only the platform ``admin`` role may create, test, or run them.
ADMIN_ONLY_CONNECTOR_TYPES = frozenset({"singer_tap"})

# Normalized role of the authenticated principal for the current request.
# ``None`` means no request principal is bound (auth disabled, scheduler, worker).
_REQUEST_ROLE: ContextVar[str | None] = ContextVar("df_request_role", default=None)


def connector_type_requires_admin(connector_type: Any) -> bool:
    return str(connector_type or "").strip().lower() in ADMIN_ONLY_CONNECTOR_TYPES


def admin_only_types(connector_types: Iterable[Any]) -> list[str]:
    return sorted({str(t).strip().lower() for t in connector_types if connector_type_requires_admin(t)})


def admin_only_message(gated: list[str]) -> str:
    return (
        f"Permission denied: connector type {', '.join(gated)} runs executables on the "
        "API server and requires the admin role"
    )


def connector_type_aliases(connector_type: Any) -> list[str]:
    raw = str(connector_type or "").strip()
    if not raw:
        return []
    try:
        from src.transfer.connector_capabilities import resolve_driver_type
    except ImportError:
        return [raw]
    return [raw, resolve_driver_type(raw)]


def assert_bound_principal_may_use_connector(connector_type: Any) -> None:
    """Refuse admin-only connectors for a non-admin bound request principal."""
    if not connector_type_requires_admin(connector_type):
        return
    role = _REQUEST_ROLE.get()
    if role is not None and role != "admin":
        raise PermissionError(admin_only_message(admin_only_types([connector_type])))


def endpoint_connector_types(workspace_id: str | None, *endpoints: Any) -> list[str]:
    """Connector types an endpoint can execute: its format, saved connector type, Singer extras."""
    types: list[str] = []
    for ep in endpoints:
        if ep is None:
            continue
        get = ep.get if isinstance(ep, dict) else lambda k, _ep=ep: getattr(_ep, k, None)
        types.append(str(get("format") or ""))
        extra = get("extra") or {}
        if (isinstance(extra, dict) and "tap_command" in extra) or get("tap_command"):
            types.append("singer_tap")
        connector_id = get("connector_id")
        if connector_id:
            from services.connector_store import get_connector

            saved = get_connector(str(connector_id), workspace_id=workspace_id or None)
            if saved is not None:
                types.append(saved.type)
    return [t for t in types if t]


def assert_bound_principal_may_use_endpoints(workspace_id: str | None, *endpoints: Any) -> None:
    """Endpoint guard: no-op without a bound principal (scheduler/worker) or for admins."""
    if _REQUEST_ROLE.get() in (None, "admin"):
        return
    for connector_type in endpoint_connector_types(workspace_id, *endpoints):
        for alias in connector_type_aliases(connector_type):
            assert_bound_principal_may_use_connector(alias)
