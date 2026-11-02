"""Request-principal binding for sinks that run outside the router layer.

:class:`services.rbac.RBACMiddleware` binds the authenticated role for the
duration of a request so connectors and stores can refuse admin-only work
without importing the auth stack. Scheduler and worker threads have no bound
principal; they rely on creation-time gates.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from contextvars import ContextVar
from typing import Any

from services.api_errors import ApiError
from services.brand_env import getenv_brand

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


class SingerUnavailable(ApiError, PermissionError):
    """A Singer tap cannot be used here; the app renders ``{"error": {code, message}}``.

    Codes: ``singer_taps_dir_unset`` (409, the operator has not enabled taps) and
    ``singer_admin_required`` (403, the caller is not the platform admin). It is
    still a ``PermissionError`` so the bridge sinks, which tell an allowlist or
    role refusal from an exec failure, keep doing so.
    """


def singer_admin_required(gated: list[str]) -> SingerUnavailable:
    return SingerUnavailable(403, "singer_admin_required", admin_only_message(gated))


def singer_taps_dir() -> str:
    """The operator's taps directory, or ``SingerUnavailable`` naming the setting."""
    raw = (getenv_brand("SINGER_TAPS_DIR") or "").strip()
    if not raw:
        raise SingerUnavailable(
            409,
            "singer_taps_dir_unset",
            "singer_tap is disabled: the server administrator must set DATAWRAP_SINGER_TAPS_DIR "
            "(DATAFLOW_SINGER_TAPS_DIR also works) to the directory of operator-installed "
            "tap-* executables and restart the API",
        )
    root = os.path.realpath(raw)
    if not os.path.isdir(root):
        raise SingerUnavailable(
            409,
            "singer_taps_dir_unset",
            f"singer_tap is disabled: DATAWRAP_SINGER_TAPS_DIR ({raw}) is not a directory; "
            "the server administrator must point it at the tap-* executables",
        )
    return root


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
        raise singer_admin_required(admin_only_types([connector_type]))


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


def assert_bound_principal_may_use_endpoints(
    workspace_id: str | None, *endpoints: Any, require_taps: bool = False
) -> None:
    """Endpoint guard: role check for a bound non-admin principal.

    No-op for the role without a bound principal (scheduler/worker) or for admins.
    ``require_taps`` also refuses a Singer endpoint while taps are disabled
    (``singer_taps_dir_unset``): the schedule create/enable and run-now paths.
    """
    role = _REQUEST_ROLE.get()
    if role in (None, "admin") and not require_taps:
        return
    singer = False
    for connector_type in endpoint_connector_types(workspace_id, *endpoints):
        for alias in connector_type_aliases(connector_type):
            assert_bound_principal_may_use_connector(alias)
            singer = singer or connector_type_requires_admin(alias)
    if require_taps and singer:
        singer_taps_dir()
