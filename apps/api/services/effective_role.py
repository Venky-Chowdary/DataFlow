"""One answer to "what may this caller do here", from two role authorities.

A deployment has *platform* roles (``admin`` / ``member``) that say who may
create accounts and workspaces, and each workspace has its own membership role
(``admin`` / ``editor`` / ``viewer``) that says what a person may do *inside*
that workspace. The API gate (``services.rbac``) used to read only the platform
role off the token, and ``member`` is not one of its four labels, so it failed
closed to ``viewer``: an account the Team UI created as an **editor** could read
everything and write nothing, with no message saying why.

The effective role resolved here is the caller's authority for the workspace the
request names:

- a platform administrator is an administrator everywhere;
- otherwise the membership role in the addressed workspace decides;
- a request that names no workspace is answered by the caller's single
  membership when they have exactly one, and never guesses between several;
- with no membership to read, the platform label decides, and an unknown label
  still fails closed to viewer.

Membership can only ever *raise* authority to what a workspace admin granted —
it never grants more than the membership says, and never less than the platform
role already carried.
"""

from __future__ import annotations

from typing import Any

from services import rbac as _rbac
from services.rbac import Permission

# Workspace membership roles map onto gate roles by the same name; ``editor`` is
# deliberately the gate's ``editor`` (write, but not workspace administration).
_MEMBERSHIP_TO_GATE_ROLE = {"admin": "admin", "editor": "editor", "viewer": "viewer"}

_ROLE_RANK = {"viewer": 0, "operator": 1, "editor": 2, "admin": 3}


def _rank(role: str) -> int:
    return _ROLE_RANK.get(role, 0)


def workspace_id_from_request_headers(headers: Any) -> str:
    """The workspace the request addresses, or ``""`` when it names none."""
    try:
        return str(headers.get("x-workspace-id", "") or "").strip()
    except AttributeError:
        return ""


def _membership_role(*, email: str, workspace_id: str) -> str:
    """The membership role that decides this request, or ``""`` when none does.

    A named workspace is answered directly. When the request names none — the web
    client only sends ``X-Workspace-Id`` once a workspace has been chosen — a
    caller who belongs to exactly one workspace is unambiguous, so that
    membership answers. A caller who belongs to several is not: guessing between
    them could hand an editor's authority to someone who is a viewer in the
    workspace they are actually looking at, so the request stays on the platform
    label until it names the workspace.
    """
    from services.team_store import get_workspace_role, list_memberships_for_user

    if workspace_id:
        return get_workspace_role(workspace_id=workspace_id, email=email)
    memberships = list_memberships_for_user(email)
    if len(memberships) == 1:
        return str(memberships[0].get("role") or "")
    return ""


def resolved_workspace_id(user: dict[str, Any] | None, workspace_id: str = "") -> str:
    """The workspace this request is actually decided in.

    A request that names a workspace is decided there. One that names none is
    decided in the caller's single membership when they have exactly one, so the
    client can name that workspace on every later request instead of leaving the
    resolution implicit. Several memberships stay unresolved — see
    :func:`_membership_role`.
    """
    if workspace_id or not user:
        return workspace_id
    email = str(user.get("email") or "").strip()
    if not email:
        return ""
    try:
        from services.team_store import list_memberships_for_user

        memberships = list_memberships_for_user(email)
    except Exception:
        return ""
    if len(memberships) != 1:
        return ""
    return str(memberships[0].get("workspace_id") or "")


def resolve_effective_role(user: dict[str, Any] | None, workspace_id: str = "") -> str:
    """The caller's gate role for ``workspace_id``.

    ``user`` is the identity the auth middleware attached (``email`` + platform
    ``role``). Resolution never raises: a metadata store that cannot be read
    falls back to the platform label rather than locking every caller out.
    """
    if not user:
        return "viewer"
    # Read through the module so a test that substitutes the role normalizer on
    # ``services.rbac`` is honoured here too.
    platform_role = _rbac.normalize_role(user.get("role"))
    if platform_role == "admin":
        return "admin"
    email = str(user.get("email") or "").strip()
    if not email:
        return platform_role
    try:
        membership = _membership_role(email=email, workspace_id=workspace_id)
    except Exception:
        return platform_role
    gate_role = _MEMBERSHIP_TO_GATE_ROLE.get((membership or "").strip().lower(), "")
    if not gate_role:
        return platform_role
    # Membership decides, but never demotes an identity the platform already
    # trusted more than the workspace does.
    return gate_role if _rank(gate_role) >= _rank(platform_role) else platform_role


def effective_permissions(user: dict[str, Any] | None, workspace_id: str = "") -> set[str]:
    return _rbac.role_permissions(resolve_effective_role(user, workspace_id))


def workspace_choice_is_ambiguous(user: dict[str, Any] | None, workspace_id: str = "") -> bool:
    """True when several memberships could apply and the request named none."""
    if workspace_id or not user:
        return False
    if _rbac.normalize_role(user.get("role")) == "admin":
        return False
    email = str(user.get("email") or "").strip()
    if not email:
        return False
    try:
        from services.team_store import list_memberships_for_user

        return len(list_memberships_for_user(email)) > 1
    except Exception:
        return False


def permission_summary(user: dict[str, Any] | None, workspace_id: str = "") -> dict[str, Any]:
    """What the client needs to gate its own controls honestly."""
    role = resolve_effective_role(user, workspace_id)
    granted = sorted(_rbac.role_permissions(role))
    return {
        "workspace_choice_ambiguous": workspace_choice_is_ambiguous(user, workspace_id),
        "effective_role": role,
        "permissions": granted,
        "can_write_connectors": Permission.CONNECTOR_WRITE in granted,
        "can_run_jobs": Permission.JOB_RUN in granted,
        "can_manage_schedules": Permission.SCHEDULE_MANAGE in granted,
        "can_manage_workspace": Permission.WORKSPACE_MANAGE in granted,
    }
