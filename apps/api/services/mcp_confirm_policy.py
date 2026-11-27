"""Who may spend a staged Pilot / MCP approval (the human-in-the-loop gate).

Staging tools (``create_connector``, ``start_transfer``, ``create_schedule``,
lifecycle actions) only return an ``ack_id``; ``confirm_action`` (MCP) or
``POST /api/v1/copilot/confirm`` performs the mutation. Until this module,
any caller allowed to read the workspace and to do the staged kind of change
could confirm, on any channel. An MCP client that calls ``confirm_action``
right after staging (many agents auto-approve tool calls) therefore removed
the human approval entirely, with the same workspace API key it staged with.

Confirmation now has to come from a human-bound channel. The policy is set
per workspace (``mcp_policy.confirm`` in the integrations store), with a safe
default:

``interactive`` (default)
    Confirm only from an interactive sign-in session (the browser session
    cookie with its CSRF token, or a sign-in session token), never through
    the MCP endpoint, never with a workspace API key or an MCP OAuth token.
    The confirmer must be the person who staged the approval (an API key
    stages as the person who created it) or a workspace admin.
``four_eyes``
    As ``interactive``, and the confirmer must be a different person from the
    one who staged it and hold workspace admin (``workspace.manage``).
``agent``
    The previous behaviour: any caller with the staged kind's permission, on
    any channel, including MCP ``confirm_action``. An explicit opt-out for
    unattended automation; the confirmation is then not a human approval.

What this does not cover: a person who pastes their own sign-in session
token into an agent has delegated their session. Sign-in tokens expire and
are revoked by the workspace idle policy; ``four_eyes`` is the setting for
workspaces that need a second person regardless.
"""

from __future__ import annotations

from typing import Any

MODES = ("interactive", "four_eyes", "agent")
#: Strictness order: a workspace admin may not go below the deployment default.
STRICTNESS = {"agent": 0, "interactive": 1, "four_eyes": 2}
DEFAULT_MODE = "interactive"
#: Modes whose confirmations are human-bound. Weakening a workspace from one
#: of these to ``agent`` must itself be done from an interactive session.
HUMAN_BOUND = frozenset({"interactive", "four_eyes"})

MODE_DESCRIPTIONS = {
    "interactive": "Confirm in the product, signed in, by the person who staged it or a workspace admin.",
    "four_eyes": "Confirm in the product, signed in, by a different workspace admin.",
    "agent": "Any caller with permission may confirm, including an MCP client (no human approval).",
}


class ConfirmRefused(PermissionError):
    """The confirmation did not come from the channel the workspace requires."""


def normalize_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    if mode not in MODES:
        raise ValueError(f"confirm policy must be one of {', '.join(MODES)}")
    return mode


def normalize_confirm_policy(raw: Any) -> dict[str, Any]:
    """``{"default": mode, "workspaces": {id: mode}}``; unknown values fall back to safe."""
    raw = raw if isinstance(raw, dict) else {}
    default = str(raw.get("default") or DEFAULT_MODE).strip().lower()
    if default not in MODES:
        default = DEFAULT_MODE
    workspaces: dict[str, str] = {}
    for ws, mode in dict(raw.get("workspaces") or {}).items():
        m = str(mode or "").strip().lower()
        if str(ws).strip() and m in MODES:
            workspaces[str(ws).strip()] = m
    return {"default": default, "workspaces": workspaces}


def confirm_mode_for(workspace_id: str) -> str:
    """The confirm policy of ``workspace_id``. An unreadable store reads as the safe default."""
    try:
        from services.integrations_store import get_mcp_policy

        policy = normalize_confirm_policy(get_mcp_policy().get("confirm"))
    except Exception:  # noqa: BLE001 - never fall open to ``agent``
        return DEFAULT_MODE
    ws = str(workspace_id or "").strip()
    return policy["workspaces"].get(ws, policy["default"]) if ws else policy["default"]


def credential_kind(request: Any) -> str:
    """``cookie_session``, ``session_token``, ``api_key``, ``oauth`` or ``none``."""
    state = getattr(request, "state", None)
    user = getattr(state, "user", None) or {}
    if getattr(state, "api_key_auth", False) or user.get("auth_kind") == "api_key":
        return "api_key"
    if user.get("auth_kind") == "oauth":
        return "oauth"
    if getattr(state, "cookie_session_jti", None):
        return "cookie_session"
    if user:
        return "session_token"
    return "none"


def via_mcp(request: Any) -> bool:
    """True when the confirmation arrives through the MCP endpoint (``confirm_action``)."""
    try:
        path = str(request.url.path or "")
    except Exception:  # noqa: BLE001 - a request without a URL is not the MCP route
        return False
    return path == "/api/v1/mcp" or path.startswith("/api/v1/mcp/")


def _is_admin(role: str) -> bool:
    from services.rbac import Permission, role_permissions

    return Permission.WORKSPACE_MANAGE in role_permissions(role)


def check_confirm_channel(
    request: Any,
    *,
    staged_by: str,
    staged_in: str,
    role: str,
    actor: str,
    auth_enforced: bool,
) -> str:
    """Return the mode applied, or raise :class:`ConfirmRefused` with the reason."""
    mode = confirm_mode_for(staged_in)
    if mode == "agent":
        return mode
    if via_mcp(request):
        raise ConfirmRefused(
            "This workspace requires a person to confirm in the product: an MCP client "
            "cannot spend the approval it staged. Open the approval in Pilot (or "
            "POST /api/v1/copilot/confirm from a signed-in session) to confirm it."
        )
    if not auth_enforced:
        # An open deployment has no identity to bind; only the channel is held.
        return mode
    kind = credential_kind(request)
    if kind not in {"cookie_session", "session_token"}:
        raise ConfirmRefused(
            "Confirming needs an interactive sign-in session; a workspace API key or "
            "MCP token cannot confirm in this workspace."
        )
    owner = str(staged_by or "").strip().lower()
    who = str(actor or "").strip().lower()
    if mode == "four_eyes":
        if not _is_admin(role):
            raise ConfirmRefused("This workspace requires a workspace admin to confirm (four-eyes).")
        if not who or not owner or who == owner:
            raise ConfirmRefused(
                "This workspace requires a second person to confirm: the person who staged "
                "the approval cannot confirm it (four-eyes)."
            )
        return mode
    if owner and who != owner and not _is_admin(role):
        raise ConfirmRefused(
            "Only the person who staged this approval, or a workspace admin, can confirm it."
        )
    return mode


def merge_confirm_change(
    stored: Any,
    requested: Any,
    *,
    platform_admin: bool,
    caller_workspace: str,
) -> dict[str, Any]:
    """Apply a requested confirm-policy change on top of the stored one.

    The store is deployment-wide, but ``PUT /api/v1/mcp/policy`` is open to any
    workspace admin. So: ``default`` and other workspaces' entries need a
    platform admin; a workspace admin may set (or clear, with ``null``) only
    their own workspace's entry, and only as strict as or stricter than the
    default (agent < interactive < four_eyes). Entries the request does not
    name are kept — a change merges, it never replaces another workspace's
    policy.
    Raises ``PermissionError`` (refused) or ``ValueError`` (invalid mode).
    """
    if not isinstance(requested, dict):
        raise ValueError("confirm must be an object: {default, workspaces}")
    current = normalize_confirm_policy(stored)
    merged = {"default": current["default"], "workspaces": dict(current["workspaces"])}
    if "default" in requested and requested["default"] is not None:
        mode = normalize_mode(requested["default"])
        if mode != current["default"]:
            if not platform_admin:
                raise PermissionError(
                    "Only a platform admin can change the default confirm policy for every workspace."
                )
            merged["default"] = mode
    own = str(caller_workspace or "").strip()
    workspaces = requested.get("workspaces")
    if workspaces is not None and not isinstance(workspaces, dict):
        raise ValueError("confirm.workspaces must be an object of workspace id -> mode")
    for ws, mode in dict(workspaces or {}).items():
        key = str(ws or "").strip()
        if not key:
            continue
        new_mode = None if mode is None else normalize_mode(mode)
        if new_mode == current["workspaces"].get(key):
            continue
        if key != own and not platform_admin:
            raise PermissionError(
                f"A workspace admin can change only their own workspace's confirm policy, not {key!r}."
            )
        if (
            new_mode is not None
            and not platform_admin
            and STRICTNESS[new_mode] < STRICTNESS[merged["default"]]
        ):
            # The deployment default is a floor: a workspace admin may be as
            # strict or stricter, never looser (review N6).
            raise PermissionError(
                f"'{new_mode}' is looser than the deployment default '{merged['default']}'; "
                "only a platform admin can set a workspace below the default."
            )
        if new_mode is None:
            merged["workspaces"].pop(key, None)
        else:
            merged["workspaces"][key] = new_mode
    return merged


def own_admin_workspace(user: Any, header_workspace: str) -> str:
    """The workspace a workspace admin's policy change applies to.

    The ``X-Workspace-Id`` header when sent; otherwise the one workspace the
    caller administers (review N7: such an admin got 403 for sending none).
    ``""`` when it is ambiguous or unknown.
    """
    if str(header_workspace or "").strip():
        return str(header_workspace).strip()
    email = str((user or {}).get("email") or "").strip()
    if not email:
        return ""
    try:
        from services.team_store import list_memberships_for_user

        rows = list_memberships_for_user(email)
    except Exception:  # noqa: BLE001 - unknown means no implied workspace
        return ""
    admin = {
        str(r.get("workspace_id") or "")
        for r in rows
        if str(r.get("role") or "").lower() in ("admin", "owner") and r.get("workspace_id")
    }
    return next(iter(admin)) if len(admin) == 1 else ""
