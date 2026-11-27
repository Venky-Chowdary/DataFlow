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
#: Workspaces whose entry a platform admin set: only these may sit below the
#: default. Any other entry is clamped up to the default when read.
PLATFORM_SET = "set_by_platform_admin"
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
    """``{"default": mode, "workspaces": {id: mode}}``; unknown values fall back to safe.

    ``set_by_platform_admin`` (the workspaces whose entry a platform admin set)
    is kept only when non-empty and only for entries that exist.
    """
    raw = raw if isinstance(raw, dict) else {}
    default = str(raw.get("default") or DEFAULT_MODE).strip().lower()
    if default not in MODES:
        default = DEFAULT_MODE
    workspaces: dict[str, str] = {}
    for ws, mode in dict(raw.get("workspaces") or {}).items():
        m = str(mode or "").strip().lower()
        if str(ws).strip() and m in MODES:
            workspaces[str(ws).strip()] = m
    out: dict[str, Any] = {"default": default, "workspaces": workspaces}
    raw_set = raw.get(PLATFORM_SET)
    platform_set = sorted(
        {str(ws).strip() for ws in raw_set if str(ws).strip() in workspaces}
    ) if isinstance(raw_set, list) else []
    if platform_set:
        out[PLATFORM_SET] = platform_set
    return out


def effective_mode(policy: dict[str, Any], workspace_id: str) -> str:
    """The mode in force: the default is a floor unless a platform admin set the entry.

    Enforced at read, not only at write (review round 3): an entry stored
    before the floor existed, or by a direct store write, cannot stay looser.
    """
    default = policy["default"]
    ws = str(workspace_id or "").strip()
    entry = policy["workspaces"].get(ws) if ws else None
    if entry is None:
        return default
    if ws in policy.get(PLATFORM_SET, ()) or STRICTNESS[entry] >= STRICTNESS[default]:
        return entry
    return default


def confirm_mode_for(workspace_id: str) -> str:
    """The confirm policy of ``workspace_id``. An unreadable store reads as the safe default."""
    try:
        from services.integrations_store import get_mcp_policy

        policy = normalize_confirm_policy(get_mcp_policy().get("confirm"))
    except Exception:  # noqa: BLE001 - never fall open to ``agent``
        return DEFAULT_MODE
    return effective_mode(policy, workspace_id)


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
    platform_set = set(current.get(PLATFORM_SET, ()))
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
            if platform_admin and new_mode is not None:
                platform_set.add(key)  # a platform admin re-affirmed it
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
        if platform_admin and new_mode is not None:
            platform_set.add(key)
        else:
            platform_set.discard(key)
    merged[PLATFORM_SET] = sorted(platform_set)
    return normalize_confirm_policy(merged)


def own_admin_workspace(user: Any, header_workspace: str) -> str:
    """The workspace a workspace admin's policy change applies to.

    The same answer the RBAC gate decided the request in
    (:func:`services.effective_role.resolved_workspace_id`): the
    ``X-Workspace-Id`` header when sent, else the caller's single membership.
    ``""`` when it is ambiguous or unknown (review round 3: this used to pick
    the one *admin* membership among several, a workspace the gate never read).
    """
    from services.effective_role import resolved_workspace_id

    return resolved_workspace_id(user, str(header_workspace or "").strip())
