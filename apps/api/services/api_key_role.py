"""Role carried by a workspace API key.

A key is an automation principal. It is not a person, and it is not "whoever
happens to call MCP." The role is stored on the key and checked on every
request, the same way a signed-in operator's role is checked.

Keys created before this field existed have no role. Those keys are the
workspace automation credential the MCP page tells an operator to paste into
Cursor. They resolve to **editor**: that role can create connectors, run
transfers, and manage schedules, and it cannot administer the workspace.
An unknown label fails closed to viewer. A key that must only read is created
with ``viewer`` explicitly.
"""

from __future__ import annotations

API_KEY_ROLES = ("viewer", "operator", "editor", "admin")
DEFAULT_API_KEY_ROLE = "editor"


def resolve_stored_api_key_role(raw: object) -> str:
    """Role a stored key actually has. Blank means the legacy editor credential."""
    if raw is None:
        return DEFAULT_API_KEY_ROLE
    token = str(raw).strip().lower()
    if not token:
        return DEFAULT_API_KEY_ROLE
    if token in API_KEY_ROLES:
        return token
    return "viewer"


def parse_requested_api_key_role(raw: object) -> str:
    """Role an admin asked to mint. Blank means editor. Anything else must be named."""
    if raw is None or not str(raw).strip():
        return DEFAULT_API_KEY_ROLE
    token = str(raw).strip().lower()
    if token in API_KEY_ROLES:
        return token
    allowed = ", ".join(API_KEY_ROLES)
    raise ValueError(f"role must be one of: {allowed}")
