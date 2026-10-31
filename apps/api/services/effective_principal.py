"""Who is acting, and with what authority, for one request in one workspace.

Every route used to answer this its own way. The REST gate
(``services.rbac.RBACMiddleware``) read the API key's role; the team routes read
the *key creator's* membership by email. On production that let a viewer key
minted by the owner add members, and an editor key demote the owner (QA
enterprise #24/#25). This module is the one resolver both use:

- **session** (password, SSO, cookie): the user's membership in the workspace;
  a platform administrator is an administrator everywhere.
- **API key**: the role stored on the key, capped at what its creator may do in
  that workspace today (``effective_role.api_key_creator_cap``). A key bound to
  a workspace has no authority in any other.
- **service account / SCIM token**: an API key whose permissions are further
  narrowed to its scopes.

``member_role`` is the authority inside the workspace (``""`` = none: not a
member, or a key bound elsewhere); ``permissions`` are what that role grants
after scopes. Membership decisions read these, never an email lookup.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services import rbac as _rbac
from services.effective_role import (
    _rank,
    api_key_creator_cap,
    cap_role,
    membership_role_to_gate_role,
)


@dataclass(frozen=True)
class EffectivePrincipal:
    kind: str  # session | api_key | service_account | scim | oauth | anonymous
    email: str  # the person accountable (a key's creator)
    workspace_id: str
    member_role: str  # gate role inside the workspace, "" = no authority there
    permissions: frozenset[str]
    platform_admin: bool
    api_key_id: str = ""
    scopes: tuple[str, ...] | None = None

    def has(self, permission: str) -> bool:
        return permission in self.permissions

    def outranks_or_equals(self, role: str) -> bool:
        return bool(self.member_role) and _rank(self.member_role) >= _rank(
            membership_role_to_gate_role(role)
        )

    def describe(self) -> dict[str, Any]:
        """Audit form: enough to answer "who tried this" without any secret."""
        return {
            "kind": self.kind,
            "email": self.email,
            "api_key_id": self.api_key_id or None,
            "effective_role": self.member_role or "none",
            "platform_admin": self.platform_admin,
            "scopes": list(self.scopes) if self.scopes is not None else None,
        }


def _kind(user: dict[str, Any] | None) -> str:
    if not isinstance(user, dict):
        return "anonymous"
    auth_kind = str(user.get("auth_kind") or "")
    if auth_kind == "api_key":
        kind = str(user.get("kind") or "api_key")
        return kind if kind in ("service_account", "scim") else "api_key"
    if auth_kind == "oauth":
        return "oauth"
    return "session"


def _session_member_role(email: str, workspace_id: str) -> str:
    from services.team_store import get_workspace_role

    role = get_workspace_role(workspace_id=workspace_id, email=email) if email else ""
    return membership_role_to_gate_role(role) if role else ""


def _creator_member_role(email: str, workspace_id: str) -> str:
    """What a key's creator may do in the workspace: admin for a platform admin."""
    from src.services.auth_service import lookup_user

    account = lookup_user(email) if email else None
    if account and _rbac.normalize_role(account.get("role")) == "admin":
        return "admin"
    return _session_member_role(email, workspace_id)


def resolve_principal(user: dict[str, Any] | None, workspace_id: str) -> EffectivePrincipal:
    """The effective principal of ``user`` inside ``workspace_id``."""
    from services.tenant_scope import actor_is_unrestricted

    workspace_id = str(workspace_id or "")
    kind = _kind(user)
    email = str((user or {}).get("email") or "").strip().lower()
    platform_admin = actor_is_unrestricted(user)
    scopes_raw = (user or {}).get("scopes")
    scopes = (
        tuple(sorted(s for s in scopes_raw if isinstance(s, str)))
        if isinstance(scopes_raw, list)
        else None
    )

    if platform_admin:
        member_role = "admin"
    elif kind in ("api_key", "service_account", "scim"):
        bound = str((user or {}).get("workspace_id") or "")
        if bound and workspace_id and bound != workspace_id:
            member_role = ""
        else:
            key_role = _rbac.normalize_role((user or {}).get("role"))
            # The creator must hold authority here; a creator with none gives
            # the key none, whatever role was written on it.
            creator_role = _creator_member_role(email, workspace_id)
            member_role = (
                cap_role(
                    cap_role(key_role, api_key_creator_cap(user or {}, workspace_id)),
                    creator_role,
                )
                if creator_role
                else ""
            )
    elif kind == "anonymous":
        member_role = ""
    else:
        member_role = _session_member_role(email, workspace_id)

    permissions = (
        frozenset(_rbac.principal_permissions(user, member_role)) if member_role else frozenset()
    )
    return EffectivePrincipal(
        kind=kind,
        email=email or "anonymous",
        workspace_id=workspace_id,
        member_role=member_role,
        permissions=permissions,
        platform_admin=platform_admin,
        api_key_id=str((user or {}).get("api_key_id") or ""),
        scopes=scopes,
    )


def request_principal(request: Any, workspace_id: str) -> EffectivePrincipal:
    """``resolve_principal`` for the identity the auth middleware attached."""
    state = getattr(request, "state", None)
    user = getattr(state, "user", None)
    if not isinstance(user, dict):
        email = getattr(state, "user_email", None)
        user = {"email": email, "role": "viewer"} if email else None
    return resolve_principal(user, workspace_id)


def request_may_read_workspace(request: Any, workspace_id: str) -> bool:
    """Any authority in the workspace, by the request principal."""
    from services.team_store import require_workspace_isolation

    if not workspace_id:
        return not require_workspace_isolation()
    return bool(request_principal(request, workspace_id).member_role)


def request_may_write_workspace(request: Any, workspace_id: str) -> bool:
    """Write authority in a workspace (editor or admin), by the request principal.

    Replaces ``team_store.can_write_workspace(ws, <email>)`` on routes: an email
    lookup answered with the key creator's membership, not the key's.
    """
    from services.team_store import require_workspace_isolation

    if not workspace_id:
        return not require_workspace_isolation()
    return request_principal(request, workspace_id).member_role in ("editor", "admin")


_GATE_TO_MEMBERSHIP = {"admin": "admin", "editor": "editor", "operator": "viewer", "viewer": "viewer"}


def api_key_memberships(user: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    """The workspaces an API key may read, with its role there.

    A key minted in a workspace reads that workspace only — never the other
    workspaces its creator belongs to — and only while its creator still holds
    authority there (``resolve_principal``). A legacy key minted before keys
    carried a workspace reads its creator's workspace when that is unambiguous
    (exactly one membership), and nothing otherwise.
    """
    bound = str(user.get("workspace_id") or "").strip()
    if bound:
        candidates = [bound]
    else:
        from services.team_store import list_memberships_for_user

        email = str(user.get("email") or "").strip().lower()
        rows = [m for m in list_memberships_for_user(email) if m.get("workspace_id")] if email else []
        candidates = [str(rows[0]["workspace_id"])] if len(rows) == 1 else []
    out: list[tuple[str, str]] = []
    for workspace_id in candidates:
        role = resolve_principal(user, workspace_id).member_role
        if role:
            out.append((workspace_id, _GATE_TO_MEMBERSHIP.get(role, "viewer")))
    return tuple(out)


def api_key_creator_is_accountable(creator_email: str, kind: str = "api_key") -> bool:
    """A key's creator must still be someone: an active account, an
    environment identity, or a workspace member. A deleted or never-existing
    creator leaves the key with no one accountable for it, so it is refused
    rather than left with its full role; a disabled account's personal keys
    are refused too."""
    creator = str(creator_email or "").strip().lower()
    if not creator or creator in ("api-key", "anonymous"):
        return False
    from services.team_store import list_memberships_for_user
    from services.user_store import get_user_record
    from src.services.auth_service import lookup_user

    stored = get_user_record(creator)
    if stored is not None:
        if str(stored.get("status") or "active") == "active":
            return True
        # A deactivated person's own keys die with the account; service
        # accounts and SCIM tokens belong to the organization and survive it
        # (SCIM deprovisioning keeps them, capped at viewer by the creator cap).
        return kind in ("service_account", "scim")
    if lookup_user(creator) is not None:
        return True
    return bool(list_memberships_for_user(creator))
