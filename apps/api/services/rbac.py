"""Role-based access control for the Datawrap API.

Permission model (enterprise-friendly):

- viewer:  read jobs, connectors, schedules, audit, workspace.
- editor:  viewer + run transfers, manage connectors, schedules, plans.
- admin:   editor + workspace administration, user management, settings.

Unknown roles and the dev "Workspace tester" role map to editor so development
is not blocked, but production must still gate based on the actual role claim.
"""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from src.services import auth_service as _auth_service


class Permission:
    JOB_READ = "job.read"
    JOB_RUN = "job.run"
    JOB_PLAN = "job.plan"
    JOB_MANAGE = "job.manage"  # cancel/retry/resume
    CONNECTOR_READ = "connector.read"
    CONNECTOR_WRITE = "connector.write"
    CONNECTOR_DELETE = "connector.delete"
    SCHEDULE_READ = "schedule.read"
    SCHEDULE_MANAGE = "schedule.manage"
    # Minting standing authority for unattended runs is a separate, higher power
    # than operating a schedule: approving one run is schedule.manage, while
    # delegating a signature to every future run of that plan is admin-only.
    SCHEDULE_AUTHORIZE = "schedule.authorize"
    AUDIT_READ = "audit.read"
    WORKSPACE_READ = "workspace.read"
    WORKSPACE_MANAGE = "workspace.manage"
    AI_USE = "ai.use"
    QUERY_USE = "query.use"
    # Acting on your *own* credential (rotating a one-time password). Every role
    # holds it: without it an admin-issued temporary password could never be
    # retired by the person who received it.
    ACCOUNT_SELF = "account.self"


_ALL_PERMISSIONS = {
    Permission.JOB_READ,
    Permission.JOB_RUN,
    Permission.JOB_PLAN,
    Permission.JOB_MANAGE,
    Permission.CONNECTOR_READ,
    Permission.CONNECTOR_WRITE,
    Permission.CONNECTOR_DELETE,
    Permission.SCHEDULE_READ,
    Permission.SCHEDULE_MANAGE,
    Permission.SCHEDULE_AUTHORIZE,
    Permission.AUDIT_READ,
    Permission.WORKSPACE_READ,
    Permission.WORKSPACE_MANAGE,
    Permission.AI_USE,
    Permission.QUERY_USE,
    Permission.ACCOUNT_SELF,
}


_ROLE_PERMISSIONS: dict[str, set[str]] = {
    "viewer": {
        Permission.JOB_READ,
        Permission.CONNECTOR_READ,
        Permission.SCHEDULE_READ,
        Permission.AUDIT_READ,
        Permission.WORKSPACE_READ,
        Permission.ACCOUNT_SELF,
        Permission.QUERY_USE,
        # Asking the assistant is a read: Pilot gates each tool it reaches by the
        # same permission as the REST route that performs it, so ai.use lets a
        # viewer ask "why did this job fail" without letting it run anything.
        Permission.AI_USE,
    },
    "editor": {
        Permission.JOB_READ,
        Permission.JOB_RUN,
        Permission.JOB_PLAN,
        Permission.JOB_MANAGE,
        Permission.CONNECTOR_READ,
        Permission.CONNECTOR_WRITE,
        Permission.SCHEDULE_READ,
        Permission.SCHEDULE_MANAGE,
        Permission.AUDIT_READ,
        Permission.WORKSPACE_READ,
        Permission.ACCOUNT_SELF,
        Permission.AI_USE,
        Permission.QUERY_USE,
    },
    "operator": {
        Permission.JOB_READ,
        Permission.JOB_RUN,
        Permission.JOB_MANAGE,
        Permission.CONNECTOR_READ,
        Permission.SCHEDULE_READ,
        Permission.AUDIT_READ,
        Permission.WORKSPACE_READ,
        Permission.ACCOUNT_SELF,
        Permission.QUERY_USE,
        Permission.AI_USE,
    },
    "admin": _ALL_PERMISSIONS,
}


# Paths that are always public, even when RBAC is enabled.
_PUBLIC_PATHS = {
    "/health",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/api/v1/auth/login",
    "/api/v1/auth/logout",
    "/api/v1/auth/bootstrap",
    "/api/v1/auth/sso/providers",
    "/api/v1/auth/sso/start",
    "/api/v1/auth/sso/callback",
    "/auth/login",
    "/auth/logout",
    "/auth/bootstrap",
    "/auth/sso/providers",
    "/api/v1/transfer/capabilities",
    "/api/v1/transfer/platform",
    "/api/v1/transfer/readiness",
    "/api/v1/catalog",
}


# Ordered list of (method, path_prefix, permission) rules.  The first match wins.
# Method "*" matches any method.
_PATH_RULES: list[tuple[str, str, str]] = [
    ("*", "/api/v1/admin/", Permission.WORKSPACE_MANAGE),
    # Rotating your own password is not workspace administration.
    ("POST", "/api/v1/auth/change-password", Permission.ACCOUNT_SELF),
    ("POST", "/auth/change-password", Permission.ACCOUNT_SELF),
    # Accounts are deployment-level administration. Membership changes inside a
    # workspace are authorized by the *workspace* role in ``services.team_store``
    # (a workspace admin need not be a platform admin), so the middleware only
    # requires membership-level read here and lets the store refuse with a reason.
    ("*", "/api/v1/team/users", Permission.WORKSPACE_MANAGE),
    ("*", "/api/v1/team/workspaces/", Permission.WORKSPACE_READ),
    ("GET", "/api/v1/team/workspaces", Permission.WORKSPACE_READ),
    ("*", "/api/v1/team/workspaces", Permission.WORKSPACE_MANAGE),
    # Proof ledger is readable by any workspace member; fidelity runs need job.run.
    ("GET", "/api/v1/workspace/proofs/", Permission.WORKSPACE_READ),
    ("POST", "/api/v1/workspace/proofs/", Permission.JOB_RUN),
    # Reading the workspace's own name, timezone and retention is not workspace
    # administration: refusing it left a viewer on a Settings page with nothing
    # honest to show, which the client papered over with invented defaults.
    # Only the secret-bearing reads (SSO certificates, provider keys, API keys,
    # notification targets, BYOK) and the engine choice stay with workspace
    # administration — which engine answers is read behind the same gate that
    # changes it (tests/test_byo_provider_keys.py).
    ("GET", "/api/v1/workspace/settings", Permission.WORKSPACE_READ),
    ("*", "/api/v1/workspace/", Permission.WORKSPACE_MANAGE),
    ("*", "/api/v1/resource-acls", Permission.WORKSPACE_MANAGE),
    ("GET", "/api/v1/audit/", Permission.AUDIT_READ),
    ("POST", "/api/v1/audit/tip/", Permission.WORKSPACE_MANAGE),
    ("GET", "/api/v1/cdc/mapping-reviews", Permission.JOB_READ),
    ("POST", "/api/v1/cdc/mapping-reviews/", Permission.JOB_MANAGE),
    ("POST", "/api/v1/transfer/run", Permission.JOB_RUN),
    ("*", "/api/v1/transfer/plans/", Permission.JOB_PLAN),
    ("GET", "/api/v1/transfer/", Permission.JOB_READ),
    # Reading a schedule is schedule.read — the permission every role already
    # holds. Requiring schedule.manage to *list* them refused the viewer's own
    # Schedules page, which the client then drew as "No schedules yet" while
    # schedules existed. Creating, changing, running and deciding stay manage.
    ("GET", "/api/v1/schedules/", Permission.SCHEDULE_READ),
    ("*", "/api/v1/schedules/", Permission.SCHEDULE_MANAGE),
    ("GET", "/api/v1/audit/", Permission.AUDIT_READ),
    ("*", "/api/v1/ai/", Permission.AI_USE),
    # Pilot. Talking to the assistant is ai.use for every role; what the turn is
    # allowed to *do* is decided per tool (src/ai/copilot/tool_permissions.py),
    # so a viewer can ask questions but cannot reach a mutating tool. Confirm
    # re-checks the permission of the specific staged mutation. Training rewrites
    # workspace-wide knowledge, so it stays with workspace administration.
    ("POST", "/api/v1/copilot/train", Permission.WORKSPACE_MANAGE),
    ("*", "/api/v1/copilot/", Permission.AI_USE),
    # MCP tool execution is an AI surface — same permission as Pilot tools.
    ("POST", "/api/v1/mcp/tools/call", Permission.AI_USE),
    ("GET", "/api/v1/mcp/logs", Permission.AI_USE),
    ("*", "/api/v1/mcp/", Permission.AI_USE),
    ("GET", "/api/v1/connectors/", Permission.CONNECTOR_READ),
    ("*", "/api/v1/connectors/", Permission.CONNECTOR_WRITE),
    ("*", "/api/v1/query/", Permission.QUERY_USE),
    ("GET", "/api/v1/jobs/", Permission.JOB_READ),
    ("POST", "/api/v1/jobs/", Permission.JOB_MANAGE),
]


def normalize_role(role: str | None) -> str:
    """Map role labels to the closed set {admin, editor, operator, viewer}.

    Phase D5 — unknown / legacy labels (including ``Workspace tester``) fail
    closed to **viewer**, never escalate to editor.
    """
    if not role:
        return "viewer"
    role = str(role).strip().lower()
    if role in ("admin", "editor", "operator", "viewer"):
        return role
    return "viewer"


def role_permissions(role: str) -> set[str]:
    return _ROLE_PERMISSIONS.get(normalize_role(role), _ROLE_PERMISSIONS["viewer"])


def has_permission(user: dict[str, str] | None, permission: str) -> bool:
    if not user:
        return False
    role = normalize_role(user.get("role"))
    return permission in role_permissions(role)


def _is_public_mcp_path(path: str) -> bool:
    """Discovery + Streamable handshake only — not ``/tools/call`` or ``/logs``."""
    if path in ("/api/v1/mcp", "/api/v1/mcp/"):
        return True
    if path.startswith("/api/v1/mcp/manifest") or path.startswith("/api/v1/mcp/status"):
        return True
    if path.rstrip("/") == "/api/v1/mcp/tools":
        return True
    return False


def _is_public_path(path: str) -> bool:
    if path in _PUBLIC_PATHS:
        return True
    for prefix in ("/api/v1/auth/sso/", "/auth/sso/", "/api/v1/catalog/"):
        if path.startswith(prefix):
            return True
    if _is_public_mcp_path(path):
        return True
    return False


def _required_permission(method: str, path: str) -> str | None:
    if _is_public_path(path):
        return None
    for rule_method, prefix, permission in _PATH_RULES:
        if rule_method != "*" and method != rule_method:
            continue
        if path.startswith(prefix):
            return permission
    # Default: GET is read, mutations require editor-level write.
    if method == "GET":
        return Permission.JOB_READ
    if method in ("POST", "PUT", "PATCH", "DELETE"):
        return Permission.CONNECTOR_WRITE
    return None


class RBACMiddleware(BaseHTTPMiddleware):
    """Enforce role-based permissions for authenticated API requests."""

    async def dispatch(self, request: Request, call_next):
        # RBAC only matters when authentication is enforced. Read it off the
        # auth module per request rather than copying the symbol at import
        # time, so RBAC can never enforce against a stale view of the setting.
        if not _auth_service.auth_required():
            return await call_next(request)

        path = request.url.path
        if _is_public_path(path):
            return await call_next(request)

        permission = _required_permission(request.method, path)
        if permission is None:
            return await call_next(request)

        user = getattr(request.state, "user", None)
        # Imported here, not at module import: the resolver reads this module's
        # role table, so a module-level import would be circular.
        from services.effective_role import (
            resolve_effective_role,
            workspace_id_from_request_headers,
        )

        workspace_id = workspace_id_from_request_headers(request.headers)
        effective = resolve_effective_role(user, workspace_id)
        request.state.effective_role = effective
        if permission in role_permissions(effective):
            return await call_next(request)

        return JSONResponse(
            status_code=403,
            content={
                "detail": f"Permission denied: {permission}",
                "required_permission": permission,
                "effective_role": effective,
            },
        )
