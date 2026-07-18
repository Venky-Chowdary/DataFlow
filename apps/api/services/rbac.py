"""Role-based access control for the DataFlow API.

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

from services.auth_service import auth_required


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
    AUDIT_READ = "audit.read"
    WORKSPACE_READ = "workspace.read"
    WORKSPACE_MANAGE = "workspace.manage"
    AI_USE = "ai.use"
    QUERY_USE = "query.use"


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
    Permission.AUDIT_READ,
    Permission.WORKSPACE_READ,
    Permission.WORKSPACE_MANAGE,
    Permission.AI_USE,
    Permission.QUERY_USE,
}


_ROLE_PERMISSIONS: dict[str, set[str]] = {
    "viewer": {
        Permission.JOB_READ,
        Permission.CONNECTOR_READ,
        Permission.SCHEDULE_READ,
        Permission.AUDIT_READ,
        Permission.WORKSPACE_READ,
        Permission.QUERY_USE,
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
        Permission.QUERY_USE,
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
    "/api/v1/auth/bootstrap",
    "/api/v1/auth/sso/providers",
    "/api/v1/auth/sso/start",
    "/api/v1/auth/sso/callback",
    "/api/v1/transfer/capabilities",
    "/api/v1/transfer/platform",
    "/api/v1/transfer/readiness",
    "/api/v1/catalog",
}


# Ordered list of (method, path_prefix, permission) rules.  The first match wins.
# Method "*" matches any method.
_PATH_RULES: list[tuple[str, str, str]] = [
    ("*", "/api/v1/admin/", Permission.WORKSPACE_MANAGE),
    # Proof ledger is readable by any workspace member; fidelity runs need job.run.
    ("GET", "/api/v1/workspace/proofs/", Permission.WORKSPACE_READ),
    ("POST", "/api/v1/workspace/proofs/", Permission.JOB_RUN),
    ("*", "/api/v1/workspace/", Permission.WORKSPACE_MANAGE),
    ("GET", "/api/v1/audit/", Permission.AUDIT_READ),
    ("POST", "/api/v1/transfer/run", Permission.JOB_RUN),
    ("*", "/api/v1/transfer/plans/", Permission.JOB_PLAN),
    ("GET", "/api/v1/transfer/", Permission.JOB_READ),
    ("*", "/api/v1/schedules/", Permission.SCHEDULE_MANAGE),
    ("GET", "/api/v1/audit/", Permission.AUDIT_READ),
    ("*", "/api/v1/ai/", Permission.AI_USE),
    ("GET", "/api/v1/connectors/", Permission.CONNECTOR_READ),
    ("*", "/api/v1/connectors/", Permission.CONNECTOR_WRITE),
    ("*", "/api/v1/query/", Permission.QUERY_USE),
    ("GET", "/api/v1/jobs/", Permission.JOB_READ),
    ("POST", "/api/v1/jobs/", Permission.JOB_MANAGE),
]


def normalize_role(role: str | None) -> str:
    if not role:
        return "viewer"
    role = str(role).strip().lower()
    if role in ("admin", "editor", "operator", "viewer"):
        return role
    # Map legacy/test roles to editor so the dev user keeps working.
    if "tester" in role or "test" in role:
        return "editor"
    return "viewer"


def role_permissions(role: str) -> set[str]:
    return _ROLE_PERMISSIONS.get(normalize_role(role), _ROLE_PERMISSIONS["viewer"])


def has_permission(user: dict[str, str] | None, permission: str) -> bool:
    if not user:
        return False
    role = normalize_role(user.get("role"))
    return permission in role_permissions(role)


def _is_public_path(path: str) -> bool:
    if path in _PUBLIC_PATHS:
        return True
    for prefix in ("/api/v1/auth/sso/", "/api/v1/catalog/", "/api/v1/mcp"):
        if path.startswith(prefix):
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
        # RBAC only matters when authentication is enforced.
        if not auth_required():
            return await call_next(request)

        path = request.url.path
        if _is_public_path(path):
            return await call_next(request)

        permission = _required_permission(request.method, path)
        if permission is None:
            return await call_next(request)

        user = getattr(request.state, "user", None)
        if has_permission(user, permission):
            return await call_next(request)

        return JSONResponse(
            status_code=403,
            content={"detail": f"Permission denied: {permission}"},
        )
