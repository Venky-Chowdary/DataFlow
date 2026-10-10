"""Role-based access control for the Datawrap API.

Permission model (enterprise-friendly):

- viewer:  read jobs, connectors, schedules, audit, workspace.
- editor:  viewer + run transfers, manage connectors, schedules, plans, and
           invite non-admin members to the workspace (granting admin is not).
- admin:   editor + workspace administration, user management, settings.

Unknown roles and the dev "Workspace tester" role map to editor so development
is not blocked, but production must still gate based on the actual role claim.
"""

from __future__ import annotations

import logging
import os
import re
import threading
from typing import Any
from weakref import WeakKeyDictionary

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount

from src.services import auth_service as _auth_service

logger = logging.getLogger(__name__)


class RBACConfigError(ValueError):
    """Invalid RBAC environment configuration."""


class _NoRule:
    __slots__ = ()

    def __repr__(self) -> str:
        return "NO_RULE"


NO_RULE = _NoRule()

UNRULED_ROUTES_ENV = "DATAFLOW_RBAC_UNRULED_ROUTES"


def _parse_unruled_route_mode(value: str | None) -> str:
    mode = "deny" if value is None else value.strip().lower()
    if mode not in {"deny", "allow_and_log"}:
        raise RBACConfigError(
            f"Invalid {UNRULED_ROUTES_ENV} value {value!r}; expected 'deny' or "
            "'allow_and_log'."
        )
    return mode


def _load_unruled_route_mode(value: str | None) -> str:
    try:
        return _parse_unruled_route_mode(value)
    except RBACConfigError as exc:
        logger.error("%s", exc)
        return "deny"


UNRULED_ROUTE_MODE = _load_unruled_route_mode(os.getenv(UNRULED_ROUTES_ENV))
_UNRULED_STARTUP_RECORDED = False
_UNRULED_STARTUP_LOCK = threading.Lock()
_APP_ROUTE_CACHE: WeakKeyDictionary[object, tuple[tuple[frozenset[str], re.Pattern[str]], ...]] = (
    WeakKeyDictionary()
)
_APP_ROUTE_CACHE_LOCK = threading.Lock()


def record_unruled_routes_allowed_startup() -> None:
    """Warn and audit once per process when legacy unruled fallback is enabled."""
    global _UNRULED_STARTUP_RECORDED
    if UNRULED_ROUTE_MODE != "allow_and_log":
        return
    with _UNRULED_STARTUP_LOCK:
        if _UNRULED_STARTUP_RECORDED:
            return
        _UNRULED_STARTUP_RECORDED = True
    logger.warning(
        "%s=allow_and_log preserves legacy permission fallbacks for unruled routes",
        UNRULED_ROUTES_ENV,
    )
    try:
        from services import audit_log

        audit_log.append_audit_event(
            action="authz.config.unruled_routes_allowed",
            actor="system",
            resource=UNRULED_ROUTES_ENV,
            details={"mode": "allow_and_log", "environment_variable": UNRULED_ROUTES_ENV},
        )
    except Exception as exc:
        logger.error(
            "Unruled-route startup audit failed (%s)",
            type(exc).__name__,
            exc_info=exc,
        )

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
    # Bringing a peer into the workspace you already work in. Held by editor as
    # well as admin, because ``services.team_store.add_workspace_member`` accepts
    # an editor adding a non-admin member — a client gating membership on
    # workspace.manage disabled the control the API would have honoured, and told
    # an editor to ask for the role it already had.
    MEMBER_INVITE = "member.invite"
    AI_USE = "ai.use"
    QUERY_USE = "query.use"
    IAM_MANAGE = "iam.manage"
    SCIM_PROVISION = "scim.provision"
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
    Permission.MEMBER_INVITE,
    Permission.AI_USE,
    Permission.QUERY_USE,
    Permission.ACCOUNT_SELF,
    Permission.IAM_MANAGE,
    Permission.SCIM_PROVISION,
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
        Permission.MEMBER_INVITE,
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
    "/.well-known/oauth-protected-resource",
    "/.well-known/oauth-protected-resource/api/v1/mcp",
}


# Ordered list of (method, path_prefix, permission) rules.  The first match wins.
# Method "*" matches any method.
_PATH_RULES: list[tuple[str, str, str]] = [
    ("*", "/api/v1/iam/", Permission.IAM_MANAGE),
    ("*", "/api/v1/scim/v2", Permission.SCIM_PROVISION),
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
    # Comparing a live source against its destination is a *run* of that pipeline,
    # not a change to a connector. Falling through to the mutation default gave it
    # connector.write, which refused the operator whose whole role is to run and
    # reconcile pipelines, while the UI control stayed enabled.
    ("POST", "/api/v1/fidelity/", Permission.JOB_RUN),
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
    ("POST", "/api/v1/audit/retention/purge", Permission.WORKSPACE_MANAGE),
    ("GET", "/api/v1/audit/", Permission.AUDIT_READ),
    ("POST", "/api/v1/audit/tip/", Permission.WORKSPACE_MANAGE),
    ("GET", "/api/v1/cdc/mapping-reviews", Permission.JOB_READ),
    ("POST", "/api/v1/cdc/mapping-reviews/", Permission.JOB_MANAGE),
    # Transform (pre-load). Reading the vocabulary is a read every role holds, so
    # a viewer's Transform step renders the real operations it cannot apply
    # instead of an empty panel. Profiling is also a read: it only describes rows
    # the caller sent, touches no route and composes no recipe, so a viewer sees
    # the same findings it is being told it may inspect. Previewing and validating
    # mint a recipe identity that Execute is held to — that is design work on a
    # route, gated by the permission that plans a transfer (an operator runs
    # approved plans, it does not author the recipe inside one).
    ("GET", "/api/v1/shape/catalog", Permission.JOB_READ),
    ("POST", "/api/v1/shape/profile", Permission.JOB_READ),
    ("*", "/api/v1/shape/", Permission.JOB_PLAN),
    ("POST", "/api/v1/transfer/run", Permission.JOB_RUN),
    ("POST", "/api/v1/transfer/rules/import", Permission.JOB_PLAN),
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
    ("*", "/api/v1/mcp/policy", Permission.WORKSPACE_MANAGE),
    ("*", "/api/v1/mcp/", Permission.AI_USE),
    ("GET", "/api/v1/connectors/", Permission.CONNECTOR_READ),
    ("*", "/api/v1/connectors/", Permission.CONNECTOR_WRITE),
    ("*", "/api/v1/query/", Permission.QUERY_USE),
    # Explicit rules for routes that previously relied on method fallbacks.
    # Keep these after specific policy entries so the latter retain precedence.
    ("POST", "/api/v1/audit/", Permission.CONNECTOR_WRITE),
    ("GET", "/api/v1/auth/", Permission.JOB_READ),
    ("GET", "/api/v1/automations/", Permission.JOB_READ),
    ("GET", "/api/v1/cdc/", Permission.JOB_READ),
    ("POST", "/api/v1/cdc/", Permission.CONNECTOR_WRITE),
    ("GET", "/api/v1/contracts/", Permission.JOB_READ),
    ("POST", "/api/v1/contracts/", Permission.CONNECTOR_WRITE),
    ("GET", "/api/v1/ops/", Permission.JOB_READ),
    ("POST", "/api/v1/ops/", Permission.CONNECTOR_WRITE),
    ("GET", "/api/v1/preflight/", Permission.JOB_READ),
    ("POST", "/api/v1/preflight/", Permission.CONNECTOR_WRITE),
    ("GET", "/api/v1/repair/", Permission.JOB_READ),
    ("POST", "/api/v1/repair/", Permission.CONNECTOR_WRITE),
    ("GET", "/api/v1/training-agent/", Permission.JOB_READ),
    ("POST", "/api/v1/training-agent/", Permission.CONNECTOR_WRITE),
    ("POST", "/api/v1/transfer/", Permission.CONNECTOR_WRITE),
    ("GET", "/api/v1/transforms/", Permission.JOB_READ),
    ("POST", "/api/v1/transforms/", Permission.CONNECTOR_WRITE),
    ("PATCH", "/api/v1/transforms/", Permission.CONNECTOR_WRITE),
    ("DELETE", "/api/v1/transforms/", Permission.CONNECTOR_WRITE),
    ("GET", "/api/v1/usage/", Permission.JOB_READ),
    ("GET", "/auth/", Permission.JOB_READ),
    ("GET", "/docs/", Permission.JOB_READ),
    ("GET", "/health/", Permission.JOB_READ),
    ("GET", "/metrics", Permission.JOB_READ),
]

# Exact method/path entries preserve fallback behavior on root and special
# paths that cannot be represented safely by a prefix.
_EXACT_PATH_RULES: tuple[tuple[str, str, str | None], ...] = (
    ("GET", "/", Permission.JOB_READ),
    ("GET", "/api/v1", Permission.JOB_READ),
    ("GET", "/api/v1/health", Permission.JOB_READ),
    ("GET", "/api/v1/contracts", Permission.JOB_READ),
    ("POST", "/api/v1/contracts", Permission.CONNECTOR_WRITE),
)


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


def principal_permissions(user: dict[str, Any] | None, role: str) -> set[str]:
    permissions = role_permissions(role)
    scopes = user.get("scopes") if user else None
    if isinstance(scopes, list):
        return permissions & {scope for scope in scopes if isinstance(scope, str)}
    return permissions


class PrivilegeEscalation(PermissionError):
    def __init__(self, missing: set[str]):
        self.missing = tuple(sorted(missing))
        super().__init__(", ".join(self.missing))


def assert_grant_within(
    caller_permissions: set[str],
    granted_permissions: set[str],
) -> None:
    missing = granted_permissions - caller_permissions
    if missing:
        raise PrivilegeEscalation(missing)


def role_names() -> tuple[str, ...]:
    """The closed role set, least to most authority.

    Public so surfaces that have to *describe* the model — Pilot answering "what
    can a viewer do" — read the same table the middleware enforces, instead of
    prose that drifts from it.
    """
    return ("viewer", "operator", "editor", "admin")


def all_permissions() -> tuple[str, ...]:
    """Every permission the role table can grant, sorted for stable rendering."""
    return tuple(sorted(_ALL_PERMISSIONS))


def has_permission(user: dict[str, Any] | None, permission: str) -> bool:
    if not user:
        return False
    role = normalize_role(user.get("role"))
    return permission in principal_permissions(user, role)


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


def _fallback_permission(method: str) -> str | None:
    if method == "GET":
        return Permission.JOB_READ
    if method in ("POST", "PUT", "PATCH", "DELETE"):
        return Permission.CONNECTOR_WRITE
    return None


def _join_route_path(prefix: str, path: str) -> str:
    if not prefix:
        return path
    if prefix.endswith("/") and path.startswith("/"):
        return prefix + path[1:]
    return prefix + path


def _walk_route_patterns(routes: Any, prefix: str = ""):
    for route in routes:
        if isinstance(route, Mount):
            children = getattr(route, "routes", None)
            if children is None:
                children = getattr(getattr(route, "app", None), "routes", ())
            yield from _walk_route_patterns(
                children or (), _join_route_path(prefix, route.path)
            )
            continue

        if type(route).__name__ == "_IncludedRouter":
            context = getattr(route, "include_context", None)
            nested_prefix = getattr(context, "prefix", "") or ""
            original_router = getattr(route, "original_router", None)
            yield from _walk_route_patterns(
                getattr(original_router, "routes", ()),
                _join_route_path(prefix, nested_prefix),
            )
            continue

        template = getattr(route, "path", None)
        if template is None:
            continue
        methods = getattr(route, "methods", None) or {"GET"}
        for method in methods:
            yield method.upper(), _join_route_path(prefix, template)


def _compile_route_template(template: str) -> re.Pattern[str]:
    parts = re.split(r"(\{[^{}]+\})", template)
    pattern = "".join(
        ".*"
        if part.startswith("{")
        and part.endswith("}")
        and part[1:-1].endswith(":path")
        else (
            "[^/]+"
            if part.startswith("{") and part.endswith("}")
            else re.escape(part)
        )
        for part in parts
    )
    return re.compile(f"^{pattern}$")


def _app_route_patterns(
    app: object,
) -> tuple[tuple[frozenset[str], re.Pattern[str]], ...]:
    with _APP_ROUTE_CACHE_LOCK:
        cached = _APP_ROUTE_CACHE.get(app)
        if cached is not None:
            return cached
        patterns = tuple(
            (frozenset({method}), _compile_route_template(template))
            for method, template in _walk_route_patterns(
                getattr(app, "routes", ())
            )
        )
        _APP_ROUTE_CACHE[app] = patterns
        return patterns


def _request_matches_real_route(app: object, method: str, path: str) -> bool:
    method = "GET" if method.upper() == "HEAD" else method.upper()
    return any(
        method in methods and pattern.fullmatch(path)
        for methods, pattern in _app_route_patterns(app)
    )


def _required_permission(method: str, path: str) -> str | _NoRule | None:
    method = "GET" if method.upper() == "HEAD" else method.upper()
    if method == "POST" and path == "/api/v1/mcp/tools/call":
        return None
    if _is_public_path(path):
        return None
    for rule_method, exact_path, permission in _EXACT_PATH_RULES:
        if method == rule_method and path == exact_path:
            return permission
    for rule_method, prefix, permission in _PATH_RULES:
        if rule_method != "*" and method != rule_method:
            continue
        if path.startswith(prefix):
            return permission
    return NO_RULE


class RBACMiddleware(BaseHTTPMiddleware):
    """Enforce role-based permissions for authenticated API requests."""

    async def dispatch(self, request: Request, call_next):
        # RBAC only matters when authentication is enforced. Read it off the
        # auth module per request rather than copying the symbol at import
        # time, so RBAC can never enforce against a stale view of the setting.
        if not _auth_service.auth_required():
            return await call_next(request)

        path = request.url.path
        method = request.method.upper()
        if _is_public_path(path) or (
            method == "POST" and path == "/api/v1/mcp/tools/call"
        ):
            return await call_next(request)

        route_method = "GET" if method == "HEAD" else method
        if _request_matches_real_route(request.app, route_method, path):
            permission = _required_permission(route_method, path)
        else:
            permission = NO_RULE
        user = getattr(request.state, "user", None)

        if permission is NO_RULE:
            if UNRULED_ROUTE_MODE == "allow_and_log":
                suppressed = None
                try:
                    from services.audit_coverage import (
                        unruled_route_suppressed_since_last,
                    )

                    suppressed = unruled_route_suppressed_since_last(request)
                except Exception as exc:
                    logger.error(
                        "Unruled-route warning dedupe failed (%s)",
                        type(exc).__name__,
                        exc_info=exc,
                    )
                logger.warning(
                    "Unruled RBAC route %s %s allowed by %s "
                    "(deduplicated=%s, suppressed_since_last=%s)",
                    request.method.upper(),
                    path,
                    UNRULED_ROUTES_ENV,
                    suppressed is None,
                    suppressed,
                )
                permission = _fallback_permission(request.method.upper())
            else:
                from services.effective_role import (
                    resolve_effective_role,
                    workspace_id_from_request_headers,
                )

                effective = resolve_effective_role(
                    user, workspace_id_from_request_headers(request.headers)
                )
                request.state.effective_role = effective
                try:
                    from services.audit_coverage import record_authz_denial

                    record_authz_denial(
                        request,
                        required_permission="no_rule",
                        effective_role=effective,
                        reason="no_rule",
                    )
                except Exception as exc:
                    logger.error(
                        "RBAC no-rule denial audit failed (%s)",
                        type(exc).__name__,
                        exc_info=exc,
                    )
                return JSONResponse(
                    status_code=403,
                    content={
                        "detail": "No RBAC rule is configured for this route.",
                        "reason": "no_rule",
                        "required_permission": "no_rule",
                        "effective_role": effective,
                    },
                )

        if permission is None:
            return await call_next(request)

        # Imported here, not at module import: the resolver reads this module's
        # role table, so a module-level import would be circular.
        from services.effective_role import (
            resolve_effective_role,
            workspace_id_from_request_headers,
        )

        workspace_id = workspace_id_from_request_headers(request.headers)
        effective = resolve_effective_role(user, workspace_id)
        request.state.effective_role = effective
        if permission in principal_permissions(user, effective):
            return await call_next(request)

        try:
            from services.audit_coverage import record_authz_denial

            record_authz_denial(
                request,
                required_permission=permission,
                effective_role=effective,
            )
        except Exception as exc:
            # Auditing must not change the authorization decision or response.
            import logging

            logging.getLogger(__name__).error(
                "RBAC denial audit failed (%s)", type(exc).__name__, exc_info=exc
            )
        return JSONResponse(
            status_code=403,
            content={
                "detail": f"Permission denied: {permission}",
                "required_permission": permission,
                "effective_role": effective,
            },
        )
