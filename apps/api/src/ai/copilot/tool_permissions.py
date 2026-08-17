"""Which role may run which Pilot tool.

Pilot is a product surface, not a side channel. The rule this module enforces is
that **a tool is governed by the same permission as the REST route that performs
the same operation** — asking Pilot to run a pipeline is `schedule.manage`
exactly as `POST /api/v1/schedules/{id}/run` is, and a viewer who cannot start a
transfer through Transfer Studio cannot start one by typing it in chat either.

Two properties matter more than the table itself:

* A tool with no entry is **admin-only**, so a new tool cannot ship ungated by
  omission. ``test_pilot_tool_permissions`` asserts the table covers the whole
  registry, so the omission is caught in CI rather than in production.
* The caller's role is bound per turn through a :class:`~contextvars.ContextVar`
  rather than stored on the (process-wide) agent, so two concurrent operators
  can never inherit each other's permissions. :func:`bind_current_context` keeps
  that binding when a turn fans out to a worker thread.
"""

from __future__ import annotations

import contextlib
import contextvars
from collections.abc import Callable, Iterator
from typing import Any, TypeVar

from services.rbac import Permission, normalize_role, role_permissions

# What running the tool does, in the operator's terms. "plan" reads the source
# and produces a proposal; only "mutate" can change workspace state, and every
# mutating tool additionally stages a Confirm ack before anything happens.
READ = "read"
PLAN = "plan"
MUTATE = "mutate"


#: tool name -> (required permission, effect)
TOOL_PERMISSIONS: dict[str, tuple[str, str]] = {
    # Uploaded datasets and product knowledge — no workspace state involved.
    "list_datasets": (Permission.WORKSPACE_READ, READ),
    "analyze_dataset": (Permission.WORKSPACE_READ, READ),
    "search_data": (Permission.WORKSPACE_READ, READ),
    "compare_datasets": (Permission.WORKSPACE_READ, READ),
    "search_knowledge": (Permission.WORKSPACE_READ, READ),
    "describe_pilot": (Permission.WORKSPACE_READ, READ),
    "explain_product": (Permission.WORKSPACE_READ, READ),
    "get_transfer_capabilities": (Permission.WORKSPACE_READ, READ),
    "explain_mapping_assurance": (Permission.WORKSPACE_READ, READ),
    "recommend_sync_mode": (Permission.WORKSPACE_READ, READ),
    "profile_quality_rules": (Permission.WORKSPACE_READ, READ),
    # Navigation only opens a screen; the screen enforces its own permissions.
    "navigate": (Permission.WORKSPACE_READ, READ),
    "open_job": (Permission.JOB_READ, READ),
    "open_schedule": (Permission.SCHEDULE_READ, READ),
    "start_transfer_studio": (Permission.WORKSPACE_READ, READ),
    # Connector metadata.
    "list_connectors": (Permission.CONNECTOR_READ, READ),
    "search_connectors": (Permission.CONNECTOR_READ, READ),
    "list_connector_objects": (Permission.CONNECTOR_READ, READ),
    "introspect_connector_schema": (Permission.CONNECTOR_READ, READ),
    "diff_schemas": (Permission.CONNECTOR_READ, READ),
    "map_connector_schemas": (Permission.CONNECTOR_READ, READ),
    "inspect_schema_policy": (Permission.CONNECTOR_READ, READ),
    # Live data reads mirror /api/v1/query.
    "sample_connector_object": (Permission.QUERY_USE, READ),
    "run_query": (Permission.QUERY_USE, READ),
    "aggregate_data": (Permission.QUERY_USE, READ),
    "analyze_result": (Permission.QUERY_USE, READ),
    "filter_result": (Permission.QUERY_USE, READ),
    # Jobs and proof reads.
    "list_jobs": (Permission.JOB_READ, READ),
    "get_job": (Permission.JOB_READ, READ),
    "get_preflight_run": (Permission.JOB_READ, READ),
    "list_contracts": (Permission.JOB_READ, READ),
    "list_schedules": (Permission.SCHEDULE_READ, READ),
    "get_schedule": (Permission.SCHEDULE_READ, READ),
    # Planning mirrors /api/v1/transfer/plans.
    "plan_transfer": (Permission.JOB_PLAN, PLAN),
    "plan_transfer_route": (Permission.JOB_PLAN, PLAN),
    "remediate_validation": (Permission.JOB_PLAN, PLAN),
    # Mutations. Each of these also stages a Confirm ack — permission decides
    # whether the operator may stage it at all.
    "start_transfer": (Permission.JOB_RUN, MUTATE),
    "create_connector": (Permission.CONNECTOR_WRITE, MUTATE),
    "run_schedule_now": (Permission.SCHEDULE_MANAGE, MUTATE),
    "create_schedule": (Permission.SCHEDULE_MANAGE, MUTATE),
}


#: Ack ledger kind -> permission required to *confirm* it. Staging and
#: confirming are separate requests, so the permission is re-checked on Confirm:
#: a role change between the two must take effect immediately.
ACK_KIND_PERMISSIONS: dict[str, str] = {
    "start_transfer": Permission.JOB_RUN,
    "create_connector": Permission.CONNECTOR_WRITE,
    "run_schedule": Permission.SCHEDULE_MANAGE,
    "create_schedule": Permission.SCHEDULE_MANAGE,
}


#: Plain-language name for each permission, for refusal messages. Operators do
#: not know what ``schedule.manage`` is.
_PERMISSION_WORDS: dict[str, str] = {
    Permission.JOB_READ: "view jobs",
    Permission.JOB_RUN: "run transfers",
    Permission.JOB_PLAN: "plan transfers",
    Permission.JOB_MANAGE: "manage jobs",
    Permission.CONNECTOR_READ: "view connectors",
    Permission.CONNECTOR_WRITE: "create or edit connectors",
    Permission.CONNECTOR_DELETE: "delete connectors",
    Permission.SCHEDULE_READ: "view pipelines",
    Permission.SCHEDULE_MANAGE: "create or run pipelines",
    Permission.AUDIT_READ: "read the audit log",
    Permission.WORKSPACE_READ: "read this workspace",
    Permission.WORKSPACE_MANAGE: "administer this workspace",
    Permission.AI_USE: "use AI features",
    Permission.QUERY_USE: "query data",
}

#: Roles that can grant what a denied caller is missing, so the refusal ends in
#: an action instead of a wall.
_GRANTING_ROLE = "an editor or admin"

#: Every refusal opens with this, so other layers can recognise a permission
#: refusal without parsing prose and never mistake it for a connector or table
#: problem (which would send the operator chasing the wrong fix).
DENIAL_PREFIX = "Your role"

_UNKNOWN_TOOL = (Permission.WORKSPACE_MANAGE, MUTATE)

_caller_role: contextvars.ContextVar[str] = contextvars.ContextVar(
    "pilot_caller_role", default=""
)

T = TypeVar("T")


def tool_requirement(name: str) -> tuple[str, str]:
    """Return ``(permission, effect)`` for a tool. Unknown tools are admin-only."""
    return TOOL_PERMISSIONS.get(name, _UNKNOWN_TOOL)


def set_caller_role(role: str) -> contextvars.Token[str]:
    """Bind the caller's role for this turn. ``""`` means "unauthenticated"."""
    return _caller_role.set(normalize_role(role) if role else "")


def reset_caller_role(token: contextvars.Token[str]) -> None:
    _caller_role.reset(token)


def current_caller_role() -> str:
    return _caller_role.get()


@contextlib.contextmanager
def caller_role(role: str) -> Iterator[None]:
    """Bind ``role`` for the duration of one request, then restore it."""
    token = set_caller_role(role)
    try:
        yield
    finally:
        reset_caller_role(token)


def bind_current_context(fn: Callable[..., T]) -> Callable[..., T]:
    """Wrap ``fn`` so a worker thread runs it inside the caller's context.

    ``ThreadPoolExecutor`` does not propagate context variables, so an LLM tool
    loop dispatched to the pool would otherwise run with no bound role — which
    reads as "unauthenticated" and skips the permission check.
    """
    ctx = contextvars.copy_context()

    def _run(*args: Any, **kwargs: Any) -> T:
        return ctx.run(fn, *args, **kwargs)

    return _run


def is_tool_allowed(role: str, name: str) -> bool:
    """Whether ``role`` may run ``name``. An empty role skips the check.

    An empty role is the unauthenticated single-operator deployment, where the
    API itself is open (``auth_required()`` is false) and there is no identity to
    gate on. The router only binds a role when authentication is enforced, so
    this is the same posture the REST routes take.
    """
    if not role:
        return True
    permission, _effect = tool_requirement(name)
    return permission in role_permissions(role)


def allowed_tools(role: str) -> set[str]:
    """Every tool ``role`` may run — used to filter a plan before it executes."""
    if not role:
        return set(TOOL_PERMISSIONS)
    granted = role_permissions(role)
    return {
        name
        for name, (permission, _effect) in TOOL_PERMISSIONS.items()
        if permission in granted
    }


def denial_message(role: str, name: str) -> str:
    """Say what the role may not do, and who can do it — never a bare 403."""
    permission, effect = tool_requirement(name)
    need = _PERMISSION_WORDS.get(permission, permission)
    role_l = normalize_role(role)
    can = sorted(
        _PERMISSION_WORDS[p]
        for p in role_permissions(role_l)
        if p in _PERMISSION_WORDS
    )
    allowed_words = ", ".join(can[:4]) or "nothing in this workspace"
    verb = "change anything" if effect == MUTATE else "run that"
    return (
        f"{DENIAL_PREFIX} (**{role_l}**) cannot {verb}: “{name}” needs permission to "
        f"{need}. You can {allowed_words}. Ask {_GRANTING_ROLE} to run it, or to "
        f"grant you that permission in Settings ▸ Workspace."
    )


def is_permission_denial(error: str) -> bool:
    """True when a tool error is a permission refusal rather than a data problem."""
    return (error or "").lstrip().startswith(DENIAL_PREFIX)


def can_confirm_kind(role: str, kind: str) -> bool:
    """Whether ``role`` may consume an ack of this kind."""
    if not role:
        return True
    permission = ACK_KIND_PERMISSIONS.get(kind)
    if permission is None:
        # An unmapped kind is refused rather than allowed: Confirm is the last
        # gate before a write, so an unknown mutation must never pass it.
        return False
    return permission in role_permissions(role)


def confirm_denial_message(role: str, kind: str) -> str:
    permission = ACK_KIND_PERMISSIONS.get(kind, Permission.WORKSPACE_MANAGE)
    need = _PERMISSION_WORDS.get(permission, permission)
    return (
        f"{DENIAL_PREFIX} ({normalize_role(role)}) cannot confirm this: it needs "
        f"permission to {need}."
    )
