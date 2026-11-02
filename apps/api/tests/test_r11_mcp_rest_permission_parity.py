"""RB6 (QA handover, build 9c75ada4): every MCP tool against its REST twin.

QA: a viewer key could run ``test_connector`` over MCP while
``POST /connectors/saved/{id}/test`` refused it. ``tool_permissions`` mapped the
tool to ``connector.read``; the REST route is ``connector.write``. The module's
own rule is "a tool is governed by the same permission as the REST route that
performs the same operation", but nothing checked it, so it drifted.

This walks the whole tool registry. Each tool is either:

* paired with the REST route that performs the same operation — the set of
  roles the tool's permission admits must equal the set the route's RBAC rule
  admits (a tool may be stricter only where listed in ``STRICTER`` with the
  reason), and is never wider;
* a Pilot-only mutation, confirmed through ``POST /copilot/confirm`` — its
  ack-kind permission must equal the tool's; or
* a read with no REST twin (product knowledge, navigation, chat-local
  analysis) — it must be a read that every workspace member holds anyway.

A tool missing from all three tables fails the test, so a new tool cannot ship
without a decided REST counterpart.
"""

from __future__ import annotations

import pytest

#: tool -> (method, concrete path of the REST route doing the same thing)
REST_TWIN: dict[str, tuple[str, str]] = {
    "list_datasets": ("GET", "/api/v1/copilot/datasets"),
    "list_connectors": ("GET", "/api/v1/connectors/saved"),
    "search_connectors": ("GET", "/api/v1/connectors/saved"),
    "list_connector_objects": ("POST", "/api/v1/query/schemas"),
    "introspect_connector_schema": ("POST", "/api/v1/query/schema"),
    "sample_connector_object": ("POST", "/api/v1/query/execute"),
    "rank_connector_tables": ("POST", "/api/v1/query/schemas"),
    "run_query": ("POST", "/api/v1/query/execute"),
    "list_jobs": ("GET", "/api/v1/jobs"),
    "get_job": ("GET", "/api/v1/jobs/job-1"),
    "open_job": ("GET", "/api/v1/jobs/job-1"),
    "get_preflight_run": ("GET", "/api/v1/preflight/runs/run-1"),
    "list_contracts": ("GET", "/api/v1/contracts"),
    "list_schedules": ("GET", "/api/v1/schedules/"),
    "get_schedule": ("GET", "/api/v1/schedules/s-1"),
    "open_schedule": ("GET", "/api/v1/schedules/s-1"),
    "plan_transfer": ("POST", "/api/v1/transfer/plans"),
    "plan_transfer_route": ("POST", "/api/v1/transfer/route"),
    "remediate_validation": ("POST", "/api/v1/repair/propose/preflight"),
    "start_transfer": ("POST", "/api/v1/transfer/execute"),
    "create_connector": ("POST", "/api/v1/connectors/saved"),
    "update_connector": ("PUT", "/api/v1/connectors/saved/c-1"),
    "delete_connector": ("DELETE", "/api/v1/connectors/saved/c-1"),
    "test_connector": ("POST", "/api/v1/connectors/saved/c-1/test"),
    "run_schedule_now": ("POST", "/api/v1/schedules/s-1/run"),
    "create_schedule": ("POST", "/api/v1/schedules/"),
    "set_schedule_enabled": ("PATCH", "/api/v1/schedules/s-1"),
    "update_schedule": ("PATCH", "/api/v1/schedules/s-1"),
    "delete_schedule": ("DELETE", "/api/v1/schedules/s-1"),
    "cancel_job": ("POST", "/api/v1/connectors/jobs/job-1/cancel"),
    "retry_job": ("POST", "/api/v1/connectors/jobs/job-1/retry"),
    "rerun_job": ("POST", "/api/v1/connectors/jobs/job-1/rerun"),
    "resume_job": ("POST", "/api/v1/connectors/jobs/job-1/resume"),
    "replay_quarantine": ("POST", "/api/v1/connectors/jobs/job-1/quarantine/replay"),
    "clear_cursor_resnapshot": ("POST", "/api/v1/ops/cdc-cursors/clear"),
}

#: Tools deliberately stricter than their REST twin, and why.
STRICTER: dict[str, str] = {
    "delete_connector": (
        "Deleting a connector from chat is admin-only (connector.delete); the "
        "REST route is reached only from the Connectors page's own confirm dialog."
    ),
}

#: Tools whose permission follows the role model while the REST route's RBAC
#: rule is narrower — each admits exactly the listed extra roles and no other.
#: Job lifecycle: ``job.manage`` is the role model's "cancel/retry/resume"
#: permission (operators hold it, and Pilot confirms for them by design —
#: tests/test_pilot_lifecycle_tools.py), but ``/connectors/jobs/{id}/...``
#: falls through to the ``/api/v1/connectors/`` prefix rule (connector.write).
#: Not a privilege-escalation class (the operator role exists to run jobs);
#: recorded for an owner decision: give the REST routes job.manage, or narrow
#: Pilot. A NEW divergence fails the test.
ROLE_MODEL_WIDER_THAN_REST: dict[str, set[str]] = {
    tool: {"operator"}
    for tool in ("cancel_job", "retry_job", "rerun_job", "resume_job", "replay_quarantine")
}

#: Pilot-only mutations: no REST twin, confirmed through /copilot/confirm.
PILOT_ONLY_MUTATION: dict[str, str] = {
    "start_dataset_transfer": "start_transfer",
    "prepare_cdc_source": "prepare_cdc_source",
    "repair_cdc_drift": "repair_cdc_drift",
    "rebaseline_transfer": "rebaseline_transfer",
}

#: Reads with no REST twin: product knowledge, navigation, chat-local analysis.
NO_REST_READ: frozenset[str] = frozenset({
    "analyze_dataset", "search_data", "compare_datasets", "search_knowledge",
    "describe_pilot", "describe_connector_transforms", "brief_workspace",
    "explain_product", "get_transfer_capabilities", "explain_mapping_assurance",
    "recommend_sync_mode", "profile_quality_rules", "navigate",
    "start_transfer_studio", "compare_connectors", "diff_schemas",
    "map_connector_schemas", "inspect_schema_policy", "aggregate_data",
    "analyze_result", "filter_result", "confirm_action",
})


def _roles(permission: str) -> set[str]:
    from services.rbac import role_names, role_permissions

    return {r for r in role_names() if permission in role_permissions(r)}


def _registry() -> list[str]:
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    return [t["name"] for t in TOOL_DEFINITIONS]


def test_every_tool_has_a_decided_rest_counterpart():
    decided = set(REST_TWIN) | set(PILOT_ONLY_MUTATION) | NO_REST_READ
    missing = sorted(set(_registry()) - decided)
    assert not missing, f"MCP tools with no REST parity decision: {missing}"
    overlap = (set(REST_TWIN) & NO_REST_READ) | (set(REST_TWIN) & set(PILOT_ONLY_MUTATION))
    assert not overlap


@pytest.mark.parametrize("tool", sorted(REST_TWIN))
def test_tool_admits_exactly_the_roles_its_rest_twin_admits(tool):
    from services.rbac import _required_permission
    from src.ai.copilot.tool_permissions import tool_requirement

    method, path = REST_TWIN[tool]
    rest = _required_permission(method, path)
    assert isinstance(rest, str), (tool, method, path, rest)
    permission, _effect = tool_requirement(tool)
    tool_roles, rest_roles = _roles(permission), _roles(rest)
    if tool in ROLE_MODEL_WIDER_THAN_REST:
        assert tool_roles - rest_roles == ROLE_MODEL_WIDER_THAN_REST[tool], (tool, permission, rest)
        assert rest_roles <= tool_roles, (tool, permission, rest)
        return
    assert tool_roles <= rest_roles, (
        f"{tool} admits {sorted(tool_roles - rest_roles)} that {method} {path} refuses "
        f"(tool {permission}, REST {rest})"
    )
    if tool not in STRICTER:
        assert tool_roles == rest_roles, (tool, permission, rest)


@pytest.mark.parametrize("tool", sorted(PILOT_ONLY_MUTATION))
def test_pilot_only_mutation_confirms_under_its_own_permission(tool):
    from src.ai.copilot.tool_permissions import ACK_KIND_PERMISSIONS, MUTATE, tool_requirement

    permission, effect = tool_requirement(tool)
    assert effect == MUTATE
    assert ACK_KIND_PERMISSIONS[PILOT_ONLY_MUTATION[tool]] == permission


def test_every_twinned_mutation_confirms_under_the_tool_permission():
    from src.ai.copilot.tool_permissions import ACK_KIND_PERMISSIONS, MUTATE, tool_requirement

    for tool in REST_TWIN:
        permission, effect = tool_requirement(tool)
        if effect == MUTATE and tool in ACK_KIND_PERMISSIONS:
            assert ACK_KIND_PERMISSIONS[tool] == permission, tool


@pytest.mark.parametrize("tool", sorted(NO_REST_READ))
def test_reads_without_a_rest_twin_grant_nothing_beyond_membership(tool):
    from src.ai.copilot.tool_permissions import PLAN, READ, tool_requirement

    permission, effect = tool_requirement(tool)
    assert effect in (READ, PLAN), tool
    assert _roles(permission) == {"viewer", "operator", "editor", "admin"}, (tool, permission)
