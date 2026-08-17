"""Every Pilot tool is governed, and governed like its REST equivalent.

Pilot can reach the same operations the product's routes perform, so a chat turn
is a privilege boundary. These tests pin the properties that make it one:

* the policy table covers the **whole** registry, so a new tool cannot ship
  ungated by omission — and an unlisted tool is admin-only, not open;
* a mutating tool is refused for a role that lacks the permission, at the
  dispatcher, whoever chose the tool (deterministic router, LLM loop, or MCP);
* a refusal says what is missing and who can grant it, instead of reading like a
  connector or data problem;
* the role is bound per turn, so a turn that fans out to a worker thread does not
  run unauthenticated.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from services.rbac import Permission, role_permissions
from src.ai.copilot.tool_permissions import (
    MUTATE,
    TOOL_PERMISSIONS,
    ACK_KIND_PERMISSIONS,
    bind_current_context,
    caller_role,
    can_confirm_kind,
    current_caller_role,
    denial_message,
    is_permission_denial,
    is_tool_allowed,
    tool_requirement,
)
from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools

REGISTERED = {d["name"] for d in TOOL_DEFINITIONS}


def test_every_registered_tool_has_a_permission_and_an_effect():
    missing = sorted(REGISTERED - set(TOOL_PERMISSIONS))
    assert missing == [], f"ungated tools: {missing}"


def test_the_policy_table_has_no_entries_for_tools_that_do_not_exist():
    # A stale entry hides the coverage check above: the tool it names is gone, so
    # it can silently stand in for one that was never added.
    extra = sorted(set(TOOL_PERMISSIONS) - REGISTERED)
    assert extra == [], f"policy names unknown tools: {extra}"


def test_an_unlisted_tool_is_admin_only_rather_than_open():
    permission, effect = tool_requirement("some_future_delete_tool")
    assert effect == MUTATE
    assert permission == Permission.WORKSPACE_MANAGE
    assert is_tool_allowed("viewer", "some_future_delete_tool") is False
    assert is_tool_allowed("editor", "some_future_delete_tool") is False


@pytest.mark.parametrize(
    ("tool", "permission"),
    [
        ("start_transfer", Permission.JOB_RUN),
        ("create_connector", Permission.CONNECTOR_WRITE),
        ("run_schedule_now", Permission.SCHEDULE_MANAGE),
        ("create_schedule", Permission.SCHEDULE_MANAGE),
        ("plan_transfer", Permission.JOB_PLAN),
        ("run_query", Permission.QUERY_USE),
        ("get_job", Permission.JOB_READ),
    ],
)
def test_a_tool_is_gated_by_the_permission_of_the_route_that_does_the_same_thing(
    tool: str, permission: str
):
    assert tool_requirement(tool)[0] == permission


def test_a_viewer_can_read_and_ask_but_cannot_reach_a_mutation():
    granted = role_permissions("viewer")
    assert Permission.AI_USE in granted
    mutating = [n for n, (_p, e) in TOOL_PERMISSIONS.items() if e == MUTATE]
    assert mutating, "the registry must have mutating tools for this to mean anything"
    assert [n for n in mutating if is_tool_allowed("viewer", n)] == []
    assert is_tool_allowed("viewer", "get_job") is True
    assert is_tool_allowed("viewer", "run_query") is True


def test_an_operator_runs_jobs_but_does_not_author_connectors_or_pipelines():
    assert is_tool_allowed("operator", "start_transfer") is True
    assert is_tool_allowed("operator", "create_connector") is False
    assert is_tool_allowed("operator", "create_schedule") is False


def test_an_admin_may_reach_every_registered_tool():
    assert [n for n in TOOL_PERMISSIONS if not is_tool_allowed("admin", n)] == []


def test_an_unknown_role_is_treated_as_a_viewer_not_as_an_admin():
    assert is_tool_allowed("data-wizard", "start_transfer") is False
    assert is_tool_allowed("data-wizard", "get_job") is True


def test_an_open_deployment_binds_no_role_and_keeps_working():
    # ``""`` is the unauthenticated single-operator posture the REST routes take.
    assert is_tool_allowed("", "start_transfer") is True
    assert can_confirm_kind("", "start_transfer") is True


# --------------------------------------------------------------------------
# Enforcement at the dispatcher, not at the caller
# --------------------------------------------------------------------------


def test_the_dispatcher_refuses_a_mutation_before_the_handler_runs(monkeypatch):
    tools = DataPilotTools()

    def _must_not_run(**kwargs: object):
        raise AssertionError("handler ran despite the permission refusal")

    monkeypatch.setattr(tools, "_create_schedule", _must_not_run, raising=False)
    with caller_role("viewer"):
        result = tools.execute("create_schedule", {"cadence": "daily at 02:00 UTC"})
    assert result.success is False
    assert is_permission_denial(result.error or "")


def test_an_unknown_tool_name_is_refused_rather_than_dispatched():
    with caller_role("admin"):
        result = DataPilotTools().execute("drop_everything", {})
    assert result.success is False
    assert "unknown tool" in (result.error or "").lower()


def test_a_refusal_names_the_missing_permission_and_who_can_grant_it():
    msg = denial_message("viewer", "create_schedule")
    assert "create or run pipelines" in msg
    assert "editor or admin" in msg
    # It must be recognisable as a permission problem, so no other layer sends
    # the operator off to fix a connector that is perfectly fine.
    assert is_permission_denial(msg)
    assert is_permission_denial("Which connector did you mean?") is False


# --------------------------------------------------------------------------
# The binding itself
# --------------------------------------------------------------------------


def test_the_bound_role_is_restored_after_the_turn():
    with caller_role("viewer"):
        assert current_caller_role() == "viewer"
    assert current_caller_role() == ""


def test_a_turn_that_fans_out_to_a_worker_thread_keeps_its_role():
    with ThreadPoolExecutor(max_workers=1) as pool, caller_role("viewer"):
        # Without bind_current_context the worker sees no role, which reads as
        # the open posture and skips the check entirely.
        assert pool.submit(current_caller_role).result() == ""
        assert pool.submit(bind_current_context(current_caller_role)).result() == "viewer"


# --------------------------------------------------------------------------
# Confirm is the last gate
# --------------------------------------------------------------------------


def test_every_mutating_tool_that_stages_an_ack_has_a_confirm_permission():
    # A staged mutation with no confirm mapping could never be confirmed; one
    # with a weaker mapping would let Confirm undo the staging gate.
    for kind, permission in ACK_KIND_PERMISSIONS.items():
        assert permission in role_permissions("admin"), kind
    assert set(ACK_KIND_PERMISSIONS) >= {
        "start_transfer",
        "create_connector",
        "create_schedule",
    }


@pytest.mark.parametrize(
    ("kind", "role", "allowed"),
    [
        ("start_transfer", "viewer", False),
        ("start_transfer", "operator", True),
        ("create_connector", "operator", False),
        ("create_connector", "editor", True),
        ("create_schedule", "operator", False),
        ("create_schedule", "editor", True),
        ("run_schedule", "viewer", False),
    ],
)
def test_confirm_is_rechecked_against_the_role_at_confirm_time(
    kind: str, role: str, allowed: bool
):
    assert can_confirm_kind(role, kind) is allowed


# --------------------------------------------------------------------------
# The route in front of it all
# --------------------------------------------------------------------------


def test_talking_to_pilot_is_ai_use_and_training_is_workspace_administration():
    from services.rbac import _required_permission

    assert _required_permission("POST", "/api/v1/copilot/chat") == Permission.AI_USE
    assert _required_permission("POST", "/api/v1/copilot/confirm") == Permission.AI_USE
    # Training rewrites workspace-wide knowledge; it is not a chat turn.
    assert (
        _required_permission("POST", "/api/v1/copilot/train")
        == Permission.WORKSPACE_MANAGE
    )
    assert Permission.WORKSPACE_MANAGE not in role_permissions("editor")
