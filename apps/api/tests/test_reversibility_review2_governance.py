"""Review 2, N6: ``require_reversibility`` cannot be lost or switched off without re-approval.

GitOps export / apply keeps it, the standing-authorization binding includes it
(turning it off invalidates a grant signed while it was on), and Pilot's
``create_schedule`` can set it.
"""

from __future__ import annotations

from typing import Any

import pytest

from services.schedule_store import PipelineSchedule

_BASE = dict(id="s1", name="n", source_connector_id="a", source_table="t", dest_connector_id="b",
             dest_table="t", interval="daily", mappings=[{"source": "id", "target": "id"}])


def _sched(flag: bool) -> PipelineSchedule:
    return PipelineSchedule.from_dict({**_BASE, "require_reversibility": flag})


def test_n6_gitops_export_and_apply_keep_require_reversibility():
    from services.gitops_manifest import apply_schedule_spec, schedule_spec

    spec = schedule_spec(_sched(True))
    assert spec.get("require_reversibility") is True
    assert PipelineSchedule.from_dict(apply_schedule_spec(spec)).require_reversibility is True


def test_n6_turning_require_reversibility_off_invalidates_a_standing_authorization():
    from services.standing_authorization import (
        CODE_BINDING_CHANGED,
        SCOPE_NET_ADDITIVE_DRIFT,
        binding_from_schedule,
        evaluate_authorization,
        grant_authorization,
    )

    grant = grant_authorization(actor="ops lead", reason="nightly unattended runs", scopes=[SCOPE_NET_ADDITIVE_DRIFT],
                                binding=binding_from_schedule(_sched(True)))
    assert evaluate_authorization(grant, binding_now=binding_from_schedule(_sched(True))).applies is True
    decision = evaluate_authorization(grant, binding_now=binding_from_schedule(_sched(False)))
    assert decision.applies is False and decision.code == CODE_BINDING_CHANGED
    assert any(d["field"] == "require_reversibility" for d in decision.differences)
    assert "reversibility" in decision.reason


def test_n6_a_grant_signed_before_the_flag_existed_still_applies_while_it_is_off():
    from services.standing_authorization import (
        SCOPE_NET_ADDITIVE_DRIFT,
        binding_from_schedule,
        evaluate_authorization,
        grant_authorization,
    )

    legacy = dict(binding_from_schedule(_sched(False)))
    legacy.pop("require_reversibility", None)
    grant = grant_authorization(actor="ops lead", reason="nightly unattended runs", scopes=[SCOPE_NET_ADDITIVE_DRIFT], binding=legacy)
    assert evaluate_authorization(grant, binding_now=binding_from_schedule(_sched(False))).applies is True
    # ... and its hash is unchanged (old grants keep their binding hash).
    assert binding_from_schedule(_sched(False))["binding_hash"] == legacy["binding_hash"]
    assert binding_from_schedule(_sched(True))["binding_hash"] != legacy["binding_hash"]


@pytest.fixture
def cleared_plan(monkeypatch: pytest.MonkeyPatch):
    from src.ai.copilot import schedule_tools
    from tests.test_pilot_schedule_nl import _plan_stub

    monkeypatch.setattr(schedule_tools, "plan_transfer", lambda **kw: _plan_stub())
    monkeypatch.setattr(schedule_tools, "_is_execute_cleared", lambda pf: True)
    return schedule_tools


def test_n6_pilot_create_schedule_carries_require_reversibility(cleared_plan, monkeypatch):
    import src.ai.copilot.ack_ledger as ack_ledger

    staged: list[dict[str, Any]] = []

    class _Ledger:
        def put(self, *, kind, payload, preview):
            staged.append(payload)
            return f"ack_{len(staged)}"

    monkeypatch.setattr(ack_ledger, "get_ack_ledger", lambda: _Ledger())
    result = cleared_plan.create_schedule(cadence="daily at 02:00 UTC", require_reversibility=True)
    assert result.success is True, result.error
    out: dict[str, Any] = result.output or {}
    assert out["preview"]["require_reversibility"] is True
    assert staged[-1]["require_reversibility"] is True
    assert cleared_plan.create_schedule(cadence="daily at 02:00 UTC").success is True
    assert staged[-1]["require_reversibility"] is False


def test_n6_the_pilot_tool_schema_offers_require_reversibility():
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    tool = next(t for t in TOOL_DEFINITIONS if t["name"] == "create_schedule")
    assert tool["input_schema"]["properties"]["require_reversibility"]["type"] == "boolean"
