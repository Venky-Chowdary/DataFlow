"""Pilot lifecycle operations: cancel/retry/resume/replay, connector test/delete,
schedule pause/resume/delete.

Covers the verb+object planner, the status guards that refuse impossible
transitions, ack staging, and that every lifecycle tool is permission-mapped
and confirm-dispatchable.
"""

from __future__ import annotations

import importlib

import pytest

from src.ai.copilot import lifecycle_tools as lt
from src.ai.copilot.tool_permissions import (
    ACK_KIND_PERMISSIONS,
    MUTATE,
    READ,
    TOOL_PERMISSIONS,
)
from src.ai.copilot.tools import TOOL_DEFINITIONS, infer_tools_from_message


# ---------------------------------------------------------------- planner ---

@pytest.mark.parametrize(
    "message, tool, args",
    [
        ("cancel the running job", "cancel_job", {"selector": "running"}),
        ("stop my transfer", "cancel_job", {"selector": "last"}),
        ("abort job 5f1e2d3c4b5a69788796a5b4", "cancel_job", {"job_id": "5f1e2d3c4b5a69788796a5b4"}),
        ("retry the failed job", "retry_job", {"selector": "failed"}),
        ("rerun the last transfer", "retry_job", {"selector": "last"}),
        ("resume the failed job", "resume_job", {"selector": "failed"}),
        ("resume job pilotqa-failed-0001", "resume_job", {"job_id": "pilotqa-failed-0001"}),
        ("replay the quarantine rows of the last job", "replay_quarantine", {"selector": "quarantine"}),
        ("replay quarantine for job pilotqa-failed-0001", "replay_quarantine", {"job_id": "pilotqa-failed-0001"}),
        ("test the Demo Orders connector", "test_connector", {"name": "Demo Orders"}),
        ("is Quarantine SQLite reachable?", "test_connector", {"name": "Quarantine SQLite"}),
        ("delete the staging postgres connector", "delete_connector", {"name": "staging postgres"}),
        ("remove connector Demo Orders", "delete_connector", {"name": "Demo Orders"}),
        ("Delete the Quarantine SQLite connector please", "delete_connector", {"name": "Quarantine SQLite"}),
        ("pause the Nightly Orders pipeline", "set_schedule_enabled", {"name": "Nightly Orders", "enabled": False}),
        ("disable schedule Nightly Orders", "set_schedule_enabled", {"name": "Nightly Orders", "enabled": False}),
        ("resume pipeline Nightly Orders", "set_schedule_enabled", {"name": "Nightly Orders", "enabled": True}),
        ("delete pipeline Nightly Orders", "delete_schedule", {"name": "Nightly Orders"}),
    ],
)
def test_planner_maps_verb_object_to_tool(message, tool, args):
    assert lt.plan_lifecycle_operation(message) == [(tool, args)]
    assert infer_tools_from_message(message) == [(tool, args)]


@pytest.mark.parametrize(
    "message",
    [
        "how do i delete a connector",
        "what does cancel do to a job",
        "can I retry a failed job?",
        "delete all my connectors",
        "remove every pipeline",
        "show me the connectors",
        "list jobs",
    ],
)
def test_planner_ignores_howto_bulk_and_reads(message):
    assert lt.plan_lifecycle_operation(message) is None


def test_planner_asks_when_object_is_missing():
    assert lt.plan_lifecycle_operation("delete the connector") == [("delete_connector", {})]
    assert lt.plan_lifecycle_operation("resume the paused pipeline") == [
        ("set_schedule_enabled", {"enabled": True})
    ]


# ------------------------------------------------------------ registry ------

def test_every_lifecycle_tool_is_registered_and_permission_mapped():
    names = {t["name"] for t in TOOL_DEFINITIONS}
    for tool in lt.LIFECYCLE_TOOL_NAMES:
        assert tool in names
        assert tool in TOOL_PERMISSIONS
    assert TOOL_PERMISSIONS["test_connector"][1] == READ
    for tool, kind in lt.ACK_KIND_BY_TOOL.items():
        assert TOOL_PERMISSIONS[tool][1] == MUTATE
        assert kind in ACK_KIND_PERMISSIONS
        assert ACK_KIND_PERMISSIONS[kind] == TOOL_PERMISSIONS[tool][0]


# ---------------------------------------------------------- status guards ---

def _job(status: str, **extra):
    return {
        "id": "job-abc123def456",
        "status": status,
        "transfer_request": {
            "source": {"connector_name": "Src", "table": "orders"},
            "destination": {"connector_name": "Dst"},
        },
        "rows_written": 10,
        **extra,
    }


@pytest.fixture
def staged(monkeypatch):
    acks: list[dict] = []

    class Ledger:
        def put(self, *, kind, payload, preview):
            acks.append({"kind": kind, "payload": payload, "preview": preview})
            return f"ack-{len(acks)}"

    import src.ai.copilot.ack_ledger as ledger_mod

    monkeypatch.setattr(ledger_mod, "get_ack_ledger", lambda: Ledger())
    return acks


def test_cancel_refuses_terminal_job(monkeypatch, staged):
    monkeypatch.setattr(lt, "resolve_job", lambda job_id="", selector="": (_job("failed"), ""))
    tr = lt._job_tool("cancel_job", "", "failed")
    assert not tr.success and "nothing to cancel" in tr.error
    assert staged == []


def test_cancel_stages_running_job(monkeypatch, staged):
    monkeypatch.setattr(lt, "resolve_job", lambda job_id="", selector="": (_job("running"), ""))
    tr = lt._job_tool("cancel_job", "", "running")
    assert tr.success and tr.output["requires_confirm"] and tr.output["ack_id"] == "ack-1"
    assert staged[0]["kind"] == "cancel_job"
    assert staged[0]["payload"] == {"job_id": "job-abc123def456"}
    assert "password" not in str(staged[0]["preview"])


def test_retry_only_from_failed_or_cancelled(monkeypatch, staged):
    monkeypatch.setattr(lt, "resolve_job", lambda job_id="", selector="": (_job("running"), ""))
    assert not lt._job_tool("retry_job", "", "").success
    monkeypatch.setattr(lt, "resolve_job", lambda job_id="", selector="": (_job("failed"), ""))
    tr = lt._job_tool("retry_job", "", "")
    assert tr.success and tr.output["destructive"] is True


def test_resume_refuses_completed(monkeypatch, staged):
    monkeypatch.setattr(lt, "resolve_job", lambda job_id="", selector="": (_job("completed"), ""))
    tr = lt._job_tool("resume_job", "", "")
    assert not tr.success and "only a failed" in tr.error


def test_replay_requires_quarantine_rows(monkeypatch, staged):
    monkeypatch.setattr(lt, "resolve_job", lambda job_id="", selector="": (_job("completed"), ""))
    assert "no quarantined rows" in lt._job_tool("replay_quarantine", "", "").error
    monkeypatch.setattr(
        lt, "resolve_job", lambda job_id="", selector="": (_job("completed_with_quarantine", rejected_rows=3), "")
    )
    assert lt._job_tool("replay_quarantine", "", "").success


def test_resolve_job_ambiguity_becomes_question(monkeypatch):
    jobs = [{"id": "a" * 24, "status": "failed"}, {"id": "b" * 24, "status": "failed"}]
    import src.ai.copilot.job_reads as jr

    monkeypatch.setattr(jr, "list_transfer_jobs", lambda limit=10, workspace_id=None: (jobs, {}, "mongo"))
    monkeypatch.setattr(jr, "read_transfer_job", lambda jid: None)
    job, clarify = lt.resolve_job("", "failed")
    assert job is None and "More than one job is failed" in clarify


def test_delete_connector_never_guesses(monkeypatch, staged):
    monkeypatch.setattr(lt, "_connector_dict", lambda connector_id="", name="": None)
    tr = lt.delete_connector(name="postgres")
    assert not tr.success and "No connector matched" in tr.error
    assert lt.delete_connector().error.startswith("Which connector")
    assert staged == []


def test_delete_connector_stages_with_bound_schedules(monkeypatch, staged):
    monkeypatch.setattr(
        lt, "_connector_dict", lambda connector_id="", name="": {"id": "c1", "name": "Warehouse", "type": "postgresql"}
    )
    monkeypatch.setattr(lt, "_schedules_bound_to", lambda cid: ["Nightly Orders"])
    tr = lt.delete_connector(name="Warehouse")
    assert tr.success and tr.output["destructive"]
    assert staged[0]["payload"] == {"connector_id": "c1", "name": "Warehouse"}
    assert staged[0]["preview"]["bound_schedules"] == ["Nightly Orders"]


class _Sched:
    def __init__(self, enabled):
        self.id = "s1"
        self.name = "Nightly Orders"
        self.enabled = enabled
        self.next_run_at = ""
        self.sync_mode = "mirror"
        self.run_history = []


def test_pause_is_noop_when_already_paused(staged):
    resolver = lambda sid, name: (_Sched(False), "")  # noqa: E731
    tr = lt.set_schedule_enabled(resolver, name="Nightly Orders", enabled=False)
    assert not tr.success and "already paused" in tr.error
    tr = lt.set_schedule_enabled(resolver, name="Nightly Orders", enabled=True)
    assert tr.success and staged[0]["payload"]["enabled"] is True


def test_delete_schedule_stages(staged):
    tr = lt.delete_schedule(lambda sid, name: (_Sched(True), ""), name="Nightly Orders")
    assert tr.success and staged[0]["kind"] == "delete_schedule"
    assert staged[0]["payload"] == {"schedule_id": "s1", "name": "Nightly Orders"}


# ------------------------------------------------------- confirm dispatch ---

from src.ai.copilot.tool_permissions import can_confirm_kind  # noqa: E402


@pytest.mark.parametrize(
    ("kind", "role", "allowed"),
    [
        ("cancel_job", "viewer", False),
        ("cancel_job", "operator", True),
        ("retry_job", "viewer", False),
        ("resume_job", "operator", True),
        ("replay_quarantine", "editor", True),
        ("delete_connector", "editor", False),
        ("delete_connector", "operator", False),
        ("delete_connector", "admin", True),
        ("set_schedule_enabled", "operator", False),
        ("set_schedule_enabled", "editor", True),
        ("delete_schedule", "viewer", False),
        ("delete_schedule", "admin", True),
    ],
)
def test_confirm_rechecks_role_for_lifecycle_kinds(kind, role, allowed):
    assert can_confirm_kind(role, kind) is allowed


class _Req:
    def __init__(self, ack_id: str) -> None:
        self.ack_id = ack_id
        self.actor = "owner"
        self.reason = "approved"


class _Http:
    class state:  # noqa: N801
        pass

    headers: dict[str, str] = {}


def _confirm(ack_id: str) -> dict:
    import asyncio

    from fastapi import BackgroundTasks

    from src.routers import copilot_router

    return asyncio.run(copilot_router.copilot_confirm(_Req(ack_id), _Http(), BackgroundTasks()))


def test_confirm_dispatches_cancel_once_and_replays_idempotently(monkeypatch):
    from src.ai.copilot.ack_ledger import get_ack_ledger
    from src.routers import copilot_router

    connectors_router = importlib.import_module("src.routers.connectors_router")
    calls: list[str] = []

    async def _cancel(job_id, http_request):
        calls.append(job_id)
        return {"success": True, "job_id": job_id, "status": "cancelled"}

    monkeypatch.setattr(connectors_router, "cancel_transfer_job", _cancel)
    monkeypatch.setattr(copilot_router, "_caller", lambda req: ("operator", "op@example.com"))

    ack = get_ack_ledger().put(kind="cancel_job", payload={"job_id": "j1"}, preview={"job_id": "j1"})
    first = _confirm(ack)
    assert first["ok"] and first["idempotent"] is False and first["status"] == "cancelled"
    replay = _confirm(ack)
    assert replay["idempotent"] is True and replay["status"] == "cancelled"
    assert calls == ["j1"]


def test_confirm_releases_claim_when_owner_route_refuses(monkeypatch):
    from fastapi import HTTPException

    from src.ai.copilot.ack_ledger import get_ack_ledger
    from src.routers import copilot_router

    schedules_router = importlib.import_module("src.routers.schedules_router")

    async def _patch(sid, body, http_request, workspace_id):
        raise HTTPException(status_code=400, detail="Schedule has no persisted column mappings")

    monkeypatch.setattr(schedules_router, "patch_pipeline_schedule", _patch)
    monkeypatch.setattr(copilot_router, "_caller", lambda req: ("editor", "ed@example.com"))

    ack = get_ack_ledger().put(
        kind="set_schedule_enabled", payload={"schedule_id": "s1", "name": "N", "enabled": True}, preview={}
    )
    with pytest.raises(HTTPException) as exc:
        _confirm(ack)
    assert "column mappings" in str(exc.value.detail)
    assert get_ack_ledger().peek(ack) is not None  # still spendable after the fix


def test_confirm_denies_lifecycle_kind_to_weaker_role(monkeypatch):
    from fastapi import HTTPException

    from src.ai.copilot.ack_ledger import get_ack_ledger
    from src.routers import copilot_router

    saved_connectors_router = importlib.import_module("src.routers.saved_connectors_router")
    monkeypatch.setattr(
        saved_connectors_router, "remove_saved_connector", lambda *a, **k: pytest.fail("must not run")
    )
    monkeypatch.setattr(copilot_router, "_caller", lambda req: ("editor", "ed@example.com"))
    ack = get_ack_ledger().put(kind="delete_connector", payload={"connector_id": "c1", "name": "W"}, preview={})
    with pytest.raises(HTTPException) as exc:
        _confirm(ack)
    assert exc.value.status_code == 403
