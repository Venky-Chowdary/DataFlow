"""Pilot / MCP tools of Reversibility Proof: registered, gated, read only, workspace scoped."""

from __future__ import annotations

import pytest


@pytest.fixture()
def store(tmp_path, monkeypatch):
    from services import exit_store

    monkeypatch.setattr(exit_store, "STORE_PATH", tmp_path / "exit.json")
    monkeypatch.setattr(exit_store, "_db", lambda: None)
    for doc in (
        {"id": "exs_a", "workspace_id": "ws-a", "name": "Ledger", "criticality": "critical",
         "source": {"engine": "postgresql"}, "tables": [{"name": "orders"}], "exit_window_hours": 720,
         "rehearsal_every_days": 90, "created_at": "2026-11-01T00:00:00Z", "regulatory_dates": []},
        {"id": "exs_b", "workspace_id": "ws-b", "name": "Theirs", "criticality": "standard",
         "source": {}, "tables": [{"name": "t"}], "exit_window_hours": 1, "created_at": "2026-11-01T00:00:00Z"},
    ):
        exit_store.insert(exit_store.SYSTEMS, doc)
    exit_store.insert(exit_store.REHEARSALS, {
        "id": "exr_a", "system_id": "exs_a", "system_name": "Ledger", "workspace_id": "ws-a", "n": 1,
        "verdict": "proven", "verdict_reason": "Every one of the 1 declared table(s) was extracted and proven complete.",
        "started_at": "2026-11-02T00:00:00Z", "finished_at": "2026-11-02T01:00:00Z",
        "tables": [{"name": "orders", "status": "proven", "reason": "ok", "rows_source": 5, "rows_extracted": 5,
                    "value_parity": {"method": "population_digest"}}],
        "completeness": {"statement": "1 of 1 declared table(s) extracted."},
        "duration": {"statement": "Measured 1.0 h", "measured_seconds": 3600, "window_seconds": 2592000,
                     "fits_window": True},
        "seal": {"merkle_root": "sha256:" + "a" * 64, "parts_count": 1}, "required_roles": ["engineering"],
    })
    return exit_store


def test_tools_are_registered_read_only_and_listed_over_mcp():
    from services.mcp_protocol import handle_jsonrpc
    from services.rbac import Permission
    from src.ai.copilot.tool_permissions import READ, TOOL_PERMISSIONS
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    assert TOOL_PERMISSIONS["get_exit_readiness"] == (Permission.JOB_READ, READ)
    assert TOOL_PERMISSIONS["explain_exit_rehearsal"] == (Permission.JOB_READ, READ)
    assert TOOL_PERMISSIONS["check_reversibility"] == (Permission.JOB_PLAN, READ)
    names = {t["name"] for t in TOOL_DEFINITIONS}
    assert {"get_exit_readiness", "explain_exit_rehearsal", "check_reversibility"} <= names
    listed = handle_jsonrpc({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, authenticated=True)
    assert "explain_exit_rehearsal" in {t["name"] for t in listed["result"]["tools"]}


def test_readiness_and_rehearsal_answer_only_inside_the_workspace(store):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tools import DataPilotTools

    with bound_workspace_filter(WorkspaceFilter.only("ws-a")):
        board = DataPilotTools().execute("get_exit_readiness", {})
        assert board.success and [s["id"] for s in board.output["systems"]] == ["exs_a"]
        assert board.output["kpis"]["systems_proven"] == 1 and "does not certify" in board.output["note"]
        explained = DataPilotTools().execute("explain_exit_rehearsal", {"system_id": "exs_a"})
        assert explained.success, explained.error
        assert explained.output["verdict"] == "proven"
        assert explained.output["tables"][0]["value_check"] == "population_digest"
        assert explained.output["open"] == "/#/exit?system=exs_a&rehearsal=exr_a"
        foreign = DataPilotTools().execute("explain_exit_rehearsal", {"system_id": "exs_b"})
        assert not foreign.success and "not found" in foreign.error
        never = DataPilotTools().execute("explain_exit_rehearsal", {})
        assert not never.success and "Which rehearsal" in never.error


def test_check_reversibility_names_every_lossy_column(monkeypatch):
    from services.transfer_plan_store import PlanRevision, TransferPlanRecord
    from src.ai.copilot.tools import DataPilotTools

    plan = TransferPlanRecord(
        id="plan-9", name="p", status="mapped", source={"format": "postgresql", "table": "c"},
        destination={"format": "postgresql", "table": "c"}, source_columns=["id", "email"],
        source_schema={"id": "INTEGER", "email": "TEXT"}, target_columns=["id", "email"],
        target_schema={"id": "INTEGER", "email": "TEXT"},
        revisions=[PlanRevision(version=1, mappings=[{"source": "id", "target": "id"},
                                                     {"source": "email", "target": "email", "transform": "hash_pii"}],
                                transforms=[], validation={}, mapping_hash="h", source_schema_hash="s",
                                target_schema_hash="t", agents_used=[])],
        active_version=1,
    )
    monkeypatch.setattr("services.transfer_plan_store.get_plan", lambda pid: plan if pid == "plan-9" else None)
    out = DataPilotTools().execute("check_reversibility", {"plan_id": "plan-9"})
    assert out.success, out.error
    assert out.output["verdict"] == "partly_reversible"
    assert out.output["lossy_columns"][0]["reason_code"] == "hash_pii"
    assert not DataPilotTools().execute("check_reversibility", {"plan_id": "nope"}).success
