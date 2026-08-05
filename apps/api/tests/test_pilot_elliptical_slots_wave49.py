"""Wave 49 — elliptical edits must not fill connector slots; typo + short-nav."""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_PILOT_ENGINE", "local")

from src.ai.copilot.followup import (
    looks_like_elliptical_edit,
    resolve_pending_answer,
)
from src.ai.copilot.tools import infer_tools_from_message
from src.ai.copilot.working_memory import PendingSlot


def _names(q: str) -> list[str]:
    return [n for n, _ in infer_tools_from_message(q)]


def test_elliptical_not_treated_as_connector_name():
    assert looks_like_elliptical_edit("only paid ones")
    assert looks_like_elliptical_edit("and by region?")
    assert looks_like_elliptical_edit("use upsert instead")
    assert looks_like_elliptical_edit("same for products")
    pending = PendingSlot(
        tool="aggregate_data",
        missing="connector_name",
        args={"table": "orders", "metric": "count"},
        question="Which connector?",
        candidates=[],
    )
    assert resolve_pending_answer("only paid ones", pending) is None
    assert resolve_pending_answer("and by region?", pending) is None
    assert resolve_pending_answer("Local Postgres", pending) is not None


def test_typo_and_short_nav_routes():
    assert "start_transfer" in _names("trasfer orders from Local Postgres to Warehouse")
    assert "run_schedule_now" in _names("schdule Nightly Orders now")
    assert "aggregate_data" in _names("how mny rows in orders on Local Postgres")
    assert "navigate" in _names("mcp page please")
    assert "navigate" in _names("contracts please")
    assert "navigate" in _names("query playground please")


def test_mapping_incorrect_and_g5_faq():
    assert "remediate_validation" in _names("I think my mapping is incorrect")
    assert "explain_product" in _names("what is G5 dry run")


def test_multiturn_elliptical_after_failed_connector(monkeypatch, tmp_path):
    """Follow-up filters must not become connector names when clarify is open."""
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "c.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    from services import connector_store
    from src.ai.copilot import tools as tools_mod
    from src.ai.copilot.pilot_agent import DataPilotAgent

    connector_store._backend_choice = None
    tools_mod._tools = None

    agent = DataPilotAgent()
    sid = "wave49-elliptical"
    hist = []
    first = agent.chat(
        "how many rows in orders on MissingConn",
        history=hist,
        data_context={"pilot_session_id": sid},
    )
    hist += [
        {"role": "user", "content": "how many rows in orders on MissingConn"},
        {"role": "assistant", "content": first.answer or ""},
    ]
    second = agent.chat(
        "only paid ones",
        history=hist,
        data_context={"pilot_session_id": sid},
    )
    # Must not claim the connector is literally named "only paid ones".
    assert "only paid ones" not in (second.answer or "").lower() or "connector" not in (
        second.answer or ""
    ).lower().split("only paid ones")[0][-40:]
    low = (second.answer or "").lower()
    assert 'no connector matched "only paid ones"' not in low
    assert 'no connector matched “only paid ones”' not in low
