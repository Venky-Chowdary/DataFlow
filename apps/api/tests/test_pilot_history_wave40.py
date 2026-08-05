"""Wave 40 — history coreference + auth-failure honesty."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_those_jobs_coreference_from_history(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    sid = "wave40-jobs"
    history = [
        {"role": "user", "content": "show my jobs"},
        {"role": "assistant", "content": "Here are your recent transfer jobs."},
    ]
    resp = agent.chat(
        "which of those failed?",
        history=history,
        data_context={"pilot_session_id": sid},
    )
    names = [t["name"] for t in (resp.tools_used or [])]
    assert "list_jobs" in names


def test_sample_that_table_uses_focus(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.copilot.working_memory import get_working_memory

    agent = DataPilotAgent()
    sid = "wave40-table"
    get_working_memory().update_focus(
        sid,
        connector_name="Local Postgres",
        table="orders",
    )
    planned = agent._plan_with_memory(
        "sample that table",
        {"pilot_session_id": sid},
        history=[],
    )
    assert planned
    assert planned[0][0] == "sample_connector_object"
    assert planned[0][1].get("table") == "orders"


def test_schema_that_table_uses_focus(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.copilot.working_memory import get_working_memory

    agent = DataPilotAgent()
    sid = "wave40-schema"
    get_working_memory().update_focus(sid, connector_name="Local Postgres", table="airports")
    planned = agent._plan_with_memory(
        "schema of that table",
        {"pilot_session_id": sid},
        [],
    )
    assert planned and planned[0][0] == "introspect_connector_schema"


def test_platform_coreference_helper():
    from src.ai.copilot.followup import resolve_platform_coreference

    hist = [{"role": "assistant", "content": "You have 4 recent jobs."}]
    assert resolve_platform_coreference("which of those failed?", hist)[0][0] == "list_jobs"
    assert resolve_platform_coreference("take me to jobs", hist) is None


def test_auth_failure_is_process_wide(monkeypatch):
    from src.ai.llm import provider as prov

    prov.clear_auth_failures()
    assert not prov._provider_auth_failed("openai")
    assert prov._mark_provider_auth_failed("openai", "Error code: 401 - invalid_api_key")
    assert prov._provider_auth_failed("openai")
    # New instance must also report unavailable
    o = prov.DataTransferOpenAIProvider()
    assert o.is_available() is False
    prov.clear_auth_failures()
