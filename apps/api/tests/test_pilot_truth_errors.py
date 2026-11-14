"""Pilot ground truth — error text never carries a secret into the chat (audit H6).

The MCP path masks tool errors (``services.mcp_protocol``); the chat path
copied ``str(exc)`` into the answer and into ``tools_used[].summary``, so a
connection URI in a driver error reached the operator's screen with its
password. One chokepoint now serves both paths.
"""

from __future__ import annotations

import pytest

from tests.helpers import pilot_truth as pt

PASSWORD = "FAKE-Pa55w0rd"
HOST = "mongo-prod-3.internal.corp"
URI = f"mongodb://svc_dataflow:{PASSWORD}@{HOST}:27017/dataflow?authSource=admin"


def _raise_uri(*_args, **_kwargs):
    raise RuntimeError(f"could not connect: {URI}: timed out")


def _everything_shown(response) -> str:
    return "\n".join([response.answer, *(str(t.get("summary") or "") for t in response.tools_used)])


@pytest.fixture
def workspace(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.seed_schedules(3)


def test_unhandled_tool_exception_is_not_shown_to_the_operator(workspace, monkeypatch):
    import services.schedule_store as schedule_store

    monkeypatch.setattr(schedule_store, "list_schedules", _raise_uri)
    response = pt.ask(pt.agent(), "show my pipelines")
    shown = _everything_shown(response)

    assert PASSWORD not in shown
    assert HOST not in shown and "svc_dataflow" not in shown, "driver error text reached the chat"
    assert "Reference:" in response.answer, "an operator needs the id that finds the server log line"
    assert not pt.grounded(response)


def test_tool_reported_error_has_uri_credentials_masked(workspace, monkeypatch):
    from src.ai.copilot.tools import ToolResult, get_pilot_tools

    tools = get_pilot_tools()
    monkeypatch.setattr(
        tools, "_list_contracts",
        lambda **_k: ToolResult(name="list_contracts", success=False, output=None,
                                error=f"Could not load contracts ({URI})"),
    )
    result = tools.execute("list_contracts", {})
    assert result.success is False
    assert PASSWORD not in result.error
    assert "svc_dataflow:****@" in result.error


def test_chat_and_mcp_show_the_same_masked_error(workspace, monkeypatch):
    import services.schedule_store as schedule_store
    from services.mcp_protocol import McpCallContext, _execute_tool
    from src.ai.copilot.tools import get_pilot_tools

    monkeypatch.setattr(schedule_store, "list_schedules", _raise_uri)
    chat = get_pilot_tools().execute("list_schedules", {})
    mcp = _execute_tool("list_schedules", {}, McpCallContext(actor="t", client="t", correlation_id=None))
    mcp_text = mcp["content"][0]["text"]

    assert mcp["isError"] is True
    for text in (chat.error, mcp_text):
        assert PASSWORD not in text and HOST not in text
    # Same sentence on both surfaces; only the per-call reference differs.
    assert chat.error.split(" Reference:")[0] == mcp_text.split(" Reference:")[0]


def test_connector_store_failure_text_is_masked(workspace, monkeypatch):
    import services.connector_store as connector_store

    monkeypatch.setattr(connector_store, "list_connectors", _raise_uri)
    response = pt.ask(pt.agent(), "list my connectors")
    assert PASSWORD not in _everything_shown(response)


def test_job_error_text_with_a_connection_uri_is_masked_in_answers(workspace):
    job_id = pt.seed_job("failed", minutes_ago=1, error=f"write failed: {URI} refused the connection")
    agent = pt.agent()
    for question in (f"status of job {job_id}", "what is the status of my last transfer", "show me my failed jobs"):
        response = pt.ask(agent, question)
        assert job_id in response.answer
        assert PASSWORD not in _everything_shown(response), question


def test_briefing_never_carries_exception_text(workspace, monkeypatch):
    import services.schedule_store as schedule_store

    monkeypatch.setattr(schedule_store, "list_schedules", _raise_uri)
    response = pt.ask(pt.agent(), "give me a workspace briefing")
    shown = _everything_shown(response)
    assert PASSWORD not in shown and HOST not in shown
