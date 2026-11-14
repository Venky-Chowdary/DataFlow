"""UX-18: Pilot's suggested prompts dead-ended.

The starter list appended curated demo prompts that name connections a
workspace may not have ("How many rows in orders on Local Postgres?"), and the
page offered industry demo tiles (logistics CSV, HR data). Suggestions now
come only from the workspace's own datasets and connectors, plus prompts the
agent answers from live state.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("example", ["your connector", "Orders PG"])
def test_starter_prompts_never_name_demo_connections(monkeypatch, example):
    from src.ai.copilot import example_phrases
    from src.ai.copilot.pilot_agent import DataPilotAgent

    monkeypatch.setattr(example_phrases, "example_connector_name", lambda: example)
    agent = DataPilotAgent.__new__(DataPilotAgent)
    agent.analyst = SimpleNamespace(list_datasets=lambda: [])
    prompts = agent._starter_prompts()
    joined = " | ".join(prompts)
    assert "Local Postgres" not in joined and "Warehouse" not in joined, prompts
    assert "Give me a workspace briefing" in prompts
    if example != "your connector":
        assert f"List tables on {example}" in prompts
