"""Wave 36a — stop RAG synonym dumps for live data asks."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.mark.parametrize(
    "message",
    [
        "can you get users data from postgres",
        "get users from postgres",
        "fetch orders data from Local Postgres",
        "pull customers from Warehouse",
        "users data from postgres",
    ],
)
def test_get_data_routes_to_sample_not_rag(message: str):
    from src.ai.copilot.tools import infer_tools_from_message

    planned = infer_tools_from_message(message)
    names = [n for n, _ in planned]
    assert "sample_connector_object" in names, (message, planned)
    assert "search_knowledge" not in names, (message, planned)
    args = dict(planned)["sample_connector_object"]
    assert args.get("table")
    assert args.get("connector_name")


def test_chat_get_users_never_dumps_synonyms():
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    resp = agent.chat("can you get users data from postgres")
    answer = (resp.answer or "").lower()
    assert "synonym group" not in answer
    assert "industry schema" not in answer
    assert "phone_number" not in answer or "sample" in answer or "connector" in answer
    tool_names = [t["name"] if isinstance(t, dict) else t.name for t in (resp.tools_used or [])]
    assert "search_knowledge" not in tool_names


def test_noise_knowledge_hits_filtered():
    from src.ai.copilot.tools import _is_noise_knowledge_hit

    assert _is_noise_knowledge_hit(
        "Synonym group: phone = phone, telephone, mobile, cell"
    )
    assert _is_noise_knowledge_hit(
        "Industry schema: Telecommunications. Columns: subscriber_id"
    )
    assert not _is_noise_knowledge_hit(
        "Mapping assurance uses scored assignment across semantic layers."
    )


def test_followups_do_not_suggest_fake_logistics():
    from src.ai.copilot.pilot_agent import DataPilotAgent, PilotTurn

    agent = DataPilotAgent()
    prompts = agent._follow_ups("hello", PilotTurn())
    assert not any("logistics" in p.lower() for p in prompts)
