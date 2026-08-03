"""Wave 48 — hard/colloquial NL improvements (slang, short nav, overwrite, PII)."""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_PILOT_ENGINE", "local")

from src.ai.copilot.tools import infer_tools_from_message


def _names(prompt: str) -> list[str]:
    return [n for n, _ in infer_tools_from_message(prompt)]


def test_slang_and_short_nav():
    assert "list_connector_objects" in _names("pls list tbls on Local Postgres")
    assert "navigate" in _names("docs please")
    assert "navigate" in _names("pipelines please")
    assert any(n in {"navigate", "list_schedules"} for n in _names("pipelines please"))


def test_overwrite_and_pii_faq():
    assert "explain_product" in _names("is overwrite safe for this transfer")
    assert "explain_product" in _names("what columns look like SSN")


def test_telegraphic_count_and_knowledge():
    assert "aggregate_data" in _names("count rows airports Local Postgres")
    assert "search_knowledge" in _names("search knowledge for semantic types")
    assert "search_data" not in _names("search knowledge for semantic types")


def test_mapping_wrong_and_map_schema():
    assert "remediate_validation" in _names("whats wrong with mapping")
    assert "map_connector_schemas" in _names(
        "map schema of orders on Local Postgres to Warehouse"
    )
    assert "introspect_connector_schema" not in _names(
        "map schema of orders on Local Postgres to Warehouse"
    )


def test_wave48_hard_chat_smoke(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    for q in (
        "docs please",
        "is overwrite safe for this transfer",
        "whats wrong with mapping",
        "count rows airports Local Postgres",
        "pls list tbls on Local Postgres",
    ):
        resp = agent.chat(q, data_context={"pilot_session_id": f"w48-{hash(q) % 999}"})
        assert (resp.answer or "").strip(), q
        assert "traceback" not in (resp.answer or "").lower(), q
