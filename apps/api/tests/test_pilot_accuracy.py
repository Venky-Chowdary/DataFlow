"""Accuracy / robustness guarantees for Data Pilot routing and scoring."""

from __future__ import annotations

from src.ai.copilot.agent import CopilotResponse
from src.ai.copilot.pilot_agent import _score_response
from src.ai.copilot.schema_tools import AmbiguousConnectorError, _match_score, _pick_connector
from src.ai.copilot.tools import infer_tools_from_message, prune_planned_tools


def _names(planned: list[tuple[str, dict]]) -> list[str]:
    return [n for n, _ in planned]


def test_connector_match_prefers_exact_name():
    assert _match_score("local postgres", "Local Postgres", "postgresql") > _match_score(
        "local postgres", "Prod Postgres", "postgresql"
    )
    assert _match_score("Local Postgres", "Local Postgres", "postgresql") >= 95


def test_ambiguous_connector_raises_instead_of_first_match():
    pool = [
        {"name": "Local Postgres", "type": "postgresql", "id": "a"},
        {"name": "Staging Postgres", "type": "postgresql", "id": "b"},
        {"name": "Prod Postgres", "type": "postgresql", "id": "c"},
    ]
    try:
        _pick_connector("postgres", pool)
        assert False, "expected AmbiguousConnectorError"
    except AmbiguousConnectorError as exc:
        assert "Which connector" in exc.message
        assert len(exc.candidates) >= 2


def test_clear_connector_winner_no_ambiguity():
    pool = [
        {"name": "Local Postgres", "type": "postgresql", "id": "a"},
        {"name": "Local MongoDB", "type": "mongodb", "id": "b"},
    ]
    chosen = _pick_connector("Local Postgres", pool)
    assert chosen["id"] == "a"


def test_schema_nl_does_not_also_analyze_dataset():
    planned = infer_tools_from_message("schema of airports on Local Postgres")
    assert "introspect_connector_schema" in _names(planned)
    assert "analyze_dataset" not in _names(planned)
    assert "search_knowledge" not in _names(planned)


def test_natural_schema_paraphrase_routes():
    planned = infer_tools_from_message(
        "what's the airports table look like in Local Postgres"
    )
    assert "introspect_connector_schema" in _names(planned)
    args = dict(planned)["introspect_connector_schema"]
    assert args.get("table") == "airports"
    assert "postgres" in (args.get("connector_name") or "").lower()


def test_prune_drops_low_priority_conflicts():
    planned = [
        ("introspect_connector_schema", {"table": "airports"}),
        ("analyze_dataset", {"dataset_name": "airports"}),
        ("search_knowledge", {"query": "airports"}),
        ("list_jobs", {"limit": 10}),
    ]
    pruned = prune_planned_tools(planned)
    names = _names(pruned)
    assert "introspect_connector_schema" in names
    assert "analyze_dataset" not in names
    assert "search_knowledge" not in names


def test_plan_transfer_parses_from_to():
    planned = infer_tools_from_message("plan transfer from Shopify to Snowflake")
    assert "plan_transfer_route" in _names(planned)
    args = dict(planned)["plan_transfer_route"]
    assert "shopify" in args.get("source", "").lower()
    assert "snowflake" in args.get("destination", "").lower()


def test_score_prefers_grounded_local_over_ungrounded_llm():
    local = CopilotResponse(
        answer="Live schema Local Postgres.`airports` — **5 columns**:",
        intent="schema",
        confidence=0.96,
        method="pilot_local_agent",
        tools_used=[{"name": "introspect_connector_schema", "success": True, "summary": "5 cols"}],
    )
    llm = CopilotResponse(
        answer=(
            "Based on typical airport schemas, you probably have id, name, "
            "iata_code, city, and country columns."
        ),
        intent="schema",
        confidence=0.94,
        method="anthropic_agent",
        tools_used=[],
    )
    assert _score_response(local) > _score_response(llm)


def test_score_clarification_does_not_beat_grounded():
    grounded = CopilotResponse(
        answer="You have **3 pipeline schedule(s)**.",
        intent="operate",
        confidence=0.96,
        method="pilot_local_agent",
        tools_used=[{"name": "list_schedules", "success": True, "summary": "3"}],
    )
    vague = CopilotResponse(
        answer="Could you clarify which pipeline you mean?",
        intent="operate",
        confidence=0.94,
        method="anthropic_agent",
        needs_clarification="Which pipeline?",
        tools_used=[],
    )
    assert _score_response(grounded) > _score_response(vague)
