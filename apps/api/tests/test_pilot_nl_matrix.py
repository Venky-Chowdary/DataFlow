"""NL → tools / pending_actions matrix for Data Pilot (product scope)."""

from __future__ import annotations

from src.ai.copilot.pilot_agent import DataPilotAgent, PilotTurn
from src.ai.copilot.tools import ToolResult, get_pilot_tools, infer_tools_from_message


def _names(planned: list[tuple[str, dict]]) -> list[str]:
    return [n for n, _ in planned]


def test_navigate_all_product_screens():
    cases = {
        "take me to overview": "dashboard",
        "go to pipelines": "schedules",
        "open contracts": "contracts",
        "show query playground": "query",
        "go to docs": "docs",
        "open proofs": "benchmarks",
        "take me to jobs": "jobs",
        "go to connectors": "connectors",
        "open transfer studio": "transfer",
    }
    for phrase, screen in cases.items():
        planned = infer_tools_from_message(phrase)
        nav = [a for n, a in planned if n == "navigate"]
        assert nav, f"expected navigate for {phrase!r}, got {planned}"
        assert nav[0].get("screen") == screen, f"{phrase!r} → {nav}"


def test_knowledge_and_mapping_tools():
    assert "explain_mapping_assurance" in _names(
        infer_tools_from_message("how does mapping assurance work?")
    )
    assert "get_transfer_capabilities" in _names(
        infer_tools_from_message("what any to any capabilities do you support?")
    )


def test_list_schedules_and_run_pending():
    assert "list_schedules" in _names(infer_tools_from_message("show my pipelines"))
    planned = infer_tools_from_message("run schedule Test now")
    assert "run_schedule_now" in _names(planned)


def test_remediate_goes_to_pending_not_auto_studio():
    agent = DataPilotAgent()
    turn = PilotTurn()
    tr = ToolResult(
        name="remediate_validation",
        success=True,
        output={
            "action": "studio",
            "kind": "quarantine_and_rerun",
            "label": "Quarantine",
            "risk": "mutate",
            "requires_confirm": True,
        },
    )
    agent._append_tool_actions(turn, tr)
    assert turn.pending_actions, "remediation must be pending"
    assert all(a.get("type") != "studio" for a in turn.actions)
    assert any(a.get("type") == "navigate" and a.get("screen") == "transfer" for a in turn.actions)


def test_navigate_tool_accepts_pipelines_alias():
    tools = get_pilot_tools()
    tr = tools.execute("navigate", {"screen": "pipelines"})
    assert tr.success
    assert tr.output["screen"] == "schedules"


def test_meta_pilot_lists_tools():
    tools = get_pilot_tools()
    tr = tools.execute("describe_pilot", {})
    assert tr.success
    assert "tools" in tr.output
    assert "list_schedules" in tr.output["tools"]
    assert "introspect_connector_schema" in tr.output["tools"]
    assert "schedules" in tr.output["screens"]


def test_nl_routes_live_schema_tools():
    planned = infer_tools_from_message("schema of airports on Local Postgres")
    assert "introspect_connector_schema" in _names(planned)
    args = dict(planned)["introspect_connector_schema"]
    assert args.get("table") == "airports"
    assert "postgres" in (args.get("connector_name") or "").lower()

    planned2 = infer_tools_from_message("show tables on Local Postgres")
    assert "list_connector_objects" in _names(planned2)

    planned3 = infer_tools_from_message(
        "diff airports on Local Postgres vs data on LocalMongoDB"
    )
    assert "diff_schemas" in _names(planned3)


def test_diff_schemas_uses_classify(monkeypatch):
    from src.ai.copilot import schema_tools

    def fake_introspect(connector_id="", connector_name="", table=""):
        from src.ai.copilot.tools import ToolResult

        if "mongo" in (connector_name or "").lower() or table == "data":
            return ToolResult(
                name="introspect_connector_schema",
                success=True,
                output={
                    "connector_name": connector_name or "dest",
                    "table": table,
                    "columns": [
                        {"name": "id", "inferred_type": "INTEGER", "nullable": True},
                        {"name": "extra", "inferred_type": "TEXT", "nullable": True},
                    ],
                    "schema_map": {"id": "INTEGER", "extra": "TEXT"},
                },
            )
        return ToolResult(
            name="introspect_connector_schema",
            success=True,
            output={
                "connector_name": connector_name or "src",
                "table": table,
                "columns": [
                    {"name": "id", "inferred_type": "INTEGER", "nullable": True},
                    {"name": "name", "inferred_type": "TEXT", "nullable": True},
                ],
                "schema_map": {"id": "INTEGER", "name": "TEXT"},
            },
        )

    monkeypatch.setattr(schema_tools, "introspect_connector_schema", fake_introspect)
    tr = schema_tools.diff_schemas(
        source_connector_name="Local Postgres",
        source_table="airports",
        dest_connector_name="LocalMongoDB",
        dest_table="data",
    )
    assert tr.success
    assert "name" in (tr.output or {}).get("only_in_source", [])
    assert "extra" in (tr.output or {}).get("only_in_destination", [])
    assert (tr.output or {}).get("severity") in {"additive", "breaking", "none"}


def test_nl_routes_map_connector_schemas():
    planned = infer_tools_from_message(
        "map e2e_customers on Local Postgres to data on LocalMongoDB"
    )
    assert "map_connector_schemas" in _names(planned)
    args = dict(planned)["map_connector_schemas"]
    assert args.get("source_table") == "e2e_customers"
    assert "postgres" in (args.get("source_connector_name") or "").lower()


def test_map_connector_schemas_uses_semantic_mapper(monkeypatch):
    from src.ai.copilot import schema_tools

    def fake_introspect(connector_id="", connector_name="", table=""):
        from src.ai.copilot.tools import ToolResult

        if "mongo" in (connector_name or "").lower() or table == "data":
            return ToolResult(
                name="introspect_connector_schema",
                success=True,
                output={
                    "connector_name": "LocalMongoDB",
                    "table": table,
                    "columns": [
                        {"name": "id", "inferred_type": "INTEGER"},
                        {"name": "email_addr", "inferred_type": "TEXT"},
                    ],
                    "schema_map": {"id": "INTEGER", "email_addr": "TEXT"},
                },
            )
        return ToolResult(
            name="introspect_connector_schema",
            success=True,
            output={
                "connector_name": "Local Postgres",
                "table": table,
                "columns": [
                    {"name": "id", "inferred_type": "INTEGER"},
                    {"name": "email", "inferred_type": "TEXT"},
                ],
                "schema_map": {"id": "INTEGER", "email": "TEXT"},
            },
        )

    monkeypatch.setattr(schema_tools, "introspect_connector_schema", fake_introspect)
    tr = schema_tools.map_connector_schemas(
        source_connector_name="Local Postgres",
        source_table="e2e_customers",
        dest_connector_name="LocalMongoDB",
        dest_table="data",
    )
    assert tr.success
    assert (tr.output or {}).get("mapping_count", 0) >= 1
    sources = {m["source"] for m in (tr.output or {}).get("mappings") or []}
    assert "id" in sources
