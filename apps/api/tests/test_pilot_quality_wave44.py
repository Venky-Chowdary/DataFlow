"""Wave 44 — high-impact NL routing + Confirm-sensitive ops scenarios."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _names(planned):
    return [n for n, _ in planned]


def test_connector_names_keep_full_phrase():
    from src.ai.copilot.tools import infer_tools_from_message

    cases = {
        "tables on Local Postgres": ("list_connector_objects", "local postgres"),
        "list tables on Local Postgres": ("list_connector_objects", "local postgres"),
        "can you get tables from PostgresVenkat": ("list_connector_objects", "postgresvenkat"),
        "what columns are on airports on Local Postgres": ("introspect_connector_schema", "local postgres"),
        "show me some data from orders on Local Postgres": ("sample_connector_object", "local postgres"),
        "sample orders on Local Postgres": ("sample_connector_object", "local postgres"),
        "How many rows in airports on Local Postgres?": ("aggregate_data", "Local Postgres"),
    }
    for q, (tool, conn_frag) in cases.items():
        planned = infer_tools_from_message(q)
        assert tool in _names(planned), (q, planned)
        args = next(a for n, a in planned if n == tool)
        assert conn_frag.lower() in str(args.get("connector_name", "")).lower(), (q, args)
        if tool == "list_connector_objects":
            assert "sample_connector_object" not in _names(planned), q


def test_schedule_and_transfer_routes():
    from src.ai.copilot.tools import infer_tools_from_message

    p = infer_tools_from_message("run Nightly Orders now")
    assert "run_schedule_now" in _names(p)
    assert next(a for n, a in p if n == "run_schedule_now").get("name", "").lower().startswith("nightly")

    p = infer_tools_from_message("run my nightly pipeline")
    assert "run_schedule_now" in _names(p)

    p = infer_tools_from_message("move data from postgres to mysql")
    assert "start_transfer" not in _names(p) or next(
        a for n, a in p if n == "start_transfer"
    ).get("source_table") != "data"
    assert "plan_transfer_route" in _names(p)

    p = infer_tools_from_message("cdc from mysql to snowflake")
    assert "plan_transfer_route" in _names(p)
    assert "recommend_sync_mode" not in _names(p)


def test_product_and_policy_routes():
    from src.ai.copilot.tools import infer_tools_from_message

    p = infer_tools_from_message("what is upsert")
    assert "explain_product" in _names(p)
    assert "recommend_sync_mode" not in _names(p)

    p = infer_tools_from_message("schema drift on orders")
    assert "inspect_schema_policy" in _names(p)
    assert not any(
        n == "introspect_connector_schema" and (a or {}).get("table") == "drift"
        for n, a in p
    )

    for q in ("fix my mapping", "fix the mapping"):
        p = infer_tools_from_message(q)
        assert "remediate_validation" in _names(p), q
        assert "explain_mapping_assurance" not in _names(p), q


def test_sensitive_ops_stage_confirm(monkeypatch):
    """Mutations must surface pending_actions — never silent execute."""
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.copilot.tools import ToolResult

    agent = DataPilotAgent()

    def fake_execute(name, args=None):
        args = args or {}
        if name == "start_transfer":
            from src.ai.copilot.ack_ledger import get_ack_ledger

            ack = get_ack_ledger().put(
                kind="start_transfer",
                payload={"source_table": "orders", "source_connector_name": "a", "dest_connector_name": "b"},
            )
            return ToolResult(
                name=name,
                success=True,
                output={
                    "ack_id": ack,
                    "label": "Start transfer",
                    "requires_confirm": True,
                    "risk": "mutate",
                    "source_table": "orders",
                },
            )
        if name == "run_schedule_now":
            return ToolResult(
                name=name,
                success=True,
                output={
                    "action": "run_schedule",
                    "schedule_id": "sch_test",
                    "name": "Nightly",
                    "label": "Run pipeline now",
                    "risk": "mutate",
                    "requires_confirm": True,
                },
            )
        if name == "remediate_validation":
            return ToolResult(
                name=name,
                success=True,
                output={"kind": "review_mappings", "label": "Review mappings", "run_id": ""},
            )
        if name == "create_connector":
            from src.ai.copilot.ack_ledger import get_ack_ledger

            ack = get_ack_ledger().put(
                kind="create_connector",
                payload={"name": "Demo PG", "type": "postgresql"},
            )
            return ToolResult(
                name=name,
                success=True,
                output={
                    "ack_id": ack,
                    "label": "Save connector",
                    "requires_confirm": True,
                    "risk": "mutate",
                    "preview": {"name": "Demo PG", "type": "postgresql"},
                },
            )
        if name == "navigate":
            return ToolResult(name=name, success=True, output={"screen": args.get("screen") or "transfer"})
        return ToolResult(name=name, success=True, output={})

    monkeypatch.setattr(agent.tools, "execute", fake_execute)

    cases = (
        ("transfer orders from Local Postgres to Warehouse", {"start_transfer"}),
        ("run Nightly Orders now", {"run_schedule"}),
        ("fix my mapping", {"studio"}),
        (
            "create a postgres connector named Demo PG host localhost "
            "user demo password secret database appdb",
            {"create_connector"},
        ),
    )
    for msg, types in cases:
        resp = agent.chat(msg, data_context={"pilot_session_id": f"wave44-{hash(msg) % 9999}"})
        assert resp.pending_actions, (msg, resp.method, resp.answer[:120], resp.tools_used)
        assert any(
            a.get("risk") == "mutate" and a.get("type") in types
            for a in resp.pending_actions
        ), (msg, resp.pending_actions)
        assert "confirm" in (resp.answer or "").lower() or resp.pending_actions


def test_status_singular_does_not_break_column_resolve():
    from src.ai.copilot.aggregate_tools import resolve_name

    cols = ["id", "status", "amount"]
    assert resolve_name("status", cols) == "status"
    assert resolve_name("statuses", cols) == "status"


def test_sync_mode_choice_vs_definition():
    from src.ai.copilot.tools import infer_tools_from_message

    choose = _names(infer_tools_from_message("what sync mode should I use for CDC?"))
    assert "recommend_sync_mode" in choose
    assert "search_knowledge" not in choose

    define = _names(infer_tools_from_message("what is upsert"))
    assert "explain_product" in define
    assert "recommend_sync_mode" not in define
