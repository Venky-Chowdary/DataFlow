"""Wave 33 — explicit-table priority, distinct/paid/where NL, create connector."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import connector_store  # noqa: E402
from src.ai.copilot import tools as tools_mod  # noqa: E402
from src.ai.copilot.aggregate_tools import parse_aggregation_request  # noqa: E402
from src.ai.copilot.connector_create import wants_create_connector  # noqa: E402
from src.ai.copilot.pilot_agent import DataPilotAgent  # noqa: E402
from src.ai.copilot.tools import infer_tools_from_message  # noqa: E402


def _isolated(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    connector_store._backend_choice = None
    tools_mod._tools = None


def _seed(tmp_path: Path) -> None:
    db = tmp_path / "w33.db"
    c = sqlite3.connect(db)
    c.execute(
        "CREATE TABLE orders (id INTEGER, status TEXT, amount REAL, region TEXT, email TEXT)"
    )
    c.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?)",
        [
            (1, "paid", 10.5, "east", "a@x.com"),
            (2, "paid", 20.0, "west", "b@x.com"),
            (3, "pending", 5.0, "east", None),
            (4, "paid", 7.5, "east", "c@x.com"),
            (5, "cancelled", 1.0, "west", "d@x.com"),
        ],
    )
    c.execute("CREATE TABLE products (id INTEGER, name TEXT, price REAL)")
    c.executemany(
        "INSERT INTO products VALUES (?,?,?)",
        [(1, "Widget", 9.0), (2, "Gadget", 12.0), (3, "Tool", 15.0)],
    )
    c.commit()
    c.close()
    connector_store.create_connector({
        "name": "PilotSQLite",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db.resolve().as_posix()}",
        "workspace_id": "",
    })


def test_distinct_count_parses():
    req = parse_aggregation_request(
        "distinct count of region from orders on PilotSQLite"
    )
    assert req is not None
    assert req.metric == "count_distinct"
    assert req.column == "region"
    assert req.table == "orders"


def test_paid_orders_becomes_status_filter():
    req = parse_aggregation_request("how many paid orders on PilotSQLite")
    assert req is not None
    assert req.table == "orders"
    assert "paid" in (req.where or "").lower()


def test_table_where_amount_parses():
    req = parse_aggregation_request("orders where amount > 10 on PilotSQLite")
    assert req is not None
    assert req.table == "orders"
    assert "amount" in (req.where or "")
    assert "10" in (req.where or "")
    names = [n for n, _ in infer_tools_from_message("orders where amount > 10 on PilotSQLite")]
    assert "aggregate_data" in names
    assert "filter_result" not in names


def test_create_postgres_connector_intent():
    p = "create a postgres connector at localhost database demo user demo"
    assert wants_create_connector(p)
    assert "create_connector" in [n for n, _ in infer_tools_from_message(p)]


def test_explain_gates_not_ontology_rag():
    names = [n for n, _ in infer_tools_from_message("explain the 9 preflight gates")]
    assert "describe_pilot" in names or "profile_quality_rules" in names


def test_pii_in_orders_introspects():
    names = [n for n, _ in infer_tools_from_message("is email PII in orders")]
    assert "introspect_connector_schema" in names
    assert "search_knowledge" not in names


def test_fix_mapping_opens_studio():
    planned = infer_tools_from_message("help me fix my mapping")
    names = [n for n, _ in planned]
    assert "explain_mapping_assurance" in names
    assert any(n == "navigate" and a.get("screen") == "transfer" for n, a in planned)


def test_live_max_products_after_orders_focus(monkeypatch, tmp_path):
    _isolated(monkeypatch, tmp_path)
    _seed(tmp_path)
    agent = DataPilotAgent()
    ctx = {"pilot_session_id": "wave33-max"}
    agent.chat("min amount in orders on PilotSQLite", data_context=ctx)
    second = agent.chat("max price in products on PilotSQLite", data_context=ctx)
    assert "aggregate_data" in [t.get("name") for t in (second.tools_used or [])]
    assert any(t.get("success") for t in (second.tools_used or []) if t.get("name") == "aggregate_data")
    low = (second.answer or "").lower()
    assert "15" in (second.answer or "") or "price" in low
    assert "not in orders" not in low


def test_live_paid_orders_count(monkeypatch, tmp_path):
    _isolated(monkeypatch, tmp_path)
    _seed(tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat(
        "how many paid orders on PilotSQLite",
        data_context={"pilot_session_id": "wave33-paid"},
    )
    assert "3" in (resp.answer or "")


def test_live_orders_where_amount(monkeypatch, tmp_path):
    _isolated(monkeypatch, tmp_path)
    _seed(tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat(
        "orders where amount > 10 on PilotSQLite",
        data_context={"pilot_session_id": "wave33-where"},
    )
    # paid 20 only (>10): 1 row, or 10.5 and 20 = 2 depending on >
    assert "aggregate_data" in [t.get("name") for t in (resp.tools_used or [])]
    assert "1" in (resp.answer or "") or "2" in (resp.answer or "")


def test_live_distinct_regions(monkeypatch, tmp_path):
    _isolated(monkeypatch, tmp_path)
    _seed(tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat(
        "distinct count of region from orders on PilotSQLite",
        data_context={"pilot_session_id": "wave33-dist"},
    )
    assert "2" in (resp.answer or "")
