"""Wave 30 — elliptical aggregates, RAG companions, single-table auto-resolve."""

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
from src.ai.copilot.pilot_agent import DataPilotAgent  # noqa: E402
from src.ai.copilot.tools import infer_tools_from_message  # noqa: E402


def _isolated_store(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    connector_store._backend_choice = None
    tools_mod._tools = None


def _seed_orders(tmp_path: Path) -> None:
    db_path = tmp_path / "wave30.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE orders (id INTEGER, status TEXT, amount REAL, region TEXT)"
    )
    conn.executemany(
        "INSERT INTO orders VALUES (?,?,?,?)",
        [
            (1, "paid", 10.5, "east"),
            (2, "paid", 20.0, "west"),
            (3, "pending", 5.0, "east"),
            (4, "paid", 7.5, "east"),
            (5, "cancelled", 1.0, "west"),
        ],
    )
    conn.commit()
    conn.close()
    connector_store.create_connector({
        "name": "PilotSQLite",
        "type": "sqlite",
        "role": "both",
        "connection_string": f"sqlite:///{db_path.resolve().as_posix()}",
        "workspace_id": "",
    })


def test_elliptical_sum_by_region_routes():
    planned = infer_tools_from_message("sum amount by region on PilotSQLite")
    assert planned
    assert planned[0][0] == "aggregate_data"
    args = planned[0][1]
    assert args.get("column") == "amount"
    assert args.get("group_by") == "region"
    assert "PilotSQLite" in str(args.get("connector_name"))
    # Table may be omitted — tool auto-resolves when connector has one table.
    assert not args.get("table")


def test_top_n_plural_dimension_singularizes():
    req = parse_aggregation_request("top 3 regions by amount on PilotSQLite")
    assert req is not None
    assert req.group_by == "region"
    assert req.column == "amount"
    assert req.limit == 3


def test_suggest_improvements_keeps_rag_companion():
    names = [n for n, _ in infer_tools_from_message("suggest improvements for my data")]
    assert "profile_quality_rules" in names
    assert "search_knowledge" in names


def test_live_elliptical_sum_auto_picks_single_table(monkeypatch, tmp_path):
    _isolated_store(monkeypatch, tmp_path)
    _seed_orders(tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat("sum amount by region on PilotSQLite")
    assert resp.method == "pilot_local_engine"
    assert "aggregate_data" in [t.get("name") for t in (resp.tools_used or [])]
    low = (resp.answer or "").lower()
    assert "east" in low and "west" in low
    # east: 10.5+5+7.5=23, west: 20+1=21
    assert "23" in (resp.answer or "") or "21" in (resp.answer or "")


def test_live_top_regions_by_amount(monkeypatch, tmp_path):
    _isolated_store(monkeypatch, tmp_path)
    _seed_orders(tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat("top 3 regions by amount on PilotSQLite")
    assert "aggregate_data" in [t.get("name") for t in (resp.tools_used or [])]
    low = (resp.answer or "").lower()
    assert "east" in low or "west" in low
    assert "which table" not in low


def test_schema_of_orders_composes(monkeypatch, tmp_path):
    _isolated_store(monkeypatch, tmp_path)
    _seed_orders(tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat("schema of orders on PilotSQLite")
    assert "introspect_connector_schema" in [t.get("name") for t in (resp.tools_used or [])]
    low = (resp.answer or "").lower()
    assert "amount" in low or "status" in low
    assert "column" in low


def test_inventory_still_beats_elliptical_how_many_jobs():
    names = [n for n, _ in infer_tools_from_message("how many jobs failed")]
    assert "list_jobs" in names
    assert "aggregate_data" not in names
