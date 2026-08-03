"""Wave 32 — quality gates honesty, where inheritance, upsert sync, validate triage."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import connector_store  # noqa: E402
from src.ai.copilot import tools as tools_mod  # noqa: E402
from src.ai.copilot.pilot_agent import DataPilotAgent  # noqa: E402
from src.ai.copilot.tools import get_pilot_tools, infer_tools_from_message  # noqa: E402


def _isolated(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    connector_store._backend_choice = None
    tools_mod._tools = None


def _seed(tmp_path: Path) -> None:
    db = tmp_path / "w32.db"
    c = sqlite3.connect(db)
    c.execute("CREATE TABLE orders (id INTEGER, status TEXT, amount REAL, region TEXT)")
    c.executemany(
        "INSERT INTO orders VALUES (?,?,?,?)",
        [
            (1, "paid", 10.5, "east"),
            (2, "paid", 20.0, "west"),
            (3, "pending", 5.0, "east"),
            (4, "paid", 7.5, "east"),
            (5, "cancelled", 1.0, "west"),
        ],
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


def test_product_quality_gates_not_empty_profile():
    names = [n for n, _ in infer_tools_from_message("what quality gates do you have?")]
    assert "describe_pilot" in names or "search_knowledge" in names
    # Must not be only an empty dataset profile.
    assert names != ["profile_quality_rules"]


def test_upsert_with_pk_recommends_upsert():
    tools = get_pilot_tools()
    tr = tools.execute(
        "recommend_sync_mode",
        {
            "workload": "recommend sync mode for upsert with primary key",
            "has_primary_key": True,
        },
    )
    assert tr.success
    mode = (tr.output or {}).get("recommended_mode") or ""
    assert "upsert" in mode.lower()


def test_quarantine_uses_distinct_label():
    tools = get_pilot_tools()
    tr = tools.execute("remediate_validation", {"kind": "quarantine_and_rerun"})
    assert tr.success
    assert "quarantine" in ((tr.output or {}).get("label") or "").lower()


def test_validate_fail_keeps_list_jobs():
    names = [n for n, _ in infer_tools_from_message("why did validate fail")]
    assert "list_jobs" in names
    assert "remediate_validation" in names


def test_where_survives_metric_followup(monkeypatch, tmp_path):
    _isolated(monkeypatch, tmp_path)
    _seed(tmp_path)
    agent = DataPilotAgent()
    ctx = {"pilot_session_id": "wave32-where"}
    agent.chat("how many rows in orders on PilotSQLite", data_context=ctx)
    agent.chat("only paid ones", data_context=ctx)
    third = agent.chat("what about average amount", data_context=ctx)
    assert "aggregate_data" in [t.get("name") for t in (third.tools_used or [])]
    # Paid-only average of amount: (10.5+20+7.5)/3 = 12.666...
    ans = third.answer or ""
    low = ans.lower()
    assert "12" in ans or "12.6" in ans or "12,6" in ans
    assert "average" in low or "avg" in low
    assert "orders" in low or "pilotsqlite" in low


def test_suggest_with_live_focus_samples(monkeypatch, tmp_path):
    _isolated(monkeypatch, tmp_path)
    _seed(tmp_path)
    agent = DataPilotAgent()
    ctx = {"pilot_session_id": "wave32-suggest"}
    agent.chat("how many rows in orders on PilotSQLite", data_context=ctx)
    resp = agent.chat("suggest improvements for my data", data_context=ctx)
    names = [t.get("name") for t in (resp.tools_used or [])]
    assert "profile_quality_rules" in names
    # Recovery should sample the focused live table.
    assert "sample_connector_object" in names or "list_datasets" in names
    assert "not sure" not in (resp.answer or "").lower()


def test_compare_miss_recovers_with_lists(monkeypatch, tmp_path):
    _isolated(monkeypatch, tmp_path)
    _seed(tmp_path)
    agent = DataPilotAgent()
    ctx = {"pilot_session_id": "wave32-compare"}
    agent.chat("list tables on PilotSQLite", data_context=ctx)
    resp = agent.chat("compare orders and products", data_context=ctx)
    names = [t.get("name") for t in (resp.tools_used or [])]
    assert "compare_datasets" in names
    assert "list_datasets" in names or "list_connector_objects" in names
    low = (resp.answer or "").lower()
    assert "not found" in low or "orders" in low or "dataset" in low
