"""Wave 31 — multi-turn session memory, filter is=, validate triage."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import connector_store  # noqa: E402
from src.ai.copilot import tools as tools_mod  # noqa: E402
from src.ai.copilot.followup import looks_like_followup, resolve_followup  # noqa: E402
from src.ai.copilot.pilot_agent import DataPilotAgent  # noqa: E402
from src.ai.copilot.tools import infer_tools_from_message  # noqa: E402
from src.ai.copilot.working_memory import PilotFocus  # noqa: E402


def _isolated(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    connector_store._backend_choice = None
    tools_mod._tools = None


def _seed(tmp_path: Path) -> None:
    db = tmp_path / "w31.db"
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


def test_filter_where_status_is_paid_routes():
    planned = infer_tools_from_message("filter where status is paid")
    assert any(n == "filter_result" for n, _ in planned)
    args = dict(planned)["filter_result"]
    assert args.get("column") == "status"
    assert args.get("op") == "eq"
    assert args.get("value") == "paid"


def test_why_did_validate_fail_routes():
    names = [n for n, _ in infer_tools_from_message("why did validate fail")]
    assert "list_jobs" in names or "get_preflight_run" in names or "remediate_validation" in names


def test_only_paid_followup_builds_where():
    focus = PilotFocus(
        table="orders",
        connector_name="PilotSQLite",
        metric="count",
        columns=["id", "status", "amount", "region"],
    )
    assert looks_like_followup("only paid ones", focus)
    edit = resolve_followup("only paid ones", focus)
    assert edit is not None
    assert edit.table == "orders"
    assert "status" in (edit.where or "").lower()
    assert "paid" in (edit.where or "").lower()


def test_multi_turn_only_paid_after_count(monkeypatch, tmp_path):
    _isolated(monkeypatch, tmp_path)
    _seed(tmp_path)
    agent = DataPilotAgent()
    ctx = {"pilot_session_id": "wave31-only-paid"}
    first = agent.chat("how many rows in orders on PilotSQLite", data_context=ctx)
    assert "aggregate_data" in [t.get("name") for t in (first.tools_used or [])]
    second = agent.chat(
        "only paid ones",
        history=[
            {"role": "user", "content": "how many rows in orders on PilotSQLite"},
            {"role": "assistant", "content": first.answer or ""},
        ],
        data_context=ctx,
    )
    assert "aggregate_data" in [t.get("name") for t in (second.tools_used or [])]
    low = (second.answer or "").lower()
    assert "3" in low or "paid" in low
    assert "not sure" not in low


def test_multi_turn_analyze_that_after_sample(monkeypatch, tmp_path):
    _isolated(monkeypatch, tmp_path)
    _seed(tmp_path)
    agent = DataPilotAgent()
    ctx = {"pilot_session_id": "wave31-analyze"}
    first = agent.chat("sample orders on PilotSQLite", data_context=ctx)
    assert "sample_connector_object" in [t.get("name") for t in (first.tools_used or [])]
    second = agent.chat(
        "analyze that",
        history=[
            {"role": "user", "content": "sample orders on PilotSQLite"},
            {"role": "assistant", "content": first.answer or ""},
        ],
        data_context=ctx,
    )
    names = [t.get("name") for t in (second.tools_used or [])]
    assert "analyze_result" in names
    assert all(
        t.get("success") for t in (second.tools_used or []) if t.get("name") == "analyze_result"
    )
    assert "not sure" not in (second.answer or "").lower()
    assert "no stored result" not in (second.answer or "").lower()


def test_multi_turn_filter_after_sample(monkeypatch, tmp_path):
    _isolated(monkeypatch, tmp_path)
    _seed(tmp_path)
    agent = DataPilotAgent()
    ctx = {"pilot_session_id": "wave31-filter"}
    first = agent.chat("sample orders on PilotSQLite", data_context=ctx)
    second = agent.chat(
        "filter where status is paid",
        history=[
            {"role": "user", "content": "sample orders on PilotSQLite"},
            {"role": "assistant", "content": first.answer or ""},
        ],
        data_context=ctx,
    )
    assert "filter_result" in [t.get("name") for t in (second.tools_used or [])]
    low = (second.answer or "").lower()
    assert "paid" in low or "3" in low or "match" in low
    assert "not sure" not in low


def test_ephemeral_session_enables_followup_without_explicit_id(monkeypatch, tmp_path):
    _isolated(monkeypatch, tmp_path)
    _seed(tmp_path)
    agent = DataPilotAgent()
    first = agent.chat("how many rows in orders on PilotSQLite")
    second = agent.chat("only paid ones")
    assert "aggregate_data" in [t.get("name") for t in (second.tools_used or [])]
    assert "not sure" not in (second.answer or "").lower()
