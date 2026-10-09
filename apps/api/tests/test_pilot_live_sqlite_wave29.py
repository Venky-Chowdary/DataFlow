"""Wave 29 — live SQLite Datawrap Pilot proofs + inventory / recovery honesty.

Always-on CI path: no Postgres credentials required. Proves the local engine can
count, sample, run SQL, and recover from missing connectors/datasets against a
real sqlite connector stored like production.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import connector_store  # noqa: E402
from src.ai.copilot import tools as tools_mod  # noqa: E402
from src.ai.copilot.pilot_agent import DataPilotAgent  # noqa: E402
from src.ai.copilot.tools import infer_tools_from_message  # noqa: E402


def _sqlite_orders(tmp_path: Path) -> Path:
    db_path = tmp_path / "pilot_wave29.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE orders ("
        "id INTEGER PRIMARY KEY, status TEXT, amount REAL, region TEXT)"
    )
    conn.executemany(
        "INSERT INTO orders (id, status, amount, region) VALUES (?, ?, ?, ?)",
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
    return db_path


def _isolated_store(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    connector_store._backend_choice = None
    tools_mod._tools = None


def _seed_pilot_sqlite(tmp_path: Path) -> connector_store.SavedConnector:
    db_path = _sqlite_orders(tmp_path)
    # Windows needs sqlite:///C:/… (three slashes); never four before the drive.
    uri = f"sqlite:///{db_path.resolve().as_posix()}"
    return connector_store.create_connector({
        "name": "PilotSQLite",
        "type": "sqlite",
        "role": "both",
        "connection_string": uri,
        "workspace_id": "",
    })


@pytest.mark.parametrize(
    "prompt,expected",
    [
        ("how many jobs failed", "list_jobs"),
        ("how many connectors do I have", "list_connectors"),
        ("failed jobs", "list_jobs"),
        ("connector count", "list_connectors"),
    ],
)
def test_inventory_routes_away_from_aggregate(prompt, expected):
    names = [n for n, _ in infer_tools_from_message(prompt)]
    assert expected in names, f"{prompt!r} -> {names}"
    assert "aggregate_data" not in names


def test_run_sql_without_connector_recovers(monkeypatch, tmp_path):
    _isolated_store(monkeypatch, tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat("run sql: SELECT 1")
    assert resp.method == "pilot_local_engine"
    tool_names = [t.get("name") for t in (resp.tools_used or [])]
    assert "run_query" in tool_names
    assert "list_connectors" in tool_names
    answer = (resp.answer or "").lower()
    assert "connector" in answer
    assert resp.needs_clarification or "no saved" in answer or "which" in answer


def test_analyze_missing_dataset_recovers(monkeypatch, tmp_path):
    _isolated_store(monkeypatch, tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat("analyze the ZZZ_NO_SUCH_DATASET_wave29 data")
    assert resp.method == "pilot_local_engine"
    answer = (resp.answer or "").lower()
    # Honest miss — never invent columns for a phantom dataset.
    assert any(
        w in answer
        for w in ("dataset", "upload", "not found", "indexed", "no uploaded", "which")
    )
    tool_names = [t.get("name") for t in (resp.tools_used or [])]
    assert "list_datasets" in tool_names or "not found" in answer


def test_live_count_orders_on_sqlite(monkeypatch, tmp_path):
    _isolated_store(monkeypatch, tmp_path)
    _seed_pilot_sqlite(tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat("how many rows in orders on PilotSQLite")
    assert resp.method == "pilot_local_engine"
    tool_names = [t.get("name") for t in (resp.tools_used or [])]
    assert "aggregate_data" in tool_names
    answer = resp.answer or ""
    assert "5" in answer or "row" in answer.lower()
    assert "PilotSQLite" in answer or "orders" in answer.lower()


def test_live_group_by_status_on_sqlite(monkeypatch, tmp_path):
    _isolated_store(monkeypatch, tmp_path)
    _seed_pilot_sqlite(tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat("count of orders by status on PilotSQLite")
    assert resp.method == "pilot_local_engine"
    assert "aggregate_data" in [t.get("name") for t in (resp.tools_used or [])]
    low = (resp.answer or "").lower()
    assert "paid" in low
    assert "pending" in low or "cancelled" in low


def _sqlite_dated_orders(tmp_path: Path) -> Path:
    db_path = tmp_path / "pilot_wave29_dated.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE orders ("
        "id INTEGER PRIMARY KEY, status TEXT, hire_date TEXT, amount REAL)"
    )
    conn.executemany(
        "INSERT INTO orders (id, status, hire_date, amount) VALUES (?, ?, ?, ?)",
        [
            (1, "paid", "2023-01-04", 10.5),
            (2, "paid", "2023-02-11", 20.0),
            (3, "pending", "2023-11-30", 5.0),
            (4, "paid", "2024-01-15", 7.5),
            (5, "cancelled", "2024-03-22", 1.0),
        ],
    )
    conn.commit()
    conn.close()
    return db_path


def test_r13_group_by_year_of_column_buckets_years(monkeypatch, tmp_path):
    """QA R13 — 'year of Hire_Date' fuzzy-resolved to the bare column and
    grouped by raw day values (20 rows of count 1). It must bucket years."""
    _isolated_store(monkeypatch, tmp_path)
    db_path = _sqlite_dated_orders(tmp_path)
    uri = f"sqlite:///{db_path.resolve().as_posix()}"
    conn_rec = connector_store.create_connector({
        "name": "PilotSQLiteDates",
        "type": "sqlite",
        "role": "both",
        "connection_string": uri,
        "workspace_id": "",
    })
    from src.ai.copilot.aggregate_tools import aggregate_connector_data

    res = aggregate_connector_data(
        connector_id=conn_rec.id,
        table="orders",
        metric="count",
        group_by="year of hire_date",
    )
    assert res.success, res.error
    rows = (res.output or {}).get("rows") or []
    buckets = set()
    for r in rows:
        for k, v in r.items():
            if k not in {"row_count", "count", "value"}:
                buckets.add(str(v)[:4])
    assert buckets == {"2023", "2024"}, f"expected year buckets, got {rows}"
    assert len(rows) == 2, f"expected 2 year buckets, got {len(rows)}"


def test_r13_group_by_year_of_nontemporal_refuses(monkeypatch, tmp_path):
    """'year of status' must refuse — status is not a date column; silently
    grouping by it was the day-bucket bug in another costume."""
    _isolated_store(monkeypatch, tmp_path)
    db_path = _sqlite_dated_orders(tmp_path)
    uri = f"sqlite:///{db_path.resolve().as_posix()}"
    conn_rec = connector_store.create_connector({
        "name": "PilotSQLiteDates",
        "type": "sqlite",
        "role": "both",
        "connection_string": uri,
        "workspace_id": "",
    })
    from src.ai.copilot.aggregate_tools import aggregate_connector_data

    res = aggregate_connector_data(
        connector_id=conn_rec.id,
        table="orders",
        metric="count",
        group_by="year of status",
    )
    assert not res.success or res.error


def test_live_run_sql_on_sqlite(monkeypatch, tmp_path):
    _isolated_store(monkeypatch, tmp_path)
    _seed_pilot_sqlite(tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat(
        "run sql: SELECT status, COUNT(*) AS n FROM orders GROUP BY status "
        "on PilotSQLite"
    )
    assert resp.method == "pilot_local_engine"
    assert "run_query" in [t.get("name") for t in (resp.tools_used or [])]
    low = (resp.answer or "").lower()
    assert "paid" in low or "row" in low
    assert "not found" not in low


def test_live_sample_orders_on_sqlite(monkeypatch, tmp_path):
    _isolated_store(monkeypatch, tmp_path)
    _seed_pilot_sqlite(tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat("sample orders on PilotSQLite")
    assert resp.method == "pilot_local_engine"
    assert "sample_connector_object" in [t.get("name") for t in (resp.tools_used or [])]
    assert (resp.answer or "").strip()
    assert "not found" not in (resp.answer or "").lower()


def test_multi_turn_followup_filter_on_sqlite(monkeypatch, tmp_path):
    _isolated_store(monkeypatch, tmp_path)
    _seed_pilot_sqlite(tmp_path)
    agent = DataPilotAgent()
    first = agent.chat("how many rows in orders on PilotSQLite")
    assert "aggregate_data" in [t.get("name") for t in (first.tools_used or [])]
    assert "5" in (first.answer or "") or "row" in (first.answer or "").lower()
    history = [
        {"role": "user", "content": "how many rows in orders on PilotSQLite"},
        {"role": "assistant", "content": first.answer or ""},
    ]
    second = agent.chat("only paid ones", history=history)
    assert second.method == "pilot_local_engine"
    names = [t.get("name") for t in (second.tools_used or [])]
    low = (second.answer or "").lower()
    # Prefer tool follow-up; if clarification is needed, it must still be honest.
    if names:
        assert any(n in names for n in ("aggregate_data", "filter_result", "run_query"))
        assert "3" in low or "paid" in low or "row" in low
    else:
        assert second.needs_clarification or "paid" in low or "which" in low


def test_how_many_jobs_failed_lists_not_scans(monkeypatch, tmp_path):
    _isolated_store(monkeypatch, tmp_path)
    agent = DataPilotAgent()
    resp = agent.chat("how many jobs failed")
    names = [t.get("name") for t in (resp.tools_used or [])]
    assert "list_jobs" in names
    assert "aggregate_data" not in names
    assert "job" in (resp.answer or "").lower() or "transfer" in (resp.answer or "").lower()


def test_sqlite_url_normalize_windows_vs_unix():
    """Windows drive letters keep 3 slashes; Unix abs gets 4."""
    from connectors.generic_sql import _normalize_sqlite_url

    win = "sqlite:///C:/Users/me/data.db"
    assert _normalize_sqlite_url(win) == win
    assert _normalize_sqlite_url("sqlite:///relative.db") == "sqlite:///relative.db"
    # Remainder after sqlite:/// starts with / → Unix absolute needing 4 slashes.
    assert _normalize_sqlite_url("sqlite:////abs/path.db") == "sqlite:////abs/path.db"
    assert _normalize_sqlite_url("sqlite:///" + "/abs/path.db") == "sqlite:////abs/path.db"
