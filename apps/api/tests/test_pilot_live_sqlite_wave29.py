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


def test_ds04_dataset_phrasing_routes_to_start_dataset_transfer(monkeypatch, tmp_path):
    """'transfer datasets from X to Y' names an upload, not a connector table."""
    _isolated_store(monkeypatch, tmp_path)
    from src.ai.copilot.data_analyst import get_data_analyst
    csv = tmp_path / "wave29_customers.csv"
    csv.write_text("id,name\n1,Ada\n", encoding="utf-8")
    analyst = get_data_analyst()
    monkeypatch.setattr(analyst.feeder, "upload_dirs", [str(tmp_path)])
    # The feeder caches feed_all for 60s across tests — the patch must
    # invalidate it or the upload is invisible to resolve_dataset.
    monkeypatch.setattr(analyst.feeder, "_feed_cache", None)
    monkeypatch.setattr(analyst.feeder, "_name_cache", None)

    planned = infer_tools_from_message(
        "transfer datasets from wave29_customers to Snowflake Prod"
    )
    names = [n for n, _ in planned]
    assert "start_dataset_transfer" in names
    assert "plan_transfer" not in names and "start_transfer" not in names


def test_ds05_filename_source_routes_to_dataset_transfer(monkeypatch, tmp_path):
    """'transfer customers.csv to Snowflake' must not stage a table named customers.csv."""
    _isolated_store(monkeypatch, tmp_path)
    planned = infer_tools_from_message("transfer customers.csv to Snowflake")
    names = [n for n, _ in planned]
    assert "start_dataset_transfer" in names
    args = next(a for n, a in planned if n == "start_dataset_transfer")
    assert args.get("dataset_name") == "customers.csv"
    assert args.get("dest_connector_name")


def test_r21_named_table_filter_binds_the_object():
    planned = infer_tools_from_message(
        "filter orders where status = paid on PilotSQLite"
    )
    names = [n for n, _ in planned]
    assert names == ["run_query"]
    args = planned[0][1]
    assert "orders" in args["query"]
    assert "status" in args["query"] and "paid" in args["query"]
    assert "WHERE" in args["query"].upper()
    assert args.get("connector_name")


def test_r21_unnamed_filter_stays_on_stored_result():
    planned = infer_tools_from_message("filter where status = paid")
    names = [n for n, _ in planned]
    assert names == ["filter_result"]


@pytest.mark.parametrize(
    "prompt",
    [
        "peek at orders on ghostdb",
        "give me a sample of the orders table in ghostdb",
        "rows of orders on ghostdb",
    ],
)
def test_t04_connector_hint_reaches_the_tool(prompt):
    """A named-but-wrong connector must refuse inside the tool, not silently substitute."""
    planned = infer_tools_from_message(prompt)
    names = [n for n, _ in planned]
    assert names == ["sample_connector_object"]
    assert planned[0][1].get("connector_name") == "ghostdb"


def test_c08_last_n_transfers_honors_n():
    planned = infer_tools_from_message("show last 3 transfers")
    assert planned == [("list_jobs", {"limit": 3})]


@pytest.mark.parametrize(
    "prompt",
    [
        "how many transfers have I run",
        "when was my last successful transfer",
    ],
)
def test_n05_n09_workspace_questions_reach_the_ledger(prompt):
    names = [n for n, _ in infer_tools_from_message(prompt)]
    assert names == ["list_jobs"]


def test_q02_failure_why_is_ledger_first():
    names = [n for n, _ in infer_tools_from_message("why did the transfer fail")]
    assert names == ["list_jobs"]


def test_q04_bare_hex_job_id_plans_get_job_without_open_schedule():
    planned = infer_tools_from_message(
        "what is the status of job 64f1a2b3c4d5e6f7a8b9c0d1ab"
    )
    names = [n for n, _ in planned]
    assert "get_job" in names
    assert "open_schedule" not in names


def test_ds08_local_claim_blocks_a_second_same_route_write():
    from services.mongodb_service import MongoDBService as M
    from datetime import datetime, timezone, timedelta
    M._LOCAL_CLAIMS.clear()
    exp = datetime.now(timezone.utc) + timedelta(hours=1)
    ok1, _ = M._local_claim("route-key", "job_a", exp)
    ok2, holder = M._local_claim("route-key", "job_b", exp)
    assert ok1 is True
    assert ok2 is False and holder == "job_a"
    M._local_release("route-key", "job_a")
    ok3, _ = M._local_claim("route-key", "job_b", exp)
    assert ok3 is True


def test_t19_empty_strings_are_present_not_null():
    from services.data_profiler import profile_column
    p = profile_column("name", ["Alice", "", None, "Bob", ""])
    assert p["null_rate"] == pytest.approx(0.2, abs=0.01)
    assert p["empty_string_count"] == 2


def test_stall_flag_marks_a_running_job_past_the_window():
    from services.job_status import job_stall_seconds
    from datetime import datetime, timezone, timedelta
    old = (datetime.now(timezone.utc) - timedelta(minutes=49)).isoformat()
    fresh = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    assert job_stall_seconds({"status": "running", "updated_at": old}) >= 900
    assert job_stall_seconds({"status": "running", "updated_at": fresh}) == 0.0
    assert job_stall_seconds({"status": "completed", "updated_at": old}) == 0.0
