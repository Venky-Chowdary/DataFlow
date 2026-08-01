"""Wave 90 — conversational working memory + MongoDB aggregation pipelines.

Grounded in:
* CoSQL / SParC dialogue acts (coreference, ellipsis, clarification)
* the 2026 multi-turn Text-to-SQL memory study (structured working memory +
  context-resolution layer, not raw transcript replay)
* MongoDB $group / $dateTrunc docs (exact server-side totals)

Live Postgres / MongoDB tests skip cleanly when the engine is unreachable.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from src.ai.copilot.aggregate_tools import (
    _mongo_pipeline,
    parse_aggregation_request,
)
from src.ai.copilot.followup import (
    clarification_slot,
    focus_from_tool_output,
    inherit_focus_slots,
    looks_like_followup,
    resolve_followup,
    resolve_pending_answer,
)
from src.ai.copilot.working_memory import (
    PendingSlot,
    PilotFocus,
    PilotWorkingMemory,
)


@pytest.fixture
def memory(tmp_path: Path) -> PilotWorkingMemory:
    store = PilotWorkingMemory(path=tmp_path / "mem.json", ttl_sec=3600)
    store.clear_for_tests()
    return store


# --------------------------------------------------------------------------
# Working memory persistence
# --------------------------------------------------------------------------


def test_focus_survives_round_trip(memory: PilotWorkingMemory):
    memory.remember_focus(
        "s1",
        PilotFocus(
            connector_name="Local Postgres",
            table="orders",
            metric="sum",
            column="amount",
            group_by="status",
        ),
    )
    got = memory.get_focus("s1")
    assert got is not None
    assert got.table == "orders"
    assert got.metric == "sum"
    assert got.group_by == "status"


def test_empty_group_by_is_remembered_as_cleared(memory: PilotWorkingMemory):
    memory.remember_focus(
        "s1",
        PilotFocus(connector_name="PG", table="orders", metric="avg", column="amount", group_by="region"),
    )
    memory.update_focus("s1", group_by="", grain="")
    got = memory.get_focus("s1")
    assert got is not None
    assert got.group_by == ""
    assert got.column == "amount"  # untouched


def test_sessions_do_not_leak(memory: PilotWorkingMemory):
    memory.remember_focus("alice", PilotFocus(table="a", connector_name="A"))
    memory.remember_focus("bob", PilotFocus(table="b", connector_name="B"))
    assert memory.get_focus("alice").table == "a"
    assert memory.get_focus("bob").table == "b"
    assert memory.get_focus("") is None


# --------------------------------------------------------------------------
# Follow-up resolution (CoSQL phenomena)
# --------------------------------------------------------------------------


def _focus(**kw) -> PilotFocus:
    base = dict(
        connector_name="Local Postgres",
        table="orders",
        metric="count",
        column="",
        group_by="status",
        limit=20,
        descending=True,
    )
    base.update(kw)
    return PilotFocus(**base)


@pytest.mark.parametrize(
    "message,expect",
    [
        ("and by region?", {"group_by": "region", "metric": "count"}),
        ("average amount instead", {"metric": "avg", "column": "amount", "group_by": "status"}),
        ("top 3", {"limit": 3}),
        ("no grouping", {"group_by": ""}),
        ("how many rows in it", {"metric": "count", "table": "orders"}),
        ("same for products", {"table": "products", "column": "", "group_by": ""}),
        ("max amount", {"metric": "max", "column": "amount"}),
        ("sum instead", {"metric": "sum"}),
    ],
)
def test_followup_edits_slots(message, expect):
    edit = resolve_followup(message, _focus())
    assert edit is not None, f"expected edit for {message!r}"
    for key, value in expect.items():
        assert getattr(edit, key) == value, f"{message}: {key}={getattr(edit, key)!r} != {value!r}"


def test_unrelated_prompt_is_not_a_followup():
    assert resolve_followup("list my connectors", _focus()) is None
    assert looks_like_followup("why did job abc fail?", _focus()) is False


def test_followup_without_focus_is_none():
    assert resolve_followup("and by region?", None) is None
    assert resolve_followup("and by region?", PilotFocus()) is None


def test_clarification_answer_fills_connector_slot():
    pending = PendingSlot(
        tool="aggregate_data",
        args={"table": "orders", "metric": "count"},
        missing="connector_name",
        question="Which connector did you mean?",
        candidates=["Local Postgres", "Prod Warehouse"],
    )
    assert resolve_pending_answer("Local Postgres", pending) == (
        "aggregate_data",
        {"table": "orders", "metric": "count", "connector_name": "Local Postgres"},
    )
    assert resolve_pending_answer("the first", pending)[1]["connector_name"] == "Local Postgres"
    # A long new question must not be swallowed as an answer.
    assert resolve_pending_answer("count of orders by status on Local Postgres", pending) is None


def test_inherit_focus_fills_omitted_connector_and_table():
    planned = [("aggregate_data", {"metric": "avg", "column": "price", "table": "products"})]
    out = inherit_focus_slots(planned, _focus(connector_id="c1", connector_name="Local Postgres"))
    assert out[0][1]["connector_id"] == "c1"
    assert out[0][1]["table"] == "products"  # user-named wins


def test_focus_from_aggregate_keeps_cleared_group_by():
    update = focus_from_tool_output(
        "aggregate_data",
        {
            "connector_id": "c1",
            "connector_name": "PG",
            "type": "postgresql",
            "table": "orders",
            "metric": "avg",
            "column": "amount",
            "group_by": None,
            "grain": None,
            "result_id": "pr_1",
        },
    )
    assert update["group_by"] == ""
    assert update["metric"] == "avg"
    assert update["table"] == "orders"


def test_clarification_slot_extracts_candidates():
    slot = clarification_slot(
        "aggregate_data",
        {"table": "orders", "metric": "count"},
        "Which connector did you mean? **Local Postgres**, **Staging**.",
    )
    assert slot is not None
    assert slot.missing == "connector_name"
    assert "Local Postgres" in slot.candidates


def test_coreferent_table_is_stripped_from_fresh_parse():
    req = parse_aggregation_request("how many rows in it")
    assert req is not None
    assert req.table == ""
    assert "table" in req.missing


# --------------------------------------------------------------------------
# MongoDB pipeline shape (read-only, exact-total semantics)
# --------------------------------------------------------------------------


def test_mongo_count_pipeline_is_sum_one():
    pipe = _mongo_pipeline(
        metric="count",
        measure_col="",
        dim_col="",
        grain="",
        dim_alias="",
        metric_alias="row_count",
        limit=20,
        descending=True,
    )
    assert pipe[0] == {"$group": {"_id": None, "row_count": {"$sum": 1}}}
    assert "$out" not in json.dumps(pipe)
    assert "$merge" not in json.dumps(pipe)


def test_mongo_grouped_sum_sorts_and_limits():
    pipe = _mongo_pipeline(
        metric="sum",
        measure_col="amount",
        dim_col="region",
        grain="",
        dim_alias="region",
        metric_alias="total_amount",
        limit=5,
        descending=True,
    )
    assert pipe[0]["$group"]["_id"] == "$region"
    assert pipe[0]["$group"]["total_amount"] == {"$sum": "$amount"}
    assert pipe[1] == {"$sort": {"total_amount": -1}}
    assert pipe[2] == {"$limit": 5}
    assert pipe[3]["$project"]["region"] == "$_id"


def test_mongo_month_grain_uses_date_trunc():
    pipe = _mongo_pipeline(
        metric="count",
        measure_col="",
        dim_col="ordered_at",
        grain="month",
        dim_alias="ordered_at_month",
        metric_alias="row_count",
        limit=20,
        descending=True,
    )
    group_id = pipe[0]["$group"]["_id"]
    assert group_id == {"$dateTrunc": {"date": "$ordered_at", "unit": "month"}}


def test_mongo_count_distinct_drops_nulls_like_sql():
    pipe = _mongo_pipeline(
        metric="count_distinct",
        measure_col="status",
        dim_col="",
        grain="",
        dim_alias="",
        metric_alias="distinct_status",
        limit=20,
        descending=True,
    )
    assert pipe[0] == {"$match": {"status": {"$ne": None}}}
    assert pipe[1] == {"$group": {"_id": "$status"}}
    assert pipe[2] == {"$count": "distinct_status"}


# --------------------------------------------------------------------------
# End-to-end agent planning (no live DB)
# --------------------------------------------------------------------------


def test_plan_with_memory_resolves_ellipsis(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_MEMORY_PATH", str(tmp_path / "agent_mem.json"))
    # Reset the singleton so the env path is picked up.
    import src.ai.copilot.working_memory as wm

    wm._memory = None
    memory = wm.get_working_memory()
    memory.clear_for_tests()
    memory.remember_focus(
        "sess-a",
        PilotFocus(
            connector_name="Local Postgres",
            table="orders",
            metric="count",
            group_by="status",
        ),
    )

    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    planned = agent._plan_with_memory(
        "and by region?",
        {"pilot_session_id": "sess-a"},
    )
    assert planned == [
        (
            "aggregate_data",
            {
                "metric": "count",
                "limit": 20,
                "table": "orders",
                "group_by": "region",
                "connector_name": "Local Postgres",
            },
        )
    ]


def test_plan_with_memory_answers_clarification(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_MEMORY_PATH", str(tmp_path / "agent_mem2.json"))
    import src.ai.copilot.working_memory as wm

    wm._memory = None
    memory = wm.get_working_memory()
    memory.clear_for_tests()
    memory.remember_pending(
        "sess-b",
        PendingSlot(
            tool="aggregate_data",
            args={"table": "orders", "metric": "count"},
            missing="connector_name",
            candidates=["Local Postgres", "Staging"],
            question="Which connector?",
        ),
    )

    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    planned = agent._plan_with_memory("Local Postgres", {"pilot_session_id": "sess-b"})
    assert planned[0][0] == "aggregate_data"
    assert planned[0][1]["connector_name"] == "Local Postgres"
    assert memory.get_pending("sess-b") is None


# --------------------------------------------------------------------------
# Optional live engines
# --------------------------------------------------------------------------


def _pg_reachable() -> bool:
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            dbname=os.environ.get("PG_DB", "dataflow"),
            user=os.environ.get("PG_USER", "dataflow"),
            password=os.environ.get("PG_PASSWORD", "dataflow"),
            connect_timeout=2,
        )
        conn.close()
        return True
    except Exception:
        return False


def _mongo_reachable() -> bool:
    try:
        import pymongo

        cli = pymongo.MongoClient(
            os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
            serverSelectionTimeoutMS=2000,
        )
        cli.admin.command("ping")
        cli.close()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _pg_reachable(), reason="Postgres not reachable")
def test_live_pg_multi_turn_clears_grouping(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_MEMORY_PATH", str(tmp_path / "live_pg_mem.json"))
    import src.ai.copilot.working_memory as wm
    import src.ai.copilot.pilot_agent as pa

    wm._memory = None
    pa._pilot = None
    memory = wm.get_working_memory()
    memory.clear_for_tests()

    import psycopg2
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.pilot_agent import get_pilot_agent

    table = f"wave90_{uuid.uuid4().hex[:6]}"
    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "dataflow"),
        user=os.environ.get("PG_USER", "dataflow"),
        password=os.environ.get("PG_PASSWORD", "dataflow"),
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE TABLE "{table}" '
            "(id serial, region text, amount numeric(10,2))"
        )
        cur.executemany(
            f'INSERT INTO "{table}" (region, amount) VALUES (%s, %s)',
            [("emea", 10), ("emea", 20), ("apac", 5)],
        )
    conn_name = f"Wave90PG{table[-6:]}"
    saved = create_connector(
        {
            "name": conn_name,
            "type": "postgresql",
            "host": os.environ.get("PG_HOST", "localhost"),
            "port": int(os.environ.get("PG_PORT", "5432")),
            "database": os.environ.get("PG_DB", "dataflow"),
            "username": os.environ.get("PG_USER", "dataflow"),
            "password": os.environ.get("PG_PASSWORD", "dataflow"),
            "schema": "public",
        }
    )
    agent = get_pilot_agent()
    ctx = {"pilot_session_id": f"wave90-{table}"}
    try:
        r1 = agent.chat(
            f"sum of amount in {table} by region on {conn_name}",
            history=[],
            data_context=ctx,
        )
        assert any(t.get("name") == "aggregate_data" and t.get("success") for t in r1.tools_used), r1.answer

        r2 = agent.chat("no grouping", history=[], data_context=ctx)
        assert any(t.get("name") == "aggregate_data" and t.get("success") for t in r2.tools_used), r2.answer
        assert "grouped by" not in r2.answer.lower()
        assert "35" in r2.answer  # 10+20+5

        r3 = agent.chat("how many rows in it", history=[], data_context=ctx)
        assert any(t.get("name") == "aggregate_data" and t.get("success") for t in r3.tools_used), r3.answer
        assert "grouped by" not in r3.answer.lower()
        assert "3" in r3.answer
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()
        delete_connector(saved.id)


@pytest.mark.skipif(not _mongo_reachable(), reason="MongoDB not reachable")
def test_live_mongo_aggregates_match_driver(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_MEMORY_PATH", str(tmp_path / "live_mongo_mem.json"))
    import datetime

    import pymongo
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.tools import DataPilotTools

    cli = pymongo.MongoClient(
        os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
        serverSelectionTimeoutMS=3000,
    )
    db = cli["dataflow_wave90"]
    coll = f"orders_{uuid.uuid4().hex[:6]}"
    docs = [
        {"status": "open", "region": "emea", "amount": 10.5, "ordered_at": datetime.datetime(2024, 1, 5)},
        {"status": "open", "region": "emea", "amount": 20.25, "ordered_at": datetime.datetime(2024, 1, 11)},
        {"status": "open", "region": "apac", "amount": 5.0, "ordered_at": datetime.datetime(2024, 2, 2)},
        {"status": "closed", "region": "emea", "amount": 30.0, "ordered_at": datetime.datetime(2024, 2, 14)},
        {"status": "closed", "region": "apac", "amount": 1.25, "ordered_at": datetime.datetime(2024, 3, 3)},
        {"region": "emea", "amount": 4.0, "ordered_at": datetime.datetime(2024, 3, 21)},
    ]
    db[coll].insert_many(docs)
    saved = create_connector(
        {
            "name": f"Wave90Mongo-{coll}",
            "type": "mongodb",
            "host": "localhost",
            "port": 27017,
            "database": "dataflow_wave90",
        }
    )
    tools = DataPilotTools()
    try:
        count = tools.execute(
            "aggregate_data",
            {"connector_name": f"Wave90Mongo-{coll}", "table": coll, "metric": "count"},
        )
        assert count.success, count.error
        assert count.output["value"] == 6
        assert '"$sum": 1' in count.output["query"] or '"$sum":1' in count.output["query"].replace(" ", "")

        total = tools.execute(
            "aggregate_data",
            {
                "connector_name": f"Wave90Mongo-{coll}",
                "table": coll,
                "metric": "sum",
                "column": "amount",
            },
        )
        assert total.success, total.error
        assert abs(float(total.output["value"]) - 71.0) < 1e-9

        distinct = tools.execute(
            "aggregate_data",
            {
                "connector_name": f"Wave90Mongo-{coll}",
                "table": coll,
                "metric": "count_distinct",
                "column": "status",
            },
        )
        assert distinct.success, distinct.error
        # Missing status is ignored, matching SQL COUNT(DISTINCT).
        assert distinct.output["value"] == 2

        by_region = tools.execute(
            "aggregate_data",
            {
                "connector_name": f"Wave90Mongo-{coll}",
                "table": coll,
                "metric": "sum",
                "column": "amount",
                "group_by": "region",
            },
        )
        assert by_region.success, by_region.error
        alias = by_region.output["metric_alias"]
        rows = {
            r.get("region"): float(r.get(alias))
            for r in by_region.output["rows"]
            if r.get(alias) is not None
        }
        assert abs(rows["emea"] - 64.75) < 1e-9
        assert abs(rows["apac"] - 6.25) < 1e-9

        by_month = tools.execute(
            "aggregate_data",
            {
                "connector_name": f"Wave90Mongo-{coll}",
                "table": coll,
                "metric": "count",
                "group_by": "month",
            },
        )
        assert by_month.success, by_month.error
        assert by_month.output["group_count"] == 3
        assert "$dateTrunc" in by_month.output["query"]
    finally:
        db.drop_collection(coll)
        delete_connector(saved.id)
        cli.close()
