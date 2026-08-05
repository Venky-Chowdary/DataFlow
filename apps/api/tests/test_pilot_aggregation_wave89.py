"""Wave 89 — Datawrap Pilot answers analytics questions with exact aggregates.

Before this wave the pilot had no aggregation capability at all. Every phrasing
a data operator actually uses returned nothing, or worse:

    "count of orders by status"        -> []            (silent capability blurb)
    "how many rows in airports on X"   -> search_knowledge (a docs lookup!)
    "what is wrong with my mapping"    -> run_query("with my mapping")
    "show me errors from yesterday"    -> connector_name="yesterday"

These tests lock in three things:
  1. natural analytics phrasings route to ``aggregate_data`` with correct slots;
  2. names are grounded in the live schema — a wrong column reports the real
     ones instead of emitting invalid SQL or inventing an answer;
  3. against a live Postgres, the pilot's numbers equal independently computed
     SQL — COUNT/SUM/AVG/MIN/MAX/COUNT(DISTINCT), GROUP BY, top-N and NULL
     groups included.

Live sections skip (never silently pass) when Postgres is unreachable.
"""

from __future__ import annotations

import socket
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ai.copilot.aggregate_tools import (  # noqa: E402
    aggregate_connector_data,
    parse_aggregation_request,
    resolve_name,
)
from src.ai.copilot.tools import infer_tools_from_message  # noqa: E402

PG_HOST, PG_PORT = "localhost", 5432
PG_DSN = {
    "host": PG_HOST,
    "port": PG_PORT,
    "dbname": "dataflow",
    "user": "dataflow",
    "password": "dataflow",
}


# ---------------------------------------------------------------- parsing


@pytest.mark.parametrize(
    "prompt,metric,column,table,group_by",
    [
        ("count of orders by status", "count", "", "orders", "status"),
        ("how many orders", "count", "", "orders", ""),
        ("how many rows are in the airports table", "count", "", "airports", ""),
        ("sum revenue by month from sales", "sum", "revenue", "sales", "month"),
        ("total revenue from sales by region", "sum", "revenue", "sales", "region"),
        ("what is the average price in products", "avg", "price", "products", ""),
        ("average order value from orders", "avg", "order value", "orders", ""),
        ("max price in products", "max", "price", "products", ""),
        ("lowest price in products", "min", "price", "products", ""),
        ("distinct statuses in orders", "count_distinct", "statuses", "orders", ""),
        ("number of orders per country from orders", "count", "", "orders", "country"),
    ],
)
def test_analytics_phrasings_parse_into_slots(prompt, metric, column, table, group_by):
    req = parse_aggregation_request(prompt)
    assert req is not None, f"{prompt!r} produced no aggregation intent"
    assert req.metric == metric, prompt
    assert req.column == column, prompt
    assert req.table == table, prompt
    assert req.group_by == group_by, prompt


def test_top_n_ranking_is_a_grouped_sum():
    """"top 5 customers by revenue" carries no metric word but ranks a measure."""
    req = parse_aggregation_request("top 5 customers by revenue from orders")
    assert req is not None
    assert (req.metric, req.column, req.group_by, req.table) == (
        "sum",
        "revenue",
        "customer",
        "orders",
    )
    assert req.limit == 5
    assert req.descending is True

    bottom = parse_aggregation_request("bottom 3 regions by revenue from sales")
    assert bottom is not None and bottom.limit == 3
    assert bottom.descending is False, "bottom-N must sort ascending"

    counted = parse_aggregation_request("top 10 statuses by count from orders")
    assert counted is not None
    assert counted.metric == "count" and counted.column == ""


@pytest.mark.parametrize(
    "prompt",
    [
        "hello",
        "what is wrong with my mapping",
        "SELECT * FROM orders",
        "list tables on Local Postgres",
        "sample airports on Local Postgres",
        "go to connectors",
    ],
)
def test_non_analytics_prompts_are_not_hijacked(prompt):
    assert parse_aggregation_request(prompt) is None, prompt


@pytest.mark.parametrize(
    "prompt,expected",
    [
        ("how many jobs failed", "list_jobs"),
        ("how many connectors do I have", "list_connectors"),
    ],
)
def test_platform_inventory_is_not_a_table_scan(prompt, expected):
    """"how many jobs" is Datawrap's own inventory, not SELECT COUNT(*) FROM jobs."""
    names = [n for n, _ in infer_tools_from_message(prompt)]
    assert expected in names, f"{prompt!r} routed to {names}"
    assert "aggregate_data" not in names


def test_named_connector_makes_a_platform_noun_a_real_table():
    """A connector-qualified "jobs" is a warehouse table the user owns."""
    req = parse_aggregation_request("how many rows in jobs on Local Postgres")
    assert req is not None
    assert req.table == "jobs"
    assert req.connector_name == "Local Postgres"


# ---------------------------------------------------------------- routing


@pytest.mark.parametrize(
    "prompt",
    [
        "count of orders by status",
        "how many rows are in the airports table on Local Postgres",
        "sum revenue by month from sales",
        "what is the average price in products on Local Postgres",
        "top 5 customers by revenue from orders",
    ],
)
def test_router_sends_analytics_questions_to_aggregate_data(prompt):
    planned = infer_tools_from_message(prompt)
    names = [n for n, _ in planned]
    assert "aggregate_data" in names, f"{prompt!r} routed to {names}"
    # A 25-row sample or scraped SQL beside a real aggregate is noise.
    assert "sample_connector_object" not in names
    assert "run_query" not in names
    assert "search_knowledge" not in names, "a row count is not a docs lookup"


def test_english_containing_with_is_never_sent_as_sql():
    """Regression: "what is wrong with my mapping" ran WITH my mapping on a live DB."""
    for prompt in (
        "what is wrong with my mapping",
        "compare the airports table on Local Postgres with the one on Prod Postgres",
        "I need help with my transfer",
    ):
        for name, args in infer_tools_from_message(prompt):
            if name == "run_query":
                pytest.fail(f"{prompt!r} became SQL: {args.get('query')!r}")


def test_pasted_sql_still_routes_to_run_query():
    for sql in (
        "SELECT id, name FROM orders WHERE status = 'open'",
        "WITH t AS (SELECT 1 AS x) SELECT * FROM t",
        "```sql\nSELECT count(*) FROM airports\n```",
    ):
        names = [n for n, _ in infer_tools_from_message(sql)]
        assert "run_query" in names, f"{sql!r} routed to {names}"


def test_time_phrase_is_never_a_connector_name():
    """Regression: "errors from yesterday" looked for a connector called "yesterday"."""
    for prompt in ("show me errors from yesterday", "sample orders from last week"):
        for name, args in infer_tools_from_message(prompt):
            assert args.get("connector_name", "").lower() not in {
                "yesterday",
                "last week",
            }, f"{prompt!r} invented connector {args.get('connector_name')!r}"


# ---------------------------------------------------------------- grounding


def test_column_resolution_matches_real_schema_names():
    cols = ["id", "order_status", "order_value", "created_at", "customer_id"]
    assert resolve_name("status", cols) == "order_status"
    assert resolve_name("order status", cols) == "order_status"
    assert resolve_name("statuses", cols) == "order_status"
    assert resolve_name("ORDER_VALUE", cols) == "order_value"
    assert resolve_name("id", cols) == "id"
    # No silent wrong guess when nothing matches.
    assert resolve_name("revenue", cols) == ""
    # Ambiguous prefix must not pick one arbitrarily.
    assert resolve_name("order", ["order_status", "order_value"]) == ""


def test_unknown_metric_is_refused():
    res = aggregate_connector_data(table="orders", metric="median")
    assert not res.success
    assert "median" in res.error and "count" in res.error


def test_missing_table_asks_instead_of_guessing():
    res = aggregate_connector_data(table="", metric="count")
    assert not res.success
    assert "which table" in res.error.lower()


def test_table_name_must_be_an_identifier():
    res = aggregate_connector_data(table="orders; DROP TABLE users", metric="count")
    assert not res.success
    assert "simple table" in res.error.lower()


# ---------------------------------------------------------------- live Postgres


def _pg_reachable() -> bool:
    try:
        with socket.create_connection((PG_HOST, PG_PORT), timeout=3):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def live_orders():
    """Create a real table with known aggregates, register it as a connector."""
    if not _pg_reachable():
        pytest.skip(f"Postgres {PG_HOST}:{PG_PORT} not reachable")
    try:
        import psycopg2
    except ImportError:
        pytest.skip("psycopg2 not installed")

    from services.connector_store import create_connector, delete_connector

    table = f"pilot_agg_{uuid.uuid4().hex[:8]}"
    rows = [
        # (status, region, amount, ordered_at)
        ("open", "emea", Decimal("100.50"), "2024-01-15"),
        ("open", "emea", Decimal("200.25"), "2024-01-20"),
        ("open", "apac", Decimal("50.00"), "2024-02-05"),
        ("closed", "emea", Decimal("300.00"), "2024-02-11"),
        ("closed", "apac", Decimal("10.25"), "2024-03-02"),
        ("pending", "apac", None, "2024-03-09"),
        (None, "emea", Decimal("25.00"), "2024-03-14"),
    ]
    conn = psycopg2.connect(**PG_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE TABLE "{table}" ('
            " id serial PRIMARY KEY,"
            " status text,"
            " region text NOT NULL,"
            " amount numeric(10,2),"
            " ordered_at date NOT NULL)"
        )
        cur.executemany(
            f'INSERT INTO "{table}" (status, region, amount, ordered_at)'
            " VALUES (%s, %s, %s, %s)",
            rows,
        )

    connector_id = ""
    try:
        saved = create_connector(
            {
                "name": f"PilotAggPG-{uuid.uuid4().hex[:6]}",
                "type": "postgresql",
                "host": PG_HOST,
                "port": PG_PORT,
                "database": PG_DSN["dbname"],
                "username": PG_DSN["user"],
                "password": PG_DSN["password"],
                "schema": "public",
            }
        )
        connector_id = str(getattr(saved, "id", "") or "")
        if not connector_id:
            pytest.skip("connector store did not return an id")
        yield {"connector_id": connector_id, "table": table, "rows": rows}
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.close()
        if connector_id:
            try:
                delete_connector(connector_id)
            except Exception:
                pass


def _pg_scalar(sql: str):
    import psycopg2

    with psycopg2.connect(**PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    return row[0] if row else None


def test_live_row_count_is_exact(live_orders):
    table = live_orders["table"]
    res = aggregate_connector_data(
        connector_id=live_orders["connector_id"], table=table, metric="count"
    )
    assert res.success, res.error
    expected = _pg_scalar(f'SELECT COUNT(*) FROM "{table}"')
    assert res.output["value"] == expected == len(live_orders["rows"])
    assert res.output["exact"] is True
    assert res.output["read_only"] is True


@pytest.mark.parametrize(
    "metric,sql_fn",
    [
        ("sum", "SUM(amount)"),
        ("avg", "AVG(amount)"),
        ("min", "MIN(amount)"),
        ("max", "MAX(amount)"),
    ],
)
def test_live_numeric_metrics_match_independent_sql(live_orders, metric, sql_fn):
    table = live_orders["table"]
    res = aggregate_connector_data(
        connector_id=live_orders["connector_id"],
        table=table,
        metric=metric,
        column="amount",
    )
    assert res.success, res.error
    expected = _pg_scalar(f'SELECT {sql_fn} FROM "{table}"')
    got = res.output["value"]
    assert Decimal(str(got)) == Decimal(str(expected)), f"{metric}: {got} != {expected}"


def test_live_sum_keeps_exact_decimal_scale(live_orders):
    """NUMERIC(10,2) must not arrive as a lossy float."""
    res = aggregate_connector_data(
        connector_id=live_orders["connector_id"],
        table=live_orders["table"],
        metric="sum",
        column="amount",
    )
    assert res.success, res.error
    expected = sum(
        (r[2] for r in live_orders["rows"] if r[2] is not None), Decimal("0")
    )
    assert Decimal(str(res.output["value"])) == expected == Decimal("686.00")


def test_live_count_distinct_ignores_nulls_like_sql(live_orders):
    table = live_orders["table"]
    res = aggregate_connector_data(
        connector_id=live_orders["connector_id"],
        table=table,
        metric="count_distinct",
        column="status",
    )
    assert res.success, res.error
    expected = _pg_scalar(f'SELECT COUNT(DISTINCT status) FROM "{table}"')
    assert res.output["value"] == expected == 3


def test_live_group_by_keeps_the_null_group(live_orders):
    """A NULL dimension is a real group — dropping it would hide rows."""
    table = live_orders["table"]
    res = aggregate_connector_data(
        connector_id=live_orders["connector_id"],
        table=table,
        metric="count",
        group_by="status",
        limit=50,
    )
    assert res.success, res.error
    out = res.output
    assert out["group_by"] == "status"
    counts = {}
    for row in out["rows"]:
        key = row.get("status")
        counts[key] = int(row.get(out["metric_alias"]))
    assert counts == {"open": 3, "closed": 2, "pending": 1, None: 1}
    assert sum(counts.values()) == len(live_orders["rows"]), "grouped counts lost rows"


def test_live_grouped_sum_is_ordered_and_capped(live_orders):
    table = live_orders["table"]
    res = aggregate_connector_data(
        connector_id=live_orders["connector_id"],
        table=table,
        metric="sum",
        column="amount",
        group_by="region",
        limit=1,
    )
    assert res.success, res.error
    rows = res.output["rows"]
    assert len(rows) == 1, "limit was not applied"
    alias = res.output["metric_alias"]
    # emea = 100.50 + 200.25 + 300.00 + 25.00 = 625.75 > apac = 60.25
    assert rows[0]["region"] == "emea"
    assert Decimal(str(rows[0][alias])) == Decimal("625.75")


def test_live_group_by_month_buckets_the_date_column(live_orders):
    """"by month" is a time grain, resolved to the table's only date column."""
    res = aggregate_connector_data(
        connector_id=live_orders["connector_id"],
        table=live_orders["table"],
        metric="count",
        group_by="month",
        limit=50,
    )
    assert res.success, res.error
    out = res.output
    assert out["grain"] == "month"
    assert "DATE_TRUNC" in out["query"].upper()
    alias = out["metric_alias"]
    by_month = {str(r[out["columns"][0]])[:7]: int(r[alias]) for r in out["rows"]}
    assert by_month == {"2024-01": 2, "2024-02": 2, "2024-03": 3}


def test_live_unknown_column_reports_the_real_ones(live_orders):
    res = aggregate_connector_data(
        connector_id=live_orders["connector_id"],
        table=live_orders["table"],
        metric="sum",
        column="revenue",
    )
    assert not res.success
    assert "revenue" in res.error
    assert "amount" in res.error, "the real columns must be offered"
    assert "SELECT" not in res.error.upper(), "must not leak generated SQL as an answer"


def test_live_text_column_cannot_be_averaged(live_orders):
    """Refuse before the database errors, and point at numeric columns."""
    res = aggregate_connector_data(
        connector_id=live_orders["connector_id"],
        table=live_orders["table"],
        metric="avg",
        column="status",
    )
    assert not res.success
    assert "numeric" in res.error.lower()
    assert "amount" in res.error


def test_live_natural_prompt_end_to_end(live_orders):
    """The whole path: English -> planned tool -> exact number from Postgres."""
    table = live_orders["table"]
    planned = infer_tools_from_message(f"how many rows are in the {table} table")
    names = [n for n, _ in planned]
    assert "aggregate_data" in names, names
    args = dict(next(a for n, a in planned if n == "aggregate_data"))
    assert args["table"] == table
    args.pop("connector_name", None)
    args["connector_id"] = live_orders["connector_id"]

    from src.ai.copilot.tools import get_pilot_tools

    res = get_pilot_tools().execute("aggregate_data", args)
    assert res.success, res.error
    assert res.output["value"] == len(live_orders["rows"])


def test_unmapped_intent_is_honest_not_a_capability_tour():
    """An unsupported ask must admit it, not return a silent capability blurb."""
    from src.ai.copilot.pilot_agent import _unmapped_intent_reply

    for prompt in (
        "export orders to csv",
        "delete the connector Local Postgres",
        "create a pipeline that syncs orders every hour",
        "is my data safe",
    ):
        text = _unmapped_intent_reply(prompt, {"connectors": []})
        assert "not sure" in text.lower() or "didn't catch" in text.lower(), text
        # Must quote the ask so the operator sees we heard them.
        assert prompt.split()[0] in text.lower() or "“" in text or '"' in text
        # Must not claim to have done the work.
        assert "Available datasets" not in text
        assert "I can help with any question" not in text


def test_live_grouped_answer_renders_the_numbers(live_orders):
    """The composed reply must state the groups, not just say it ran a tool."""
    from src.ai.copilot.pilot_agent import _render_aggregate

    res = aggregate_connector_data(
        connector_id=live_orders["connector_id"],
        table=live_orders["table"],
        metric="count",
        group_by="status",
        limit=50,
    )
    assert res.success, res.error
    text = _render_aggregate(res.output)
    assert "4 groups" in text
    for status in ("open", "closed", "pending"):
        assert status in text
    assert "∅ (null)" in text, "the NULL group must be visible, not omitted"
    assert "not a sample" in text
    assert "```sql" in text, "operators need the SQL that produced the number"
