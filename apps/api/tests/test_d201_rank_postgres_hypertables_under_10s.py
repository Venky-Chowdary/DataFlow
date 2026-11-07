"""D201 / D24 end to end: rank_connector_tables on PostgreSQL with
hypertable-like tables answers in under 10s with the right order.

QA: on a TimescaleDB connector with about 25 tables the tool timed out at 45s
twice, while MariaDB ranked in under 2s. A hypertable keeps no rows of its
own: they live in chunks (child tables in ``_timescaledb_internal``), so the
catalog estimate of the parent is 0 / unknown and the ranking fell back to
exact COUNT(*) over every chunk, one table after another, through the full
aggregation path (a connect and eight catalog queries per count).

No TimescaleDB image fits this machine's disk, so declarative partitions
whose children live in another schema stand in for hypertables and chunks.
Nothing is ANALYZEd (fresh tables, as after a bulk load). Each COUNT(*) on a
"hypertable" pays 5s of server-side work (``pg_sleep`` on the same session),
the cost of scanning a large one.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from injected_latency import slow_postgres  # noqa: E402

SIZES = {"hyper_metrics": 30000, "hyper_events": 18000, "hyper_ticks": 9000}


@pytest.fixture
def hyper(monkeypatch):
    psycopg2 = pytest.importorskip("psycopg2")
    from conftest import pg_test_cfg
    from services import connector_executor
    from services.connector_store import create_connector, delete_connector

    cfg = pg_test_cfg()
    try:
        pg = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    pg.autocommit = True
    sfx = uuid.uuid4().hex[:6]
    schema, chunks = f"d201_{sfx}", f"d201_{sfx}_chunks"
    with pg.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}; CREATE SCHEMA {chunks}")
        for name, rows in SIZES.items():
            cur.execute(f"CREATE TABLE {schema}.{name} (ts date NOT NULL, v int) PARTITION BY RANGE (ts)")
            for k in range(4):
                cur.execute(
                    f"CREATE TABLE {chunks}._{name}_chunk_{k} PARTITION OF {schema}.{name} "
                    f"FOR VALUES FROM ('{2020 + k}-01-01') TO ('{2021 + k}-01-01')"
                )
            cur.execute(
                f"INSERT INTO {schema}.{name} SELECT ('2020-01-01'::date + (g % 1400)), g "
                f"FROM generate_series(1, {rows}) g"
            )
        for i in range(22):
            cur.execute(
                f"CREATE TABLE {schema}.t_{i:02d} (id int primary key); "
                f"INSERT INTO {schema}.t_{i:02d} SELECT generate_series(1, {100 + i * 300})"
            )
    conn = create_connector({"name": f"pg-hyper-{sfx}", "type": "postgresql", "schema": schema, **cfg})
    monkeypatch.delenv("DATAFLOW_TOOL_DEADLINE_SECONDS", raising=False)
    connector_executor.reset_for_tests()
    slow_postgres(monkeypatch, connect=0.2, scan=lambda sql: 5.0 if "hyper_" in sql else 0.0)
    try:
        yield conn.id
    finally:
        delete_connector(conn.id)
        with pg.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE; DROP SCHEMA IF EXISTS {chunks} CASCADE")
        pg.close()


def test_rank_with_hypertables_answers_in_under_10s_in_the_right_order(hyper):
    from src.ai.copilot.pilot_agent import _render_table_ranking
    from src.ai.copilot.tools import get_pilot_tools

    started = time.monotonic()
    result = get_pilot_tools().execute("rank_connector_tables", {"connector_id": hyper, "limit": 5})
    elapsed = time.monotonic() - started
    assert result.success, result.error
    assert elapsed < 10, elapsed
    ranked = result.output["ranked"]
    assert [r["table"] for r in ranked] == [
        "hyper_metrics", "hyper_events", "hyper_ticks", "t_21", "t_20",
    ], ranked
    # Rows shown are exact counts here; an estimate would be labelled.
    assert [r["rows"] for r in ranked[:3]] == list(SIZES.values())
    text = _render_table_ranking(result.output)
    assert "hyper_metrics" in text.splitlines()[0]


def test_rank_out_of_budget_labels_estimates_instead_of_failing(hyper, monkeypatch):
    """A count that cannot fit is shown as the catalog estimate, labelled."""
    import src.ai.copilot.aggregate_tools as agg
    from src.ai.copilot.tools import get_pilot_tools

    monkeypatch.setattr(agg, "_RANK_COUNT_BUDGET_SECONDS", 1.0)
    result = get_pilot_tools().execute("rank_connector_tables", {"connector_id": hyper, "limit": 5})
    assert result.success, result.error
    assert result.output["exact"] is False
    # The hypertables were not counted in 1s; with no estimate (never
    # analyzed) they are named as not counted rather than ranked as empty.
    named = {s["table"] for s in result.output["skipped"]} | set(result.output["estimated_tables"])
    assert {"hyper_metrics", "hyper_events", "hyper_ticks"} <= named | {
        r["table"] for r in result.output["ranked"] if r.get("basis") == "count"
    }
