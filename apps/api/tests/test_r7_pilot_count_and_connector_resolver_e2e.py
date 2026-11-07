"""Pilot row counts are COUNT(*), and a connector named any natural way resolves.

QA (demo videos, production 557f556c): asked to count rows, Pilot answered with
a 25-row sample; a question naming the connector by its display name, or with
different case or spacing, answered "Connector not found".

Driven through the routes the web app and agents call — ``/api/v1/copilot/chat``
(Pilot) and ``/api/v1/mcp/tools/call`` (MCP) — against a real PostgreSQL table
behind a connector saved with ``/api/v1/connectors/saved``. Skips when the
PostgreSQL in ``PG*`` env is not reachable.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

_PG = {
    "host": os.environ.get("PGHOST", "127.0.0.1"),
    "port": int(os.environ.get("PGPORT", "5432") or 5432),
    "user": os.environ.get("PGUSER", ""),
    "password": os.environ.get("PGPASSWORD", ""),
    "dbname": os.environ.get("PGDATABASE", ""),
}
TABLE = "r7_pilot_orders"
ROWS = 61


def _pg():
    import psycopg2

    conn = psycopg2.connect(connect_timeout=3, **_PG)
    conn.autocommit = True
    return conn


def _pg_up() -> bool:
    if not (_PG["user"] and _PG["dbname"]):
        return False
    try:
        _pg().close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_up(), reason="PostgreSQL (PG* env) not reachable")


@pytest.fixture(scope="module")
def pg_table():
    conn = _pg()
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cur.execute(f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, status TEXT, city TEXT)")
    # NULL, empty, whitespace and unicode values: a count is of rows, not values.
    values = [None, "", "   ", "paid", "Zürich ✓"]
    cur.executemany(
        f"INSERT INTO {TABLE} (id, status, city) VALUES (%s, %s, %s)",
        [(i, values[i % len(values)], values[(i + 1) % len(values)]) for i in range(1, ROWS + 1)],
    )
    yield TABLE
    cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
    conn.close()


@pytest.fixture
def pilot(tmp_path, monkeypatch, pg_table):
    from services import connector_store
    from src.ai.copilot import tools as tools_mod
    from src.ai.copilot import working_memory

    # Each test is a fresh conversation: no focus carried from another store.
    monkeypatch.setenv("DATAFLOW_PILOT_MEMORY_PATH", str(tmp_path / "pilot_memory.json"))
    monkeypatch.setattr(working_memory, "_memory", None)
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(tools_mod, "_tools", None)
    from src.main import app

    with TestClient(app) as client:
        yield client


def _save(client: TestClient, name: str) -> str:
    response = client.post(
        "/api/v1/connectors/saved",
        json={
            "name": name,
            "type": "postgresql",
            "role": "both",
            "host": _PG["host"],
            "port": _PG["port"],
            "database": _PG["dbname"],
            "username": _PG["user"],
            "password": _PG["password"],
            "ssl": False,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["id"]


def _chat(client: TestClient, message: str) -> dict:
    response = client.post("/api/v1/copilot/chat", json={"message": message})
    assert response.status_code == 200, response.text
    return response.json()


def _tools(body: dict) -> list[str]:
    return [t.get("name") for t in body.get("tools_used") or []]


COUNT_PROMPTS = [
    # The QA shape: a count asked for, a 25-row sample answered.
    f"show me the count of rows in {TABLE}",
    f"count {TABLE} rows",
    f"total rows in {TABLE}",
    f"{TABLE} row count",
    f"count rows of {TABLE}",
    # The connector named naturally: display name, case, spacing, punctuation.
    f"how many rows are in {TABLE} in orders postgres?",
    f"Count rows in {TABLE} in ORDERS  POSTGRES",
    f"count rows in {TABLE} on orders-postgres",
    f"how many rows does {TABLE} have in OrdersPostgres",
    f"Count the rows of the {TABLE} table in Orders Postgres",
    f"count rows in Orders Postgres {TABLE}",
]


@pytest.mark.parametrize("prompt", COUNT_PROMPTS)
def test_count_is_select_count_star_on_the_named_connector(pilot, prompt):
    _save(pilot, "Orders Postgres")
    body = _chat(pilot, prompt)
    answer = body.get("answer") or ""
    tools = _tools(body)
    assert "sample_connector_object" not in tools, (tools, answer)
    assert "aggregate_data" in tools, (tools, answer)
    assert f"{ROWS} rows" in answer, answer
    assert "COUNT(*)" in answer, answer
    # The basis is labelled: an exact aggregate, not a sample.
    assert "not a sample" in answer, answer
    assert "Connector not found" not in answer and "No connector matched" not in answer


def test_connector_id_resolves_in_a_question(pilot):
    cid = _save(pilot, "Orders Postgres")
    body = _chat(pilot, f"count rows in {TABLE} on {cid}")
    assert f"{ROWS} rows" in (body.get("answer") or ""), body.get("answer")


def test_two_matching_connectors_ask_which_one(pilot):
    first = _save(pilot, "Orders Postgres")
    second = _save(pilot, "Orders-Postgres")
    body = _chat(pilot, f"count rows in {TABLE} on ordersPostgres")
    answer = body.get("answer") or ""
    assert f"{ROWS} rows" not in answer, answer
    assert "Orders Postgres" in answer and "Orders-Postgres" in answer, answer
    assert first in answer and second in answer, answer


def test_an_unknown_connector_is_still_not_found(pilot):
    _save(pilot, "Orders Postgres")
    body = _chat(pilot, f"count rows in {TABLE} on Nope Warehouse {uuid.uuid4().hex[:4]}")
    answer = body.get("answer") or ""
    assert f"{ROWS} rows" not in answer, answer
    assert "No connector matched" in answer or "Connector not found" in answer, answer


@pytest.mark.parametrize("connector_name", ["orders-postgres", "ORDERS  POSTGRES", "OrdersPostgres"])
def test_mcp_aggregate_resolves_the_connector_name(pilot, connector_name):
    _save(pilot, "Orders Postgres")
    response = pilot.post(
        "/api/v1/mcp/tools/call",
        json={
            "name": "aggregate_data",
            "arguments": {"connector_name": connector_name, "table": TABLE, "metric": "count"},
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    text = str(body)
    assert "not found" not in text.lower() and "No connector matched" not in text, text
    assert str(ROWS) in text, text
