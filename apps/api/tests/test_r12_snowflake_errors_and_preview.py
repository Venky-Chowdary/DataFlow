"""QA round 12 (build 8f7d246d), Snowflake D1 and D14.

D1 — a wrong warehouse answered "Destination or resource not found". Snowflake
refuses ``USE WAREHOUSE`` on a missing or ungranted warehouse with the
anonymous ``002043 Object does not exist, or operation cannot be performed``,
and the generic missing-object rule turned that into a message naming no
warehouse. Database, schema and role refusals named the object in the driver
text, but the copy dropped the name.

fakesnow answers ``USE WAREHOUSE`` as a no-op, so the refusal is injected at
the cursor with the exact error a real account returns; the message text and
code need confirming on a real Snowflake account.

D14 — ``/shape/preview`` ran unbounded on the event loop and once passed
Cloudflare's 100 s origin limit (524). It now has a server-side time budget
and says when it shaped fewer rows than it was sent.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

pytest.importorskip("fakesnow")


def _refusing_cursor(monkeypatch, *, current_warehouse: str | None = None) -> list[str]:
    """fakesnow with a real account's refusal of an unknown warehouse."""
    import snowflake.connector
    from fakesnow.cursor import FakeSnowflakeCursor

    sent: list[str] = []
    orig = FakeSnowflakeCursor.execute

    def execute(self, command, *args, **kwargs):
        text = " ".join(str(command).split()).upper()
        sent.append(text)
        if text.startswith("USE WAREHOUSE"):
            raise snowflake.connector.errors.ProgrammingError(
                msg="SQL compilation error:\nObject does not exist, or operation cannot be performed.",
                errno=2043, sqlstate="02000",
            )
        if text == "SELECT CURRENT_WAREHOUSE()":
            return orig(self, "SELECT %s AS w", (current_warehouse,))
        return orig(self, command, *args, **kwargs)

    monkeypatch.setattr(FakeSnowflakeCursor, "execute", execute)
    return sent


@pytest.fixture
def fakesnow_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    from connectors.snowflake_conn import get_connection

    conn = get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="PUBLIC", warehouse="", connection_string="",
    )
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE IF NOT EXISTS dataflow")
        cur.execute("CREATE SCHEMA IF NOT EXISTS dataflow.public")
    conn.close()


def test_d1_connection_test_names_the_wrong_warehouse(fakesnow_dir, monkeypatch):
    from connectors.snowflake import test_snowflake
    from src.transfer.connector_registry import humanize_connection_error

    _refusing_cursor(monkeypatch)
    result = test_snowflake(
        host="localhost", port=443, username="test", password="test",
        database="dataflow", schema="PUBLIC", connection_string="", ssl=True,
        warehouse="QA_NO_SUCH_WH", list_tables=False,
    )
    assert result.ok is False
    shown = humanize_connection_error("snowflake", result.error)
    assert shown.startswith("Warehouse QA_NO_SUCH_WH does not exist or is not authorized"), shown
    assert "resource not found" not in shown.lower()


def _write(warehouse: str, table: str, rows: list[list[str]]):
    from connectors.snowflake_writer import write_mapped_rows

    return write_mapped_rows(
        host="localhost", port=443, database="dataflow", username="test",
        password="test", schema="PUBLIC", connection_string="", ssl=True,
        warehouse=warehouse, table_name=table, headers=["id", "name"], data_rows=rows,
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        column_types={"id": "INTEGER", "name": "VARCHAR"}, create_table=True,
    )


def test_d1_writer_names_the_warehouse_when_the_session_has_none(fakesnow_dir, monkeypatch):
    from src.transfer.connector_registry import humanize_connection_error

    _refusing_cursor(monkeypatch, current_warehouse=None)
    res = _write("QA_NO_SUCH_WH", "r12_d1_wh", [["1", "a"], ["2", ""], ["3", " "]])
    assert res.ok is False
    assert "Warehouse QA_NO_SUCH_WH does not exist or is not authorized" in res.error, res.error
    shown = humanize_connection_error("snowflake", res.error)
    assert "QA_NO_SUCH_WH" in shown and "resource not found" not in shown.lower(), shown


def test_d1_writer_keeps_a_working_login_warehouse(fakesnow_dir, monkeypatch):
    """A refused USE with the login's warehouse still active is not fatal."""

    _refusing_cursor(monkeypatch, current_warehouse="COMPUTE_WH")
    res = _write("compute_wh", "r12_d1_ok", [["1", "Zoë 東京"], ["2", "  "]])
    assert res.ok is True, res.error
    assert res.rows_written == 2


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "002003 (02000): SQL compilation error:\nDatabase 'QA_NOPE' does not exist or not authorized.",
            "Database QA_NOPE does not exist or is not authorized",
        ),
        (
            "002003 (02000): SQL compilation error:\nSchema 'QA_SF.NOPE' does not exist or not authorized.",
            "Schema QA_SF.NOPE does not exist or is not authorized",
        ),
        (
            "250001 (08001): Failed to connect to DB: x.snowflakecomputing.com:443. Role "
            "'QA_ROLE' specified in the connect string does not exist or not authorized. "
            "Contact your local system administrator, or attempt to login with another role",
            "Role QA_ROLE does not exist or is not authorized",
        ),
    ],
)
def test_d1_database_schema_and_role_refusals_name_the_object(raw, expected):
    from src.transfer.connector_registry import humanize_connection_error

    shown = humanize_connection_error("snowflake", raw)
    assert shown.startswith(expected), shown


# ---------------------------------------------------------------------------
# D14
# ---------------------------------------------------------------------------


@pytest.fixture
def shape_client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.routers.shape_router import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    return TestClient(app)


def _rows(n: int) -> list[dict[str, Any]]:
    cells = [None, "", "   ", "Zoë 東京 ✓", "12.50", "-0.0001"]
    return [{"id": str(i), "v": cells[i % len(cells)]} for i in range(n)]


def test_d14_preview_stops_at_its_time_budget_and_says_so(shape_client, monkeypatch):
    from services import shape_apply

    monkeypatch.setenv("DATAFLOW_SHAPE_PREVIEW_BUDGET_S", "1")
    orig = shape_apply.ShapeRunner.records

    def slow(self, batch):
        time.sleep(0.005)  # a recipe that costs 5 ms a row: 2000 rows = 10 s
        return orig(self, batch)

    monkeypatch.setattr(shape_apply.ShapeRunner, "records", slow)
    started = time.monotonic()
    res = shape_client.post("/api/v1/shape/preview", json={
        "recipe": {"steps": [{"op": "trim", "column": "v"}]},
        "sample_rows": _rows(2000), "source_columns": ["id", "v"],
    })
    elapsed = time.monotonic() - started
    assert res.status_code == 200, res.text
    body = res.json()
    assert elapsed < 6, elapsed
    assert body["truncated"] is True
    assert 0 < body["evaluated_rows"] < 2000
    assert body["sampled_rows"] == body["evaluated_rows"]
    assert len(body["before"]) <= body["evaluated_rows"]
    note = body["preview_note"]
    assert "truncated" in note.lower() and f"of 2000" in note and "time limit" in note, note


def test_d14_a_preview_inside_its_budget_is_complete(shape_client):
    res = shape_client.post("/api/v1/shape/preview", json={
        "recipe": {"steps": [{"op": "trim", "column": "v"}]},
        "sample_rows": _rows(12), "source_columns": ["id", "v"],
        "source_type": "postgresql",
    })
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["truncated"] is False and body["evaluated_rows"] == 12
    after = {r["id"]: r["v"] for r in body["after"]}
    assert after["0"] is None and after["1"] == "" and after["2"] == ""
    assert after["3"] == "Zoë 東京 ✓" and after["4"] == "12.50"
    assert "truncated" not in body["preview_note"].lower()
