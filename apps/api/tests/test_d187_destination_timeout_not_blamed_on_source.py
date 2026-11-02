"""D187 — a slow destination is reported as the destination, not as the source.

QA: seven PostgreSQL/MySQL -> Snowflake setups (upsert and overwrite) failed
with "source unreachable/slow: <the source host>" while the same source loaded
into SQL Server at the same moment. The planner's preflight reads the source
AND the destination (its probe, privilege check and catalog reads each open a
Snowflake session), but ``plan_transfer`` labelled that whole step "source".
When a slow Snowflake login ran past the tool deadline, the healthy source was
named in the error.
"""

from __future__ import annotations

import os
import sqlite3
import time

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.ai.copilot.tool_deadline import note_step, run_with_deadline  # noqa: E402

SOURCE = {"host": "pg.source.test", "port": 5432}
DESTINATION = {"host": "acme-xy123.snowflakecomputing.com", "port": 443}


def test_a_step_that_reaches_both_systems_names_both():
    def slow():
        note_step(
            "running the preflight checks",
            SOURCE,
            role="source",
            also=(("destination", DESTINATION),),
        )
        time.sleep(5)

    out = run_with_deadline("plan_transfer", slow, lambda message: message, seconds=0.3)
    assert "pg.source.test:5432" in out
    assert "acme-xy123.snowflakecomputing.com:443" in out
    assert "destination" in out
    assert "running the preflight checks" in out
    # It cannot say which one is slow, so it must not accuse the source alone.
    assert "source unreachable/slow" not in out


def test_start_transfer_with_a_slow_preflight_names_the_destination(monkeypatch, tmp_path):
    import src.ai.copilot.transfer_tools as transfer_tools
    from src.ai.copilot.tools import get_pilot_tools

    db = tmp_path / "src.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO orders VALUES (1, 'a')")
    dest_db = tmp_path / "dst.db"
    source = {"name": "src", "type": "sqlite", "database": str(db), **SOURCE}
    dest = {"name": "dst", "type": "sqlite", "database": str(dest_db), **DESTINATION}

    def safe_connector(connector_id, connector_name, tool, *a, **k):
        return (source if "src" in (connector_id or "") else dest), None

    def slow_preflight(**kwargs):
        time.sleep(10)  # a destination login / warehouse resume still running

    monkeypatch.setenv("DATAFLOW_TOOL_DEADLINE_SECONDS", "2")
    monkeypatch.setattr(transfer_tools, "_safe_connector", safe_connector)
    monkeypatch.setattr(transfer_tools, "_run_preflight", slow_preflight)

    result = get_pilot_tools().execute(
        "start_transfer",
        {
            "source_connector_id": "c-src",
            "source_table": "orders",
            "dest_connector_id": "c-dst",
            "dest_table": "orders_copy",
            "sync_mode": "full_refresh_overwrite",
        },
    )

    assert not result.success
    error = str(result.error)
    assert "acme-xy123.snowflakecomputing.com:443" in error
    assert "destination" in error
    assert "source unreachable/slow" not in error
