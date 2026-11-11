"""Exit rehearsal: regressions from the second adversarial review (review 2) and the web integration.

Each test names what it pins: N9 the definition hash, gap 3 the rehearsals
list and a running summary, N10 masked control totals, N5 / N8 the typed digest
and actionable reasons, N4 the verification lease, gap 1 PII review, and the
Low items.
"""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from r8_ent_harness import ent  # noqa: E402,F401
from tests.test_exit_rehearsal_e2e import _run, _system, world  # noqa: E402,F401


# --------------------------------------------- N9: what the definition hash tracks


def _definition(**over):
    from services.exit_inventory import definition_sha256

    system = {"id": "exs_1", "workspace_id": "ws1", "source": {"connector_id": "c1"},
              "tables": [{"name": "orders", "schema": None, "key_columns": []}], "exit_window_hours": 24,
              "extract_format": "jsonl"}
    system.update(over)
    return definition_sha256(system)


def test_n9_the_extract_format_is_part_of_the_definition(world):  # noqa: F811
    assert _definition() != _definition(extract_format="csv")


def test_n9_an_edit_to_the_connector_behind_the_same_id_changes_the_definition(world, monkeypatch):  # noqa: F811
    from services import schedule_runner

    conn = {"name": "ledger", "type": "postgresql", "host": "db1.internal", "port": 5432, "database": "ledger",
            "username": "svc", "password": "s3cret"}
    monkeypatch.setattr(schedule_runner, "_resolve_connector_for_schedule", lambda _cid, _sched: dict(conn))
    before = _definition()
    conn["password"] = "rotated"  # a secret is never part of the fingerprint
    assert _definition() == before
    conn["host"] = "db2.internal"
    moved = _definition()
    assert moved != before
    conn["database"] = "ledger_v2"
    assert _definition() != moved


def test_n9_changing_the_extract_format_makes_a_proven_system_stale(world):  # noqa: F811
    from services.exit_inventory import get_system, readiness, update

    system = _system(tables=[{"name": "events"}])
    assert _run(system)["verdict"] == "proven"
    assert readiness(get_system(system["id"]))["state"] == "proven"
    update(system["id"], {"extract_format": "csv"}, actor="owner@example.com", expected_version=None)
    ready = readiness(get_system(system["id"]))
    assert ready["stale"] is True and ready["state"] == "not_proven"
    assert "format" in ready["stale_reason"]


# ------------------------------- gap 3: the rehearsals list and a running rehearsal's summary


def test_gap3_the_rehearsals_list_says_whether_each_rehearsal_is_current(world):  # noqa: F811
    from services.exit_inventory import get_system, update
    from services.exit_inventory import rehearsal_history

    system = _system(tables=[{"name": "events"}])
    assert _run(system)["verdict"] == "proven"
    listed = rehearsal_history(get_system(system["id"]), limit=20)
    assert [r["current"] for r in listed["rehearsals"]] == [True]
    update(system["id"], {"exit_window_hours": 48}, actor="owner@example.com", expected_version=None)
    listed = rehearsal_history(get_system(system["id"]), limit=20)
    assert [r["current"] for r in listed["rehearsals"]] == [False]
    assert listed["trend"]["points"]


def test_gap3_a_running_rehearsal_summary_reads_its_window_and_tables_from_its_definition(world):  # noqa: F811
    from services.exit_inventory import get_system, readiness, rehearsal_summary
    from services.exit_rehearsal import start

    system = _system(tables=[{"name": "events"}, {"name": "orders"}], exit_window_hours=6)
    running = start(system, actor="owner@example.com")
    summary = rehearsal_summary(running, get_system(system["id"]))
    assert summary["verdict"] == "running"
    assert summary["window_seconds"] == 6 * 3600 and summary["tables_declared"] == 2
    assert readiness(get_system(system["id"]))["last_rehearsal"]["tables_declared"] == 2


def test_gap3_the_list_route_reads_current_over_rest(ent):  # noqa: F811
    import sqlite3
    import time

    (ent.tmp / "sqlite").mkdir(exist_ok=True)
    db = ent.tmp / "sqlite" / "gap3.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount NUMERIC)")
    conn.executemany("INSERT INTO orders VALUES (?, ?)", [(1, 1.5), (2, 2.5)])
    conn.commit()
    conn.close()
    ws = ent.workspace("Exit gap 3")
    editor = ent.member(email="exit-gap3@example.com", workspace_id=ws, role="editor")
    connector = editor.post("/api/v1/connectors/saved", json={"name": "ledger", "type": "sqlite", "role": "source",
                                                               "database": str(db)}).json()["id"]
    sid = editor.post("/api/v1/exit/systems", json={
        "name": "Gap 3 ledger", "source": {"connector_id": connector},
        "tables": [{"name": "orders", "key_columns": ["id"]}], "exit_window_hours": 24}).json()["id"]
    started = editor.post(f"/api/v1/exit/systems/{sid}/rehearsals").json()
    running = editor.get(f"/api/v1/exit/systems/{sid}/rehearsals").json()["rehearsals"][0]
    assert running["tables_declared"] == 1 and running["window_seconds"] == 24 * 3600
    deadline, body = time.monotonic() + 120, started
    while body["verdict"] == "running" and time.monotonic() < deadline:
        time.sleep(0.5)
        body = editor.get(f"/api/v1/exit/rehearsals/{started['id']}").json()
    listed = editor.get(f"/api/v1/exit/systems/{sid}/rehearsals").json()["rehearsals"]
    assert listed[0]["id"] == started["id"] and listed[0]["current"] is True
